from fastapi import Depends, HTTPException
from opentelemetry import metrics
from fiber.logging_utils import get_logger
from fastapi.routing import APIRouter

from core.models import payload_models
from core.task_config import get_enabled_task_config
from validator.organic_node.src.core.configuration import Config
from validator.organic_node.src.core.dependencies import get_config
from validator.organic_node.src.core.middleware import verify_api_key_rate_limit
from validator.organic_node.src.models import request_models
from validator.db.src.sql.contenders import get_contenders_for_task
from validator.db.src.sql.nodes import get_node
from validator.common.query import nonstream
from core.models.payload_models import ImageResponse
from validator.organic_node.src import utils

logger = get_logger(__name__)

COUNTER_IMAGE_ERROR = metrics.get_meter(__name__).create_counter("validator.organic_node.image.error")
COUNTER_IMAGE_SUCCESS = metrics.get_meter(__name__).create_counter("validator.organic_node.image.success")

async def query_image_generation(
    config: Config,
    payload: dict,
    task: str,
    timeout: float,
) -> ImageResponse:
    """Query image generation from available contenders."""
    contenders = await get_contenders_for_task(config.psql_db, task, 5, "organic")
    if not contenders:
        COUNTER_IMAGE_ERROR.add(1, {"task": task, "kind": "no_contenders", "status_code": 500})
        raise HTTPException(status_code=500, detail="No available nodes to process request")

    for contender in contenders:
        node = await get_node(config.psql_db, contender.node_id, config.netuid)
        if not node:
            logger.error(f"Node {contender.node_id} not found")
            continue

        try:
            success, result = await nonstream.query_nonstream(
                config=config,
                contender=contender,
                node=node,
                payload=payload,
                response_model=ImageResponse,
                synthetic_query=False,
                job_id="organic",  # not used for metrics/tracking only
            )

            if success and result.formatted_response:
                image_response = result.formatted_response
                if image_response.is_nsfw:
                    COUNTER_IMAGE_ERROR.add(1, {"task": task, "kind": "nsfw", "status_code": 403})
                    raise HTTPException(status_code=403, detail="NSFW content detected")

                if not image_response.image_b64:
                    COUNTER_IMAGE_ERROR.add(1, {"task": task, "kind": "no_image", "status_code": 500})
                    continue

                COUNTER_IMAGE_SUCCESS.add(1, {"task": task, "status_code": 200})
                return image_response

        except Exception as e:
            logger.error(f"Error querying node {node.node_id}: {e}")
            continue

    COUNTER_IMAGE_ERROR.add(1, {"task": task, "kind": "all_contenders_failed", "status_code": 500})
    raise HTTPException(status_code=500, detail="No available nodes could process the request")

async def process_image_request(
    payload: payload_models.TextToImagePayload | payload_models.ImageToImagePayload | payload_models.AvatarPayload,
    task: str,
    config: Config,
) -> request_models.ImageResponse:
    task = task.replace("_", "-")
    task_config = get_enabled_task_config(task)
    if task_config is None:
        COUNTER_IMAGE_ERROR.add(1, {"task": task, "reason": "no_task_config", "status_code": 400})
        logger.error(f"Task config not found for task: {task}")
        raise HTTPException(status_code=400, detail=f"Invalid model {task}")

    utils.handle_min_steps(task_config, payload.steps)

    try:
        image_response = await query_image_generation(
            config=config,
            payload=payload.model_dump(),
            task=task,
            timeout=task_config.timeout,
        )
        return request_models.ImageResponse(image_b64=image_response.image_b64)

    except HTTPException:
        raise
    except Exception as e:
        COUNTER_IMAGE_ERROR.add(1, {"task": task, "kind": type(e).__name__, "status_code": 500})
        logger.error(f"Error processing image request: {e}")
        raise HTTPException(status_code=500, detail="Unable to process request")

async def text_to_image(
    text_to_image_request: request_models.TextToImageRequest,
    config: Config = Depends(get_config),
) -> request_models.ImageResponse:
    payload = utils.text_to_image_to_payload(text_to_image_request)
    return await process_image_request(payload, payload.model, config)

async def image_to_image(
    image_to_image_request: request_models.ImageToImageRequest,
    config: Config = Depends(get_config),
) -> request_models.ImageResponse:
    payload = await utils.image_to_image_to_payload(
        image_to_image_request,
        httpx_client=config.httpx_client,
        prod=config.prod,
    )
    return await process_image_request(payload, payload.model, config)

async def avatar(
    avatar_request: request_models.AvatarRequest,
    config: Config = Depends(get_config),
) -> request_models.ImageResponse:
    payload = await utils.avatar_to_payload(
        avatar_request,
        httpx_client=config.httpx_client,
        prod=config.prod,
    )
    return await process_image_request(payload, "avatar", config)

router = APIRouter(
    tags=["Image"],
    dependencies=[Depends(verify_api_key_rate_limit)],
)

router.add_api_route("/v1/text-to-image", text_to_image, methods=["POST"])
router.add_api_route("/v1/image-to-image", image_to_image, methods=["POST"])
router.add_api_route("/v1/avatar", avatar, methods=["POST"])
