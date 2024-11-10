import json
from typing import Any
import uuid
from fastapi import Depends, HTTPException
from opentelemetry import metrics
from redis.asyncio import Redis
from fiber.logging_utils import get_logger
from fastapi.routing import APIRouter
from core.models import payload_models
from core.task_config import get_enabled_task_config
from validator.entry_node.src.core.configuration import Config
from validator.entry_node.src.core.dependencies import get_config
from validator.entry_node.src.core.middleware import verify_api_key_rate_limit
from validator.utils.redis import redis_constants as rcst
from validator.utils.generic import generic_constants as gcst
from validator.entry_node.src.models import request_models
import time
import validator.utils.redis.redis_utils as rutils
from validator.utils.generic.generic_dataclasses import GenericResponse

logger = get_logger(__name__)


COUNTER_IMAGE_ERROR = metrics.get_meter(__name__).create_counter("validator.entry_node.image.error")
COUNTER_IMAGE_SUCCESS = metrics.get_meter(__name__).create_counter("validator.entry_node.image.success")


def _construct_organic_message(payload: dict, job_id: str, task: str) -> str:
    return json.dumps({
        "query_type": gcst.ORGANIC,
        "query_payload": payload,
        "task": task,
        "job_id": job_id
    })


async def _wait_for_acknowledgement(redis_db: Redis, job_id: str, start: float, task: str, timeout: float = 2) -> bool:
    response_queue = await rutils.get_response_queue_key(job_id)
    try:
        result = await redis_db.blpop(response_queue, timeout=timeout)
        if result is None:
            return False
        
        _, data = result
        end = time.time()
        data = data.decode()
        logger.info(f"Ack for job_id : {job_id}: {data} - ack time : {round(end-start, 3)}s")
        return data == rcst.ACK_TOKEN
    except Exception as e:
        COUNTER_IMAGE_ERROR.add(1, {"task": task, "kind": "_wait_for_acknowledgement failed", "status_code": 500})
        logger.error(f"Error waiting for acknowledgment: {e}")
        return False


async def _collect_single_result(redis_db: Redis, job_id: str, timeout: float, task: str) -> GenericResponse | None:
    response_queue = await rutils.get_response_queue_key(job_id)
    try:
        start_time = time.time()
        while (time.time() - start_time) < timeout:
            result = await redis_db.blpop(response_queue, timeout=rcst.RESPONSE_QUEUE_TTL)
            if result is None:
                continue

            _, data = result
            if not data:
                continue

            try:
                content = json.loads(data.decode())
                logger.debug(f"Received content: {content}")
                
                if gcst.STATUS_CODE in content and content[gcst.STATUS_CODE] >= 400:
                    COUNTER_IMAGE_ERROR.add(1, {"task": task, "kind": "_collect_single_result failed", "status_code": content[gcst.STATUS_CODE]})
                    raise HTTPException(
                        status_code=content[gcst.STATUS_CODE],
                        detail=content.get(gcst.ERROR_MESSAGE, "Unknown error")
                    )
                    
                if gcst.CONTENT not in content:
                    logger.warning(f"Malformed content received: {content}")
                    continue
                    
                return GenericResponse(
                    job_id=job_id,
                    content=content[gcst.CONTENT],
                    status_code=content.get(gcst.STATUS_CODE, 200)
                )
                
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode response data: {e}")
                continue

        logger.error(f"Timeout waiting for response in queue {response_queue}")
        COUNTER_IMAGE_ERROR.add(1, {"task": task, "kind": "redis_acknowledgement_timeout", "status_code": 500})
        raise HTTPException(status_code=500, detail="Request timed out")
            
    finally:
        await rutils.ensure_queue_clean(redis_db, job_id)


async def make_non_stream_organic_query(
    redis_db: Redis, 
    payload: dict[str, Any], 
    task: str, 
    timeout: float
) -> GenericResponse | None:
    job_id = rutils.generate_job_id()
    organic_message = _construct_organic_message(payload=payload, job_id=job_id, task=task)

    await rutils.ensure_queue_clean(redis_db, job_id)    
    await redis_db.lpush(rcst.QUERY_QUEUE_KEY, organic_message)
    start = time.time()
    if not await _wait_for_acknowledgement(redis_db, job_id, start, task):
        logger.error(f"No acknowledgment received for job {job_id}")
        await rutils.ensure_queue_clean(redis_db, job_id)
        raise HTTPException(status_code=500, detail="Unable to process request")

    return await _collect_single_result(redis_db, job_id, timeout, task)


async def process_image_request(
    payload: payload_models.TextToImagePayload
    | payload_models.ImageToImagePayload
    | payload_models.InpaintPayload
    | payload_models.AvatarPayload,
    task: str,
    config: Config,
) -> request_models.ImageResponse:
    task = task.replace("_", "-")
    task_config = get_enabled_task_config(task)
    if task_config is None:
        COUNTER_IMAGE_ERROR.add(1, {"reason": "no_task_config"})
        logger.error(f"Task config not found for task: {task}")
        raise HTTPException(status_code=400, detail=f"Invalid model {task}")

    job_id = uuid.uuid4().hex
    result = await make_non_stream_organic_query(
        redis_db=config.redis_db,
        payload=payload.model_dump(),
        task=task,
        timeout=rcst.RESPONSE_QUEUE_TTL
    )
    
    if result is None or result.content is None:
        logger.error(f"No content received for image request. Task: {task}")
        raise HTTPException(status_code=500, detail="Unable to process request")
    image_response = payload_models.ImageResponse(**json.loads(result.content))
    if image_response.is_nsfw:
        COUNTER_IMAGE_ERROR.add(1, {"task": task, "kind": "nsfw", "status_code": 403})
        raise HTTPException(status_code=403, detail="NSFW content detected")

    if image_response.image_b64 is None:
        COUNTER_IMAGE_ERROR.add(1, {"task": task, "kind": "no_image", "status_code": 500})
        raise HTTPException(status_code=500, detail="Unable to process request")

    COUNTER_IMAGE_SUCCESS.add(1, {"task": task, "status_code": 200})
    return request_models.ImageResponse(image_b64=image_response.image_b64)


async def text_to_image(
    text_to_image_request: request_models.TextToImageRequest,
    config: Config = Depends(get_config),
) -> request_models.ImageResponse:
    payload = request_models.text_to_image_to_payload(text_to_image_request)

    result = await process_image_request(payload, payload.model, config)
    return result

async def image_to_image(
    image_to_image_request: request_models.ImageToImageRequest,
    config: Config = Depends(get_config),
) -> request_models.ImageResponse:
    payload = await request_models.image_to_image_to_payload(
        image_to_image_request,
        httpx_client=config.httpx_client,
        prod=config.prod,
    )

    result = await process_image_request(payload, payload.model, config)
    return result


async def inpaint(
    inpaint_request: request_models.InpaintRequest,
    config: Config = Depends(get_config),
) -> request_models.ImageResponse:
    payload = await request_models.inpaint_to_payload(inpaint_request, httpx_client=config.httpx_client, prod=config.prod)

    result = await process_image_request(payload, "inpaint", config)
    return result


async def avatar(
    avatar_request: request_models.AvatarRequest,
    config: Config = Depends(get_config),
) -> request_models.ImageResponse:
    payload = await request_models.avatar_to_payload(avatar_request, httpx_client=config.httpx_client, prod=config.prod)

    result = await process_image_request(payload, "avatar", config)
    return result


router = APIRouter(
    tags=["Image"],
    dependencies=[Depends(verify_api_key_rate_limit)],
)

router.add_api_route("/v1/text-to-image", text_to_image, methods=["POST"])
router.add_api_route("/v1/image-to-image", image_to_image, methods=["POST"])
router.add_api_route("/v1/inpaint", inpaint, methods=["POST"])
router.add_api_route("/v1/avatar", avatar, methods=["POST"])