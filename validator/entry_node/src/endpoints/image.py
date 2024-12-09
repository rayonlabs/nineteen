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
import asyncio

from redis.asyncio.client import PubSub

from validator.utils.generic.generic_dataclasses import GenericResponse

logger = get_logger(__name__)


COUNTER_IMAGE_ERROR = metrics.get_meter(__name__).create_counter("validator.entry_node.image.error")
COUNTER_IMAGE_SUCCESS = metrics.get_meter(__name__).create_counter("validator.entry_node.image.success")


def _construct_organic_message(payload: dict, job_id: str, task: str) -> str:
    return json.dumps({"query_type": gcst.ORGANIC, "query_payload": payload, "task": task, "job_id": job_id})


async def _wait_for_acknowledgement(pubsub: PubSub, job_id: str) -> bool:
    async for message in pubsub.listen():
        channel = message["channel"].decode()
        if channel == f"{gcst.ACKNLOWEDGED}:{job_id}" and message["type"] == "message":
            logger.info(f"Job {job_id} confirmed by worker")
            break

    await pubsub.unsubscribe(f"{gcst.ACKNLOWEDGED}:{job_id}")
    return True


async def _collect_single_result(pubsub: PubSub, job_id: str) -> GenericResponse | None:
    result = None
    async for message in pubsub.listen():
        try:
            if message["type"] == "message":
                result = json.loads(message["data"].decode())
                if gcst.ACKNLOWEDGED in result:
                    continue
                status_code = result[gcst.STATUS_CODE]
                if status_code >= 400:
                    raise HTTPException(status_code=status_code, detail=result[gcst.ERROR_MESSAGE])
                break
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON for message: {message}. Error: {e}")
            continue
    await pubsub.unsubscribe(f"{rcst.JOB_RESULTS}:{job_id}")
    if result is None:
        return None
    return GenericResponse(**result)


async def make_non_stream_organic_query(
    job_id: str, redis_db: Redis, payload: dict[str, Any], task: str, timeout: float
) -> GenericResponse | None:
    
    organic_message = _construct_organic_message(payload=payload, job_id=job_id, task=task)  # NOTE: tis grim

    pubsub = redis_db.pubsub()
    await pubsub.subscribe(f"{gcst.ACKNLOWEDGED}:{job_id}")
    await redis_db.lpush(rcst.QUERY_QUEUE_KEY, organic_message)  # type: ignore

    try:
        await asyncio.wait_for(_wait_for_acknowledgement(pubsub, job_id), timeout=1)

        await pubsub.subscribe(f"{rcst.JOB_RESULTS}:{job_id}")

    except asyncio.TimeoutError:
        logger.error(f"No confirmation received for job {job_id} within timeout period. Task: {task}")
        COUNTER_IMAGE_ERROR.add(1, {"task": task, "kind": "redis_acknowledgement_timeout", "status_code": 500})
        raise HTTPException(status_code=500, detail=f"Unable to process task: {task} ; redis_acknowledgement_timeout, please try again later.")
    
    try:
        return await asyncio.wait_for(_collect_single_result(pubsub, job_id), timeout=timeout)
    except asyncio.TimeoutError:
        logger.error(f"Timed out waiting for the first chunk of results for job {job_id}. Task: {task}")
        COUNTER_IMAGE_ERROR.add(1, {"task": task, "kind": "_collect_single_result_failed", "status_code": 500})
        raise HTTPException(status_code=500, detail=f"Unable to process task: {task} ; _collect_single_result_failed, please try again later.")



async def process_image_request(
    payload: payload_models.TextToImagePayload
    | payload_models.ImageToImagePayload
    | payload_models.InpaintPayload
    | payload_models.AvatarPayload,
    task: str,
    config: Config,
) -> request_models.ImageResponse:
    task = task.replace("_", "-")
    task_config = get_enabled_task_config(task)  # NOTE: is grim
    if task_config is None:
        COUNTER_IMAGE_ERROR.add(1, {"reason": "no_task_config"})
        logger.error(f"Task config not found for task: {task}")
        raise HTTPException(status_code=400, detail=f"Invalid model {task}")

    job_id = uuid.uuid4().hex
    result = await make_non_stream_organic_query(
        job_id=job_id,
        redis_db=config.redis_db,
        payload=payload.model_dump(),
        task=task,
        timeout=task_config.timeout,
    )

    if result is None or result.content is None:
        COUNTER_IMAGE_ERROR.add(1, {"task": task, "kind": "no_result", "status_code": 500})
        logger.error(f"No content received an image request for some reason. Task: {task}")
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
