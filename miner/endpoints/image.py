from functools import partial
from fastapi import Depends, HTTPException

from fiber.miner.security.encryption import decrypt_general_payload
from pydantic import BaseModel
from core.models import payload_models
from fastapi.routing import APIRouter
from miner import constants as mcst
from core import task_config as tcfg
from miner.config import WorkerConfig
from miner.dependencies import get_worker_config
from miner.logic.image import get_image_from_server
from fiber.miner.core.configuration import Config
from fiber.miner.dependencies import blacklist_low_stake, get_config as get_fiber_config, verify_request
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


async def _process_image_request(
    decrypted_payload: BaseModel,
    fiber_config: Config,
    post_endpoint: str,
    worker_config: WorkerConfig,
) -> payload_models.ImageResponse:
    logger.info(f"Processing image request: {decrypted_payload}")

    assert hasattr(decrypted_payload, 'model'), "The image request payload must have a 'model' attribute"

    task_config = tcfg.get_enabled_task_config(decrypted_payload.model)
    if task_config is None:
        raise ValueError(f"Task config not found for model: {decrypted_payload.model}")
    
    # NOTE: load_model_config for image models is set only for the models that are added customly by validators 
    # It is up to miners to have a nicer way of doing this
    if task_config.orchestrator_server_config.load_model_config:
        model_name = f'{task_config.orchestrator_server_config.load_model_config["model_repo"]} | {task_config.orchestrator_server_config.load_model_config["safetensors_filename"]}'
        decrypted_payload.model = model_name

    image_response = await get_image_from_server(
        httpx_client=fiber_config.httpx_client,
        body=decrypted_payload,
        post_endpoint=post_endpoint,
        worker_config=worker_config,
        timeout=30,
    )
    if image_response is None or (image_response.get("image_b64") is None and image_response.get("is_nsfw") is None):
        logger.debug(f"Image response: {image_response}")
        raise HTTPException(status_code=500, detail="Image generation failed")
    return payload_models.ImageResponse(**image_response)


async def text_to_image(
    decrypted_payload: payload_models.TextToImagePayload = Depends(
        partial(decrypt_general_payload, payload_models.TextToImagePayload)
    ),
    fiber_config: Config = Depends(get_fiber_config),
    worker_config: WorkerConfig = Depends(get_worker_config),
) -> payload_models.ImageResponse:
    return await _process_image_request(decrypted_payload, fiber_config, mcst.TEXT_TO_IMAGE_SERVER_ENDPOINT, worker_config)


async def image_to_image(
    decrypted_payload: payload_models.ImageToImagePayload = Depends(
        partial(decrypt_general_payload, payload_models.ImageToImagePayload)
    ),
    fiber_config: Config = Depends(get_fiber_config),
    worker_config: WorkerConfig = Depends(get_worker_config),
) -> payload_models.ImageResponse:
    return await _process_image_request(decrypted_payload, fiber_config, mcst.IMAGE_TO_IMAGE_SERVER_ENDPOINT, worker_config)


async def inpaint(
    decrypted_payload: payload_models.InpaintPayload = Depends(partial(decrypt_general_payload, payload_models.InpaintPayload)),
    fiber_config: Config = Depends(get_fiber_config),
    worker_config: WorkerConfig = Depends(get_worker_config),
) -> payload_models.ImageResponse:
    return await _process_image_request(decrypted_payload, fiber_config, mcst.INPAINT_SERVER_ENDPOINT, worker_config)


async def avatar(
    decrypted_payload: payload_models.AvatarPayload = Depends(partial(decrypt_general_payload, payload_models.AvatarPayload)),
    fiber_config: Config = Depends(get_fiber_config),
    worker_config: WorkerConfig = Depends(get_worker_config),
) -> payload_models.ImageResponse:
    return await _process_image_request(decrypted_payload, fiber_config, mcst.AVATAR_SERVER_ENDPOINT, worker_config)


def factory_router() -> APIRouter:
    router = APIRouter()
    router.add_api_route(
        "/text-to-image",
        text_to_image,
        tags=["Subnet"],
        methods=["POST"],
        dependencies=[Depends(blacklist_low_stake), Depends(verify_request)],
    )
    router.add_api_route(
        "/image-to-image",
        image_to_image,
        tags=["Subnet"],
        methods=["POST"],
        dependencies=[Depends(blacklist_low_stake), Depends(verify_request)],
    )
    router.add_api_route(
        "/inpaint",
        inpaint,
        tags=["Subnet"],
        methods=["POST"],
        dependencies=[Depends(blacklist_low_stake), Depends(verify_request)],
    )
    router.add_api_route(
        "/avatar",
        avatar,
        tags=["Subnet"],
        methods=["POST"],
        dependencies=[Depends(blacklist_low_stake), Depends(verify_request)],
    )
    return router
