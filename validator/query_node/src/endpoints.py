from fastapi import Depends, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
import time
from typing import Any
from core import task_config as tcfg
from validator.utils.generic import generic_constants as gcst
from validator.query_node.src.query_config import Config
from validator.utils.redis import redis_dataclasses as rdc
from validator.query_node.src import request_models
from validator.query_node.src.process_queries import process_image_request, process_organic_stream, _handle_no_stream
from validator.query_node.core.middleware import verify_api_key_rate_limit
import validator.utils.redis.redis_utils as rutils
from fastapi.security import HTTPAuthorizationCredentials
from fastapi import Security
from fastapi.routing import APIRouter
from fiber.logging_utils import get_logger
from validator.query_node.core.configuration import load_config
from fastapi.security import HTTPBearer


auth_scheme = HTTPBearer()
logger = get_logger(__name__)

async def get_config_dependency():
    config = await load_config()
    return config

async def image_to_image(
    request: request_models.ImageToImageRequest,
    config: Config = Depends(get_config_dependency),
    credentials: HTTPAuthorizationCredentials = Security(auth_scheme)
) -> JSONResponse:
    await verify_api_key_rate_limit(config, credentials.credentials)
    payload = await request_models.image_to_image_to_payload(
        request,
        httpx_client=config.httpx_client,
        prod=config.prod,
    )
    return await process_image_request(config, payload, payload.model)

async def inpaint(
    request: request_models.InpaintRequest,
    config: Config = Depends(get_config_dependency),
    credentials: HTTPAuthorizationCredentials = Security(auth_scheme)
) -> JSONResponse:
    await verify_api_key_rate_limit(config, credentials.credentials)
    payload = await request_models.inpaint_to_payload(
        request, 
        httpx_client=config.httpx_client, 
        prod=config.prod
    )
    return await process_image_request(config, payload, "inpaint")

async def avatar(
    request: request_models.AvatarRequest,
    config: Config = Depends(get_config_dependency),
    credentials: HTTPAuthorizationCredentials = Security(auth_scheme)
) -> JSONResponse:
    await verify_api_key_rate_limit(config, credentials.credentials)
    payload = await request_models.avatar_to_payload(
        request, 
        httpx_client=config.httpx_client, 
        prod=config.prod
    )
    return await process_image_request(config, payload, "avatar")

async def text_to_image(
    request: request_models.TextToImageRequest,
    config: Config = Depends(get_config_dependency),
    credentials: HTTPAuthorizationCredentials = Security(auth_scheme)
) -> JSONResponse:
    await verify_api_key_rate_limit(config, credentials.credentials)
    payload = request_models.text_to_image_to_payload(request)
    return await process_image_request(config, payload, payload.model)


image_router = APIRouter(
    tags=["Image"],
    dependencies=[Depends(verify_api_key_rate_limit)],
)

image_router.add_api_route("/v1/text-to-image", text_to_image, methods=["POST"])
image_router.add_api_route("/v1/image-to-image", image_to_image, methods=["POST"])
image_router.add_api_route("/v1/inpaint", inpaint, methods=["POST"])
image_router.add_api_route("/v1/avatar", avatar, methods=["POST"])


## text

async def chat(
    chat_request: request_models.ChatRequest,
    config: Config = Depends(get_config_dependency),
) -> StreamingResponse | JSONResponse:
    payload = request_models.chat_to_payload(chat_request)
    job_id = rutils.generate_job_id()
    start_time = time.time()

    try:
        message = rdc.QueryQueueMessage(
            task=payload.model,
            query_type=gcst.ORGANIC,
            job_id=job_id,
            query_payload=payload.model_dump()
        )

        text_generator = process_organic_stream(config, message, start_time)

        if chat_request.stream:
            return StreamingResponse(text_generator, media_type="text/event-stream")
        else:
            return await _handle_no_stream(text_generator)

    except HTTPException as http_exc:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in chat endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred")


text_router = APIRouter()
text_router.add_api_route(
    "/v1/chat/completions",
    chat,
    methods=["POST", "OPTIONS"],
    tags=["Text"],
    response_model=None,
    dependencies=[Depends(verify_api_key_rate_limit)],
)

## common

async def models() -> list[dict[str, Any]]:
    models = tcfg.get_public_task_configs()
    new_models = []
    for model in models:
        new_model = {"model_name": model["task"]} 
        new_model.update({k: v for k, v in model.items() if k != "task"})
        new_models.append(new_model)
    return new_models

generic_router = APIRouter()
generic_router.add_api_route(
    "/v1/models",
    models,
    methods=["GET"],
    tags=["Models"],
    response_model=None,
    dependencies=[Depends(verify_api_key_rate_limit)],
)

