from fastapi import Depends, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.security import HTTPBearer
import time
from typing import Any, Annotated
from core import task_config as tcfg
from validator.utils.generic import generic_constants as gcst
from validator.utils.redis import redis_dataclasses as rdc
from validator.query_node.src import request_models
from validator.query_node.src.process_queries import process_organic_image_request, process_organic_stream, _handle_no_stream
from validator.query_node.core.middleware import verify_api_key_rate_limit
import validator.utils.redis.redis_utils as rutils
from fastapi.security import HTTPAuthorizationCredentials
from fastapi import Security
from fastapi.routing import APIRouter
from fiber.logging_utils import get_logger
from validator.query_node.core.configuration import load_config



auth_scheme = HTTPBearer()
logger = get_logger(__name__)

async def get_config_dependency():
    config = await load_config()
    return config

async def verify_api_key_dependency(
    config: Annotated[Any, Depends(get_config_dependency)],
    credentials: Annotated[HTTPAuthorizationCredentials, Security(auth_scheme)]
):
    await verify_api_key_rate_limit(config, credentials.credentials)

async def image_to_image(
    request: request_models.ImageToImageRequest,
    config: Annotated[Any, Depends(get_config_dependency)],
    _: Annotated[None, Depends(verify_api_key_dependency)]
) -> JSONResponse:
    payload = await request_models.image_to_image_to_payload(
        request,
        httpx_client=config.httpx_client,
        prod=config.prod,
    )
    return await process_organic_image_request(config, payload, payload.model)

async def inpaint(
    request: request_models.InpaintRequest,
    config: Annotated[Any, Depends(get_config_dependency)],
    _: Annotated[None, Depends(verify_api_key_dependency)]
) -> JSONResponse:
    payload = await request_models.inpaint_to_payload(
        request, 
        httpx_client=config.httpx_client, 
        prod=config.prod
    )
    return await process_organic_image_request(config, payload, "inpaint")

async def avatar(
    request: request_models.AvatarRequest,
    config: Annotated[Any, Depends(get_config_dependency)],
    _: Annotated[None, Depends(verify_api_key_dependency)]
) -> JSONResponse:
    payload = await request_models.avatar_to_payload(
        request, 
        httpx_client=config.httpx_client, 
        prod=config.prod
    )
    return await process_organic_image_request(config, payload, "avatar")

async def text_to_image(
    request: request_models.TextToImageRequest,
    config: Annotated[Any, Depends(get_config_dependency)],
    _: Annotated[None, Depends(verify_api_key_dependency)]
) -> JSONResponse:
    payload = request_models.text_to_image_to_payload(request)
    return await process_organic_image_request(config, payload, payload.model)


image_router = APIRouter(tags=["Image"])

image_router.add_api_route("/v1/text-to-image", text_to_image, methods=["POST"], response_model=None)
image_router.add_api_route("/v1/image-to-image", image_to_image, methods=["POST"], response_model=None)
image_router.add_api_route("/v1/inpaint", inpaint, methods=["POST"], response_model=None)
image_router.add_api_route("/v1/avatar", avatar, methods=["POST"], response_model=None)


## text

    
async def chat(
    chat_request: request_models.ChatRequest,
    config: Annotated[Any, Depends(get_config_dependency)],
    _: Annotated[None, Depends(verify_api_key_dependency)]
) -> StreamingResponse | JSONResponse:

    job_id = rutils.generate_job_id()
    start_time = time.time()

    try:
        payload = request_models.chat_to_payload(chat_request)
        message = rdc.QueryQueueMessage(
            task=payload.model,
            query_type=gcst.ORGANIC,
            job_id=job_id,
            query_payload=payload.model_dump()
        )
        text_generator = process_organic_stream(config, message, start_time)        

        if chat_request.stream:
            return StreamingResponse(
                text_generator,
                media_type="text/event-stream",
                status_code=200
            )
        else:
            try:
                response = await _handle_no_stream(text_generator) # type: ignore
                return response
            except Exception as e:
                if hasattr(text_generator, 'aclose'):
                    await text_generator.aclose()
                raise

    except HTTPException:
        logger.error(f"HTTP error in chat endpoint for job {job_id}")
        raise

    except Exception as e:
        error_msg = f"Unexpected error in chat endpoint for job {job_id}: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail="An unexpected error occurred")

    finally:
        duration = time.time() - start_time
        logger.info(f"Chat request {job_id} completed in {duration:.2f}s")

text_router = APIRouter(tags=["Text"])
text_router.add_api_route(
    "/v1/chat/completions",
    chat,
    methods=["POST", "OPTIONS"],
    response_model=None,
)

## common

async def models(
    config: Annotated[Any, Depends(get_config_dependency)],
    _: Annotated[None, Depends(verify_api_key_dependency)]
) -> list[dict[str, Any]]:
    models = tcfg.get_public_task_configs()
    new_models = []
    for model in models:
        new_model = {"model_name": model["task"]} 
        new_model.update({k: v for k, v in model.items() if k != "task"})
        new_models.append(new_model)
    return new_models

generic_router = APIRouter(tags=["Models"])
generic_router.add_api_route(
    "/v1/models",
    models,
    methods=["GET"],
    response_model=None,
)