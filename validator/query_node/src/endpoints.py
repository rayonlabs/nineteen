from fastapi import Depends, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
import time
from typing import Any, Annotated
from core import task_config as tcfg
from validator.utils.generic import generic_constants as gcst
from validator.query_node.src.query_config import Config
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
from fastapi.security import HTTPBearer


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

from contextlib import asynccontextmanager
from typing import AsyncGenerator, Any
from fastapi import Depends, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
import time
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

class StreamManager:
    """Manages stream lifecycle and ensures proper cleanup"""
    def __init__(self, generator: AsyncGenerator[str, None]):
        self.generator = generator
        self.started = False
        self.error: Exception | None = None

    async def stream(self) -> AsyncGenerator[str, None]:
        """Wrapper around generator that tracks stream state and errors"""
        self.started = True
        try:
            async for chunk in self.generator:
                yield chunk
        except Exception as e:
            self.error = e
            logger.error(f"Error during streaming: {str(e)}")
            # Re-raise to be caught by FastAPI's exception handlers
            raise
        finally:
            # Cleanup if needed
            if hasattr(self.generator, 'aclose'):
                await self.generator.aclose()

async def validate_stream(generator: AsyncGenerator[str, None]) -> AsyncGenerator[str, None]:
    try:
        first_chunk = await anext(generator)
        if not first_chunk:
            raise HTTPException(status_code=500, detail="Empty response from server")

        async def combined_generator() -> AsyncGenerator[str, None]:
            yield first_chunk
            async for chunk in generator:
                yield chunk

        return combined_generator()

    except StopAsyncIteration:
        raise HTTPException(status_code=500, detail="Empty response from server")
    except Exception as e:
        logger.error(f"Stream validation failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to initialize stream")
    
async def chat(
    chat_request: request_models.ChatRequest,
    config: Annotated[Any, Depends(get_config_dependency)],
    _: Annotated[None, Depends(verify_api_key_dependency)]
) -> StreamingResponse | JSONResponse:
    """
    Chat endpoint with proper error handling and resource management
    """
    job_id = rutils.generate_job_id()
    start_time = time.time()
    stream_manager = None

    try:
        payload = request_models.chat_to_payload(chat_request)
        message = rdc.QueryQueueMessage(
            task=payload.model,
            query_type=gcst.ORGANIC,
            job_id=job_id,
            query_payload=payload.model_dump()
        )
        raw_generator = process_organic_stream(config, message, start_time)        
        validated_generator = await validate_stream(raw_generator)        
        stream_manager = StreamManager(validated_generator)

        if chat_request.stream:
            return StreamingResponse(
                stream_manager.stream(),
                media_type="text/event-stream",
                status_code=200,
                headers={
                    "X-Job-ID": job_id
                }
            )
        else:
            try:
                response = await _handle_no_stream(validated_generator)
                return response
            except Exception as e:
                if hasattr(validated_generator, 'aclose'):
                    await validated_generator.aclose()
                raise

    except HTTPException:
        logger.error(f"HTTP error in chat endpoint for job {job_id}")
        raise

    except Exception as e:
        error_msg = f"Unexpected error in chat endpoint for job {job_id}: {str(e)}"
        logger.error(error_msg)
        if stream_manager and stream_manager.started:
            logger.error("Stream was started before error occurred")
            if hasattr(stream_manager.generator, 'aclose'):
                await stream_manager.generator.aclose()
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