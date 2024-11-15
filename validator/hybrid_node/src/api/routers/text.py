

from validator.hybrid_node.src.api import request_models
from validator.hybrid_node.src.handlers.process_queries import process_organic_stream, _handle_no_stream
from validator.hybrid_node.src.api.dependencies.deps import get_config_dependency
from validator.hybrid_node.src.api.dependencies.deps import verify_api_key_dependency
import validator.utils.redis.redis_utils as rutils
from validator.utils.redis import redis_dataclasses as rdc
from validator.utils.generic import generic_constants as gcst

from fastapi.routing import APIRouter
from fastapi.responses import JSONResponse
from fastapi import Depends
from typing import Any, Annotated
from fastapi.responses import StreamingResponse
from fastapi import HTTPException
from fiber.logging_utils import get_logger

import time

logger = get_logger(__name__)


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