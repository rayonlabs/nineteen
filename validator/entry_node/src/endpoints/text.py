import json
from typing import Any, AsyncGenerator
import asyncio
from fastapi import Depends, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from redis.asyncio import Redis
from fiber.logging_utils import get_logger
from fastapi.routing import APIRouter
from validator.entry_node.src.core.configuration import Config
from validator.entry_node.src.core.dependencies import get_config
from validator.entry_node.src.core.middleware import verify_api_key_rate_limit
from validator.utils.redis import redis_constants as rcst
from validator.utils.generic import generic_constants as gcst
from validator.entry_node.src.models import request_models
from validator.utils.query.query_utils import load_sse_jsons

logger = get_logger(__name__)

async def _construct_organic_message(payload: dict, job_id: str, task: str) -> str:
    return json.dumps({
        "query_type": gcst.ORGANIC,
        "query_payload": payload,
        "task": task,
        "job_id": job_id
    })

async def _wait_for_acknowledgement(redis_db: Redis, job_id: str, timeout: float = 2.0) -> bool:
    ack_key = rcst.get_ack_key(job_id)
    try:
        ack = await asyncio.wait_for(redis_db.get(ack_key), timeout=timeout)
        return ack is not None
    except asyncio.TimeoutError:
        return False

async def _cleanup_queues(redis_db: Redis, job_id: str):
    response_queue = rcst.get_response_queue_key(job_id)
    ack_key = rcst.get_ack_key(job_id)
    await redis_db.delete(response_queue, ack_key)

async def _stream_results(redis_db: Redis, job_id: str) -> AsyncGenerator[str, None]:
    response_queue = rcst.get_response_queue_key(job_id)
    
    try:
        while True:
            # use BLPOP with timeout to avoid infinite blocking
            result = await redis_db.blpop(response_queue, timeout=5)
            if result is None:
                logger.error(f"Timeout waiting for response in queue {response_queue}")
                break
                
            _, data = result
            content = json.loads(data.decode())
            
            if gcst.STATUS_CODE in content and content[gcst.STATUS_CODE] >= 400:
                raise HTTPException(status_code=content[gcst.STATUS_CODE], 
                                 detail=content[gcst.ERROR_MESSAGE])

            yield content[gcst.CONTENT]
            
            if "[DONE]" in content[gcst.CONTENT]:
                break
    finally:
        await _cleanup_queues(redis_db, job_id)

async def make_stream_organic_query(
    redis_db: Redis,
    payload: dict[str, Any],
    task: str,
) -> AsyncGenerator[str, None]:
    job_id = rcst.generate_job_id()
    organic_message = await _construct_organic_message(payload=payload, job_id=job_id, task=task)

    # create response queue with TTL
    response_queue = rcst.get_response_queue_key(job_id)
    await redis_db.expire(response_queue, rcst.RESPONSE_QUEUE_TTL)
    
    # push query to queue
    await redis_db.lpush(rcst.QUERY_QUEUE_KEY, organic_message)

    # wait for acknowledgment
    if not await _wait_for_acknowledgement(redis_db, job_id):
        logger.error(f"No acknowledgment received for job {job_id}")
        await _cleanup_queues(redis_db, job_id)
        raise HTTPException(status_code=500, detail="Unable to process request")

    return _stream_results(redis_db, job_id)

async def _handle_no_stream(text_generator: AsyncGenerator[str, None]) -> JSONResponse:
    all_content = ""
    async for chunk in text_generator:
        chunks = load_sse_jsons(chunk)
        if isinstance(chunks, list):
            for chunk in chunks:
                content = chunk["choices"][0]["delta"]["content"]
                all_content += content
                if content == "":
                    break

    return JSONResponse({"choices": [{"delta": {"content": all_content}}]})

async def chat(
    chat_request: request_models.ChatRequest,
    config: Config = Depends(get_config),
) -> StreamingResponse | JSONResponse:
    payload = request_models.chat_to_payload(chat_request)
    payload.temperature = 0.5

    try:
        text_generator = await make_stream_organic_query(
            redis_db=config.redis_db,
            payload=payload.model_dump(),
            task=payload.model
        )
        
        if chat_request.stream:
            return StreamingResponse(text_generator, media_type="text/event-stream")
        else:
            return await _handle_no_stream(text_generator)
            
    except HTTPException as http_exc:
        logger.error(f"HTTPException in chat endpoint: {str(http_exc)}")
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error in chat endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred")

router = APIRouter()
router.add_api_route(
    "/v1/chat/completions",
    chat,
    methods=["POST", "OPTIONS"],
    tags=["Text"],
    response_model=None,
    dependencies=[Depends(verify_api_key_rate_limit)],
)