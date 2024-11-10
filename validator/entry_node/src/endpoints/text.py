import json
from typing import Any, AsyncGenerator
import asyncio
from fastapi import Depends, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from opentelemetry import metrics
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
import validator.utils.redis.redis_utils as rutils
from opentelemetry import metrics
import time

logger = get_logger(__name__)

COUNTER_TEXT_GENERATION_ERROR = metrics.get_meter(__name__).create_counter("validator.entry_node.text.error")
COUNTER_TEXT_GENERATION_SUCCESS = metrics.get_meter(__name__).create_counter("validator.entry_node.text.success")
GAUGE_TOKENS_PER_SEC = metrics.get_meter(__name__).create_gauge(
    "validator.entry_node.text.tokens_per_sec",
    description="Average tokens per second metric for LLM streaming for an organic LLM query"
)

async def _construct_organic_message(payload: dict, job_id: str, task: str) -> str:
    return json.dumps({
        "query_type": gcst.ORGANIC,
        "query_payload": payload,
        "task": task,
        "job_id": job_id
    })

async def _wait_for_acknowledgement(redis_db: Redis, job_id: str, start: float, timeout: float = 2) -> bool:
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
        logger.error(f"Error waiting for acknowledgment: {e}")
        return False

async def _cleanup_queues(redis_db: Redis, job_id: str):
    response_queue = await rutils.get_response_queue_key(job_id)
    await redis_db.delete(response_queue)

async def _stream_results(redis_db: Redis, job_id: str, task: str, start_time: float, timeout: float = rcst.RESPONSE_QUEUE_TTL) -> AsyncGenerator[str, None]:
    response_queue = await rutils.get_response_queue_key(job_id)
    received_done = False
    
    try:
        num_tokens = 0
        while True:
            result = await redis_db.blpop(response_queue, timeout=timeout)
            if result is None:
                logger.error(f"Timeout waiting for response in queue {response_queue}")
                raise HTTPException(status_code=500, detail="Request timed out")

            _, data = result
            try:
                if not data:
                    continue

                content = json.loads(data.decode())
                logger.debug(f"Received content from queue: {content}")
                
                if gcst.STATUS_CODE in content and content[gcst.STATUS_CODE] >= 400:
                    logger.error(f"Error response received: {content}")
                    COUNTER_TEXT_GENERATION_ERROR.add(1, {"task": task, "kind": "nth_chunk_timeout", "status_code": content[gcst.STATUS_CODE]})
                    raise HTTPException(
                        status_code=content[gcst.STATUS_CODE],
                        detail=content.get(gcst.ERROR_MESSAGE, "Unknown error")
                    )

                if gcst.CONTENT not in content:
                    logger.warning(f"Malformed message received: {content}")
                    continue

                content_str = content[gcst.CONTENT]
                num_tokens += 1
                yield content_str

                if "[DONE]" in content_str:
                    received_done = True
                    break

            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode message '{data}': {e}")
                raise HTTPException(status_code=500, detail="Invalid response format")

        COUNTER_TEXT_GENERATION_SUCCESS.add(1, {"task": task, "status_code": 200})
        completion_time = time.time() - start_time

        tps = num_tokens / completion_time
        GAUGE_TOKENS_PER_SEC.set(tps, {"task": task})
        logger.info(f"Tokens per second for job_id: {job_id}, task: {task}: {tps}")

    finally:
        await _cleanup_queues(redis_db, job_id)
        if not received_done:
            logger.error(f"Stream ended without [DONE] marker for job {job_id}")
            raise HTTPException(status_code=500, detail="Incomplete response")

async def make_stream_organic_query(
    redis_db: Redis,
    payload: dict[str, Any],
    task: str,
) -> AsyncGenerator[str, None]:
    job_id = rutils.generate_job_id()
    organic_message = await _construct_organic_message(payload=payload, job_id=job_id, task=task)

    start_time = time.time()
    try:
        await rutils.ensure_queue_clean(redis_db, job_id)
        start = time.time()
        await redis_db.lpush(rcst.QUERY_QUEUE_KEY, organic_message)
        if not await _wait_for_acknowledgement(redis_db, job_id, start):
            logger.error(f"No acknowledgment received for job {job_id}")
            COUNTER_TEXT_GENERATION_ERROR.add(1, {"task": task, "kind": "redis_acknowledgement_timeout", "status_code": 500})
            await _cleanup_queues(redis_db, job_id)
            raise HTTPException(status_code=500, detail="Unable to process request")

        logger.debug(f"Query setup complete for job {job_id}, streaming results...")
        return _stream_results(redis_db, job_id, task, start_time)
    except Exception as e:
        logger.error(f"Error in query setup: {str(e)}")
        COUNTER_TEXT_GENERATION_ERROR.add(1, {"task": task, "kind": f"{str(e)}", "status_code": 500})
        await _cleanup_queues(redis_db, job_id)
        raise

async def _handle_no_stream(text_generator: AsyncGenerator[str, None]) -> JSONResponse:
    all_content = ""
    try:
        async for chunk in text_generator:
            logger.debug(f"Received chunk: {chunk}")
            
            try:
                chunks = load_sse_jsons(chunk)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode chunk: {e}")
                raise HTTPException(status_code=500, detail="Invalid response format")

            if not isinstance(chunks, list):
                logger.error(f"Unexpected chunk format: {chunks}")
                raise HTTPException(status_code=500, detail="Invalid response format")

            for chunk in chunks:
                try:
                    content = chunk["choices"][0]["delta"]["content"]
                    all_content += content
                    if content == "":
                        break
                except (KeyError, IndexError) as e:
                    logger.error(f"Malformed chunk structure: {e}")
                    raise HTTPException(status_code=500, detail="Invalid response structure")

        return JSONResponse({
            "choices": [{
                "delta": {"content": all_content}
            }]
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in non-streaming response: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to process response")

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
            task=payload.model,
        )

        logger.info("Here returning a response!")

        if chat_request.stream:
            return StreamingResponse(text_generator, media_type="text/event-stream")
        else:
            return await _handle_no_stream(text_generator)

    except HTTPException as http_exc:
        COUNTER_TEXT_GENERATION_ERROR.add(1, {"task": payload.model, "kind": type(http_exc).__name__, "status_code": 500})
        logger.info(f"HTTPException in chat endpoint: {str(http_exc)}")
        raise http_exc

    except Exception as e:
        COUNTER_TEXT_GENERATION_ERROR.add(1, {"task": payload.model, "kind": type(e).__name__, "status_code": 500})
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