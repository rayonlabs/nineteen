import time
from typing import AsyncGenerator
from fastapi import HTTPException
from redis.asyncio import Redis
from core.models.payload_models import ImageResponse
from validator.utils.query.query_utils import load_sse_jsons
from fastapi.responses import JSONResponse
import json
from validator.models import Contender
from validator.query_node.src.query_config import Config
from core import task_config as tcfg
from validator.utils.contender import contender_utils as putils
from fiber.logging_utils import get_logger
from asyncpg import Connection
from validator.utils.redis import redis_dataclasses as rdc
from validator.query_node.src.query import nonstream, streaming
from validator.db.src.sql.contenders import get_contenders_for_task
from validator.db.src.sql.nodes import get_node
from validator.utils.generic import generic_constants as gcst
from opentelemetry import metrics

logger = get_logger(__name__)

COUNTER_TOTAL_QUERIES = metrics.get_meter(__name__).create_counter(
    name="validator.query_node.process.total_queries",
    description="Number of total queries sent to `process_task`",
)

COUNTER_FAILED_QUERIES = metrics.get_meter(__name__).create_counter(
    name="validator.query_node.process.failed_queries",
    description="Number of failed queries within `process_task`",
)

QUERY_NODE_REQUESTS_PROCESSING_GAUGE = metrics.get_meter(__name__).create_gauge(
    name="validator.query_node.src.concurrent_synthetic_queries_processing",
    description="concurrent number of synthetic requests currently being processed",
    unit="1"
)

QUERY_NODE_FAILED_SYNTHETIC_TASKS_COUNTER = metrics.get_meter(__name__).create_counter(
    name="validator.query_node.src.query_node_failed_synthetic_tasks",
    description="number of failed synthetic `process_task` instances",
    unit="1"
)

COUNTER_TEXT_GENERATION_ERROR = metrics.get_meter(__name__).create_counter("validator.query_node.text.error")
COUNTER_TEXT_GENERATION_SUCCESS = metrics.get_meter(__name__).create_counter("validator.query_node.text.success")
COUNTER_IMAGE_ERROR = metrics.get_meter(__name__).create_counter("validator.query_node.image.error")
COUNTER_IMAGE_SUCCESS = metrics.get_meter(__name__).create_counter("validator.query_node.image.success")
GAUGE_TOKENS_PER_SEC = metrics.get_meter(__name__).create_gauge(
    "validator.query_node.text.tokens_per_sec",
    description="Average tokens per second metric for LLM streaming"
)


async def _decrement_requests_remaining(redis_db: Redis, task: str):
    """Decrement remaining synthetic requests counter."""
    key = f"task_synthetics_info:{task}:requests_remaining"
    await redis_db.decr(key)

import asyncio
lock = asyncio.Lock()

async def _get_contenders(connection: Connection, task: str, query_type: str) -> list[Contender]:
    try:
        async with lock:
            contenders = await get_contenders_for_task(connection, task, 5, query_type)
        return contenders
    except Exception as e:
        logger.error(f"Error getting contenders: {e}")
        raise

async def _handle_no_stream(text_generator: AsyncGenerator[str, None]) -> JSONResponse:
    all_content = ""
    try:
        async for chunk in text_generator:
            try:
                chunks = load_sse_jsons(chunk)
                if not isinstance(chunks, list):
                    raise HTTPException(status_code=500, detail="Invalid response format")

                for chunk in chunks:
                    content = chunk["choices"][0]["delta"]["content"]
                    all_content += content
                    if content == "":
                        break

            except (json.JSONDecodeError, KeyError, IndexError) as e:
                logger.error(f"Error processing chunk: {e}")
                raise HTTPException(status_code=500, detail="Invalid response format")

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


async def _handle_stream_synthetic(
    config: Config, 
    message: rdc.QueryQueueMessage, 
    contenders: list[Contender]
) -> bool:
    start = time.time()
    for i, contender in enumerate(contenders):
        node = await get_node(config.psql_db, contender.node_id, config.netuid)
        if not node:
            logger.error(f"Node {contender.node_id} not found in database for netuid {config.netuid}")
            continue
            
        logger.info(f"Querying node {contender.node_id} for task {contender.task}")
        generator = await streaming.query_node_stream(
            config=config, 
            contender=contender, 
            payload=message.query_payload, 
            node=node
        )

        if not generator:
            logger.info(f"Failed to get generator from node {i}")
            continue

        success = await streaming.consume_synthetic_generator(
            config=config,
            generator=generator,
            contender=contender,
            node=node,
            payload=message.query_payload,
            start_time=start,
        )
        
        if success:
            return True
            
    error_msg = f"Service for task {message.task} is not responding, please try again"
    logger.error(error_msg)
    return False

async def _handle_stream_organic(
    config: Config, 
    message: rdc.QueryQueueMessage, 
    contenders: list[Contender]
) -> AsyncGenerator[str, None]:
    """Handle streaming organic queries."""
    start = time.time()
    for i, contender in enumerate(contenders):
        node = await get_node(config.psql_db, contender.node_id, config.netuid)
        if not node:
            logger.error(f"Node {contender.node_id} not found in database for netuid {config.netuid}")
            continue
            
        logger.info(f"Querying node {contender.node_id} for task {contender.task}")
        generator = await streaming.query_node_stream(
            config=config, 
            contender=contender, 
            payload=message.query_payload, 
            node=node
        )

        if not generator:
            logger.info(f"Failed to get generator from node {i}")
            continue

        async for chunk in streaming.consume_organic_generator(
            config=config,
            generator=generator,
            contender=contender,
            node=node,
            payload=message.query_payload,
            start_time=start,
        ):
            yield chunk
        return 
            
    raise HTTPException(
        status_code=500,
        detail=f"Service for task {message.task} is not responding, please try again"
    )

async def _handle_nonstream_query(
    config: Config, 
    message: rdc.QueryQueueMessage, 
    contenders: list[Contender]
) -> bool | AsyncGenerator[str, None]:
    start = time.time()
    is_synthetic = message.query_type == gcst.SYNTHETIC
    
    for contender in contenders:
        node = await get_node(config.psql_db, contender.node_id, config.netuid)
        if not node:
            logger.error(f"Node {contender.node_id} not found in database for netuid {config.netuid}")
            continue
            
        logger.info(f"Querying node {contender.node_id} for task {contender.task}")
        result = await nonstream.query_nonstream(
            config=config,
            contender=contender,
            node=node,
            payload=message.query_payload,
            response_model=ImageResponse,
            synthetic_query=is_synthetic,
        )
        
        if is_synthetic:
            if isinstance(result, bool) and result:
                return True
        else:
            if isinstance(result, AsyncGenerator):
                return result
        
    error_msg = f"Service for task {message.task} is not responding after trying {len(contenders)} contenders"
    if is_synthetic:
        logger.error(error_msg)
        return False
    else:
        raise HTTPException(status_code=500, detail=error_msg)
    
async def process_synthetic_task(config: Config, message: rdc.QueryQueueMessage) -> bool:
    task = message.task
    
    try:
        message.query_payload = await putils.get_synthetic_payload(config.redis_db, task)
        await _decrement_requests_remaining(config.redis_db, task)

        task_config = tcfg.get_enabled_task_config(task)
        if not task_config:
            error_msg = f"Can't find the task {task}, please try again later"
            logger.error(f"Can't find the task {task} in the query node!")
            COUNTER_FAILED_QUERIES.add(1, {
                "task": task,
                "synthetic_query": "true"
            })
            return False
            
        async with await config.psql_db.connection() as connection:
            contenders = await _get_contenders(connection, task, message.query_type)
        
        COUNTER_TOTAL_QUERIES.add(1, {
            "task": task,
            "synthetic_query": "true"
        })
        
        if task_config.is_stream:
            return await _handle_stream_synthetic(config, message, contenders)
        else:
            result = await _handle_nonstream_query(config, message, contenders)
            if isinstance(result, bool):
                return result
            else:
                logger.error(f"Unexpected AsyncGenerator result for synthetic task {task}")
                return False

    except Exception as e:
        logger.error(f"Error processing synthetic task {task}: {e}")
        COUNTER_FAILED_QUERIES.add(1, {
            "task": task,
            "synthetic_query": "true"
        })
        return False
async def process_organic_task(config: Config, message: rdc.QueryQueueMessage) -> AsyncGenerator[str, None]:
    task = message.task
    
    try:
        await _decrement_requests_remaining(config.redis_db, task)
        task_config = tcfg.get_enabled_task_config(task)
        if not task_config:
            raise HTTPException(
                status_code=500,
                detail=f"Can't find the task {task}, please try again later"
            )
            
        async with await config.psql_db.connection() as connection:
            contenders = await _get_contenders(connection, task, message.query_type)
        
        COUNTER_TOTAL_QUERIES.add(1, {
            "task": task,
            "synthetic_query": "false"
        })
        
        if task_config.is_stream:
            async for chunk in _handle_stream_organic(config, message, contenders):
                yield chunk
        else:
            result = await _handle_nonstream_query(config, message, contenders)
            if isinstance(result, bool):
                raise HTTPException(
                    status_code=500,
                    detail=f"Unexpected boolean result for organic task {task}"
                )
            else:
                async for chunk in result:
                    yield chunk

    except Exception as e:
        logger.error(f"Error processing organic task {task}: {e}")
        COUNTER_FAILED_QUERIES.add(1, {
            "task": task,
            "synthetic_query": "false"
        })
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(status_code=500, detail=str(e))
    
async def process_task(config: Config, message: rdc.QueryQueueMessage) -> AsyncGenerator[str, None] | bool:
    """Process task based on query type."""
    if message.query_type == gcst.SYNTHETIC:
        return await process_synthetic_task(config, message)
    else:
        return process_organic_task(config, message)
    
async def process_organic_stream(
    config: Config,
    message: rdc.QueryQueueMessage,
    start_time: float
) -> AsyncGenerator[str, None]:
    try:
        num_tokens = 0
        async for chunk in process_organic_task(config, message):
            num_tokens += 1
            yield chunk

        COUNTER_TEXT_GENERATION_SUCCESS.add(1, {"task": message.task})
        if num_tokens > 0:
            completion_time = time.time() - start_time
            GAUGE_TOKENS_PER_SEC.set(num_tokens / completion_time, {"task": message.task})

    except Exception as e:
        logger.error(f"Error in stream processing: {str(e)}")
        COUNTER_TEXT_GENERATION_ERROR.add(1, {
            "task": message.task,
            "error": type(e).__name__
        })
        raise

async def process_image_request(
    config: Config,
    payload: payload_models.TextToImagePayload | payload_models.ImageToImagePayload | payload_models.InpaintPayload | payload_models.AvatarPayload,
    task: str
) -> JSONResponse:
    task = task.replace("_", "-")
    task_config = tcfg.get_enabled_task_config(task)
    if task_config is None:
        COUNTER_IMAGE_ERROR.add(1, {"reason": "no_task_config"})
        logger.error(f"Task config not found for task: {task}")
        raise HTTPException(status_code=400, detail=f"Invalid model {task}")
        
    job_id = rutils.generate_job_id()
    message = rdc.QueryQueueMessage(
        task=task,
        query_type=gcst.ORGANIC,
        job_id=job_id,
        query_payload=payload.model_dump()
    )
        
    try:
        generator = process_organic_task(config, message)
        response_content = None
        
        async for chunk in generator:
            try:
                chunks = load_sse_jsons(chunk)                
                if not isinstance(chunks, list):
                    logger.error(f"Expected list of chunks but got {type(chunks)}")
                    continue
                for chunk_data in chunks:
                    try:
                        content = chunk_data["choices"][0]["delta"]["content"]
                        try:
                            response_json = json.loads(content)
                            response_content = response_json
                            break
                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to parse content as JSON: {e}")
                            continue
                    except (KeyError, IndexError) as e:
                        logger.error(f"Error extracting content from chunk_data: {e}")
                        continue
                if response_content:
                    break
                    
            except (json.JSONDecodeError, KeyError, IndexError) as e:
                logger.error(f"Error processing chunk: {e}")
                continue
                
        if not response_content:
            logger.error(f"No valid response content received for job {job_id}")
            raise HTTPException(status_code=500, detail="No response received")
            
        try:
            logger.debug(f"Attempting to create ImageResponse from: {response_content}")
            image_response = payload_models.ImageResponse(**response_content)
        except Exception as e:
            logger.error(f"Failed to create ImageResponse: {e}")
            logger.error(f"Response content that caused error: {response_content}")
            raise HTTPException(status_code=500, detail="Invalid response format")
            
        if image_response.is_nsfw:
            logger.warning(f"NSFW content detected for job {job_id}")
            COUNTER_IMAGE_ERROR.add(1, {"task": task, "kind": "nsfw"})
            raise HTTPException(status_code=403, detail="NSFW content detected")
            
        if not image_response.image_b64:
            logger.error(f"No image data received for job {job_id}")
            COUNTER_IMAGE_ERROR.add(1, {"task": task, "kind": "no_image"})
            raise HTTPException(status_code=500, detail="No image generated")
            
        COUNTER_IMAGE_SUCCESS.add(1, {"task": task})
        return JSONResponse(content={"image_b64": image_response.image_b64})
        
    except Exception as e:
        logger.error(f"Error processing image request for job {job_id}: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Traceback: {''.join(traceback.format_tb(e.__traceback__))}")
        COUNTER_IMAGE_ERROR.add(1, {"task": task, "error": str(e)})
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(status_code=500, detail=str(e))
    
