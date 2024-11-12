import time
from typing import AsyncGenerator
from fastapi import HTTPException
from redis.asyncio import Redis
from core.models.payload_models import ImageResponse
from validator.utils.query.query_utils import load_sse_jsons
from fastapi.responses import JSONResponse
import validator.utils.redis.redis_utils as rutils
from validator.models import Contender
from validator.query_node.src.query_config import Config
from core.models import payload_models
from core import task_config as tcfg
from validator.utils.contender import contender_utils as putils
from fiber.logging_utils import get_logger
from asyncpg import Connection
import traceback
from validator.utils.redis import redis_dataclasses as rdc
from validator.query_node.src.query import nonstream, streaming
from validator.db.src.sql.contenders import get_contenders_for_task
from validator.db.src.sql.nodes import get_node
from validator.utils.generic import generic_constants as gcst
from opentelemetry import metrics
import asyncio

logger = get_logger(__name__)

COUNTER_TOTAL_QUERIES = metrics.get_meter(__name__).create_counter(
    name="validator.entry_node.process.total_queries",
    description="Number of total queries sent to `process_task`",
)

COUNTER_FAILED_QUERIES = metrics.get_meter(__name__).create_counter(
    name="validator.entry_node.process.failed_queries",
    description="Number of failed queries within `process_task`",
)

QUERY_NODE_REQUESTS_PROCESSING_GAUGE = metrics.get_meter(__name__).create_gauge(
    name="validator.entry_node.src.concurrent_synthetic_queries_processing",
    description="concurrent number of synthetic requests currently being processed",
    unit="1"
)

QUERY_NODE_FAILED_SYNTHETIC_TASKS_COUNTER = metrics.get_meter(__name__).create_counter(
    name="validator.entry_node.src.query_node_failed_synthetic_tasks",
    description="number of failed synthetic `process_task` instances",
    unit="1"
)

COUNTER_TEXT_GENERATION_ERROR = metrics.get_meter(__name__).create_counter("validator.entry_node.text.error")
COUNTER_TEXT_GENERATION_SUCCESS = metrics.get_meter(__name__).create_counter("validator.entry_node.text.success")
COUNTER_IMAGE_ERROR = metrics.get_meter(__name__).create_counter("validator.entry_node.image.error")
COUNTER_IMAGE_SUCCESS = metrics.get_meter(__name__).create_counter("validator.entry_node.image.success")

GAUGE_TOKENS_PER_SEC = metrics.get_meter(__name__).create_gauge(
    "validator.entry_node.text.tokens_per_sec",
    description="Average tokens per second metric for LLM streaming"
)

lock = asyncio.Lock()


async def _decrement_requests_remaining(redis_db: Redis, task: str):
    """Decrement remaining synthetic requests counter."""
    key = f"task_synthetics_info:{task}:requests_remaining"
    await redis_db.decr(key)

async def _get_contenders(connection: Connection, task: str, query_type: str) -> list[Contender]:
    try:
        async with lock:
            contenders = await get_contenders_for_task(connection, task, 5, query_type)
        logger.info(f"Selected {len(contenders)} contenders for organic task : {task}")
        return contenders
    except Exception as e:
        logger.error(f"Error getting contenders: {e}")
        raise

async def _handle_no_stream(text_generator: AsyncGenerator[str, str]) -> JSONResponse:
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
        try:
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
        except Exception:
            logger.error(f"Some issue querying node id {contender.node_id} for task {message.task}\n{traceback.format_exc()}")

            
    raise HTTPException(
        status_code=500,
        detail=f"Service for task {message.task} is not responding for all contenders!"
    )

async def _handle_nonstream_img_query(
    config: Config, 
    message: rdc.QueryQueueMessage, 
    contenders: list[Contender]
) -> ImageResponse | None:
    is_synthetic = message.query_type == gcst.SYNTHETIC
    
    for contender in contenders:
        node = await get_node(config.psql_db, contender.node_id, config.netuid)
        if not node:
            logger.error(f"Node {contender.node_id} not found in database for netuid {config.netuid}")
            continue
            
        logger.info(f"Querying node {contender.node_id} for task {contender.task}")
        result = await nonstream.query_nonstream_img(
            config=config,
            contender=contender,
            node=node,
            payload=message.query_payload,
            response_model=ImageResponse,
            synthetic_query=is_synthetic,
        )
        if result:
            return result
        
    error_msg = f"Service for task {message.task} is not responding after trying {len(contenders)} contenders"
    logger.error(error_msg)
    raise HTTPException(status_code=500, detail=error_msg)
    
async def process_synthetic_task(config: Config, message: rdc.QueryQueueMessage) -> bool:
    task = message.task
    
    try:
        message.query_payload = await putils.get_synthetic_payload(config.redis_db, task)
        await _decrement_requests_remaining(config.redis_db, task)

        task_config = tcfg.get_enabled_task_config(task)
        if not task_config:
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
            result = await _handle_nonstream_img_query(config, message, contenders)
            if result:
                return True
            else:
                return False

    except Exception as e:
        logger.error(f"Error processing synthetic task {task}: {e} - \n{traceback.format_exc()}")
        COUNTER_FAILED_QUERIES.add(1, {
            "task": task,
            "synthetic_query": "true"
        })
        return False
    
async def process_organic_img_task(config: Config, message: rdc.QueryQueueMessage) -> ImageResponse:
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
        
        result = await _handle_nonstream_img_query(config, message, contenders)
        return result

    except Exception as e:
        logger.error(f"Error processing organic task {task}: {e} - \n{traceback.format_exc()}")
        COUNTER_FAILED_QUERIES.add(1, {
            "task": task,
            "synthetic_query": "false"
        })
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(status_code=500, detail=str(e))
    
    

async def get_organic_stream(config: Config, message: rdc.QueryQueueMessage) -> AsyncGenerator[str, None]:
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
        
        async for chunk in _handle_stream_organic(config, message, contenders):
            yield chunk

    except Exception as e:
        logger.error(f"Error processing organic task {task}: {e} - \n{traceback.format_exc()}")
        COUNTER_FAILED_QUERIES.add(1, {
            "task": task,
            "synthetic_query": "false"
        })
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(status_code=500, detail=str(e))
    
async def process_organic_stream(
    config: Config,
    message: rdc.QueryQueueMessage,
    start_time: float
) -> AsyncGenerator[str, str]:
    try:
        num_tokens = 0
        async for chunk in get_organic_stream(config, message):
            num_tokens += 1
            yield chunk

        COUNTER_TEXT_GENERATION_SUCCESS.add(1, {"task": message.task})
        if num_tokens > 0:
            completion_time = time.time() - start_time
            tps = num_tokens / completion_time
            GAUGE_TOKENS_PER_SEC.set(tps, {"task": message.task})

    except Exception as e:
        logger.error(f"Error in stream processing: {str(e)} - \n{traceback.format_exc()}")
        COUNTER_TEXT_GENERATION_ERROR.add(1, {
            "task": message.task,
            "error": type(e).__name__
        })
        raise

async def process_organic_image_request(
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
        
    message = rdc.QueryQueueMessage(
        task=task,
        query_type=gcst.ORGANIC,
        job_id=rutils.generate_job_id(),
        query_payload=payload.model_dump()
    )

    result = await process_organic_img_task(config, message)
        
    if result is None:
        logger.error(f"No content received for image request. Task: {task}")
        raise HTTPException(status_code=500, detail="Unable to process request")
    if result.is_nsfw:
        COUNTER_IMAGE_ERROR.add(1, {"task": task, "kind": "nsfw", "status_code": 403})
        raise HTTPException(status_code=403, detail="NSFW content detected")

    if result.image_b64 is None:
        COUNTER_IMAGE_ERROR.add(1, {"task": task, "kind": "no_image", "status_code": 500})
        raise HTTPException(status_code=500, detail="Unable to process request")

    COUNTER_IMAGE_SUCCESS.add(1, {"task": task, "status_code": 200})
    return result