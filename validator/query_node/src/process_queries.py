import time
from typing import AsyncGenerator
from fastapi import HTTPException
from redis.asyncio import Redis
from core.models.payload_models import ImageResponse
from validator.models import Contender
from validator.query_node.src.query_config import Config
from core import task_config as tcfg
from validator.utils.generic import generic_utils as gutils
from validator.utils.contender import contender_utils as putils
from validator.utils.redis import redis_constants as rcst
from fiber.logging_utils import get_logger
from validator.utils.redis import redis_dataclasses as rdc
from validator.query_node.src.query import nonstream, streaming
from validator.db.src.sql.contenders import get_contenders_for_task
from validator.db.src.sql.nodes import get_node
from validator.utils.generic import generic_constants as gcst
import validator.utils.redis as rutils
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

async def _decrement_requests_remaining(redis_db: Redis, task: str):
    """Decrement remaining synthetic requests counter."""
    key = f"task_synthetics_info:{task}:requests_remaining"
    await redis_db.decr(key)

async def _handle_synthetic_error(config: Config, job_id: str, status_code: int, error_message: str) -> None:
    """Handle errors for synthetic queries by pushing to Redis."""
    logger.error(f"Error processing synthetic message - {status_code} - {error_message}")

async def _get_contenders(config: Config, task: str, query_type: str) -> list[Contender]:
    """Get list of contenders for task."""
    try:
        connection = await config.psql_db.pool.acquire()
        try:
            contenders = await get_contenders_for_task(connection, task, 5, query_type)
            if not contenders:
                raise ValueError("No contenders available to query")
            return contenders
        finally:
            await config.psql_db.pool.release(connection)
    except Exception as e:
        logger.error(f"Error getting contenders: {e}")
        raise

async def _handle_stream_synthetic(
    config: Config, 
    message: rdc.QueryQueueMessage, 
    contenders: list[Contender]
) -> bool:
    """Handle streaming synthetic queries."""
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
            job_id=message.job_id,
            contender=contender,
            node=node,
            payload=message.query_payload,
            start_time=start,
        )
        
        if success:
            return True
            
    error_msg = f"Service for task {message.task} is not responding, please try again"
    await _handle_synthetic_error(
        config=config,
        job_id=message.job_id, 
        status_code=500,
        error_message=error_msg
    )
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
            job_id=message.job_id,
            contender=contender,
            node=node,
            payload=message.query_payload,
            start_time=start,
        ):
            yield chunk
        return  # Successfully streamed all chunks
            
    raise HTTPException(
        status_code=500,
        detail=f"Service for task {message.task} is not responding, please try again"
    )

async def _handle_nonstream_query(
    config: Config, 
    message: rdc.QueryQueueMessage, 
    contenders: list[Contender]
) -> bool:
    """Handle non-streaming queries."""
    for contender in contenders:
        
        node = await get_node(config.psql_db, contender.node_id, config.netuid)
        if not node:
            logger.error(f"Node {contender.node_id} not found in database for netuid {config.netuid}")
            continue
            
        success = await nonstream.query_nonstream(
            config=config,
            contender=contender,
            node=node,
            payload=message.query_payload,
            response_model=ImageResponse,
            synthetic_query=message.query_type == gcst.SYNTHETIC,
            job_id=message.job_id,
        )
        if success:
            return True
        
    error_msg = f"Service for task {message.task} is not responding after trying {len(contenders)} contenders"
    if message.query_type == gcst.SYNTHETIC:
        await _handle_synthetic_error(
            config=config,
            job_id=message.job_id,
            status_code=500,
            error_message=error_msg,
        )
        return False
    else:
        raise HTTPException(status_code=500, detail=error_msg)

async def process_synthetic_task(config: Config, message: rdc.QueryQueueMessage) -> bool:
    """Process synthetic task, returning success status."""
    task = message.task
    
    try:
        message.query_payload = await putils.get_synthetic_payload(config.redis_db, task)
        await _decrement_requests_remaining(config.redis_db, task)

        task_config = tcfg.get_enabled_task_config(task)
        if not task_config:
            error_msg = f"Can't find the task {task}, please try again later"
            logger.error(f"Can't find the task {task} in the query node!")
            await _handle_synthetic_error(config, message.job_id, 500, error_msg)
            COUNTER_FAILED_QUERIES.add(1, {
                "task": task,
                "synthetic_query": "true"
            })
            return False

        contenders = await _get_contenders(config, task, message.query_type)
        
        COUNTER_TOTAL_QUERIES.add(1, {
            "task": task,
            "synthetic_query": "true"
        })
        
        if task_config.is_stream:
            return await _handle_stream_synthetic(config, message, contenders)
        else:
            return await _handle_nonstream_query(config, message, contenders)

    except Exception as e:
        logger.error(f"Error processing synthetic task {task}: {e}")
        await _handle_synthetic_error(
            config=config,
            job_id=message.job_id,
            status_code=500,
            error_message=f"Error processing task {task}: {e}",
        )
        COUNTER_FAILED_QUERIES.add(1, {
            "task": task,
            "synthetic_query": "true"
        })
        return False

async def process_organic_task(config: Config, message: rdc.QueryQueueMessage) -> AsyncGenerator[str, None]:
    """Process organic task, streaming responses."""
    task = message.task
    
    try:
        task_config = tcfg.get_enabled_task_config(task)
        if not task_config:
            raise HTTPException(
                status_code=500,
                detail=f"Can't find the task {task}, please try again later"
            )

        contenders = await _get_contenders(config, task, message.query_type)
        
        COUNTER_TOTAL_QUERIES.add(1, {
            "task": task,
            "synthetic_query": "false"
        })
        
        if task_config.is_stream:
            async for chunk in _handle_stream_organic(config, message, contenders):
                yield chunk
        else:
            success = await _handle_nonstream_query(config, message, contenders)
            if not success:
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to process non-streaming task {task}"
                )

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