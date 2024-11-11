import time
from typing import AsyncGenerator
from fastapi import HTTPException
from redis.asyncio import Redis
from core.models.payload_models import ImageResponse
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