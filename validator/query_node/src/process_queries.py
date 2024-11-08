import time
from typing import AsyncGenerator, Union
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
import validator.utils.redis.redis_utils as rutils
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
    key = f"task_synthetics_info:{task}:requests_remaining"
    await redis_db.decr(key)

async def _handle_error(config: Config, synthetic_query: bool, job_id: str, status_code: int, error_message: str) -> None:
    if synthetic_query:
        response_queue = await rutils.get_response_queue_key(job_id)
        error_event = gutils.get_error_event(job_id=job_id, error_message=error_message, status_code=status_code)
        logger.debug(f"Pushing error event to queue {response_queue}: {error_event}")
        await config.redis_db.rpush(response_queue, error_event)
    else:
        raise HTTPException(status_code=status_code, detail=error_message)

QueryResponse = Union[AsyncGenerator[str, None], bool]

    
async def _handle_stream_generator(generator: AsyncGenerator[str, None]) -> AsyncGenerator[str, None]:
    async for chunk in generator:
        yield chunk

async def _handle_stream_query(config: Config, message: rdc.QueryQueueMessage, contenders_to_query: list[Contender]) -> QueryResponse:
    start = time.time()
    for i, contender in enumerate(contenders_to_query):
        node = await get_node(config.psql_db, contender.node_id, config.netuid)
        if node is None:
            logger.error(f"Node {contender.node_id} not found in database for netuid {config.netuid}")
            continue
            
        logger.info(f"Querying node {contender.node_id} for task {contender.task}")
        generator = await streaming.query_node_stream(
            config=config, contender=contender, payload=message.query_payload, node=node
        )

        if generator is None:
            logger.info(f"fail n°{i}")
            continue

        result = await streaming.consume_generator(
            config=config,
            generator=generator,
            job_id=message.job_id,
            synthetic_query=message.query_type == gcst.SYNTHETIC,
            contender=contender,
            node=node,
            payload=message.query_payload,
            start_time=start,
        )
        
        if message.query_type == gcst.SYNTHETIC:
            if isinstance(result, bool) and result:
                return True
        else:
            if isinstance(result, AsyncGenerator):
                return _handle_stream_generator(result)
            
    error_msg = f"Service for task {message.task} is not responding, please try again"
    if message.query_type == gcst.SYNTHETIC:
        await _handle_error(config=config, synthetic_query=True, job_id=message.job_id, 
                          status_code=500, error_message=error_msg)
        return False
    else:
        raise HTTPException(status_code=500, detail=error_msg)
    
async def _handle_nonstream_query(config: Config, message: rdc.QueryQueueMessage, contenders_to_query: list[Contender]) -> bool:
    success = False
    errors = []

    for contender in contenders_to_query:
        node = await get_node(config.psql_db, contender.node_id, config.netuid)
        if node is None:
            logger.error(f"Node {contender.node_id} not found in database for netuid {config.netuid}")
            errors.append(f"Node {contender.node_id} not found")
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
            break
        
    if not success:
        error_msg = f"Service for task {message.task} is not responding after trying {len(contenders_to_query)} contenders."
        if errors:
            error_msg += f" Errors: {'; '.join(errors)}"
            
        logger.error(f"All Contenders {[contender.node_id for contender in contenders_to_query]} for task {message.task} failed to respond! :(")
        await _handle_error(
            config=config,
            synthetic_query=message.query_type == gcst.SYNTHETIC,
            job_id=message.job_id,
            status_code=500,
            error_message=error_msg,
        )
    return success


async def process_task(config: Config, message: rdc.QueryQueueMessage) -> Union[AsyncGenerator[str, None], bool]:
    """
    Process incoming tasks, handling both synthetic and organic queries.
    Returns AsyncGenerator for organic streaming queries, bool for synthetic/non-streaming queries.
    """
    task = message.task
    
    if message.query_type == gcst.SYNTHETIC:
        message.query_payload = await putils.get_synthetic_payload(config.redis_db, task)
        await _decrement_requests_remaining(config.redis_db, task)

    task_config = tcfg.get_enabled_task_config(task)
    if task_config is None:
        error_msg = f"Can't find the task {task}, please try again later"
        logger.error(f"Can't find the task {task} in the query node!")
        await _handle_error(
            config=config,
            synthetic_query=message.query_type == gcst.SYNTHETIC,
            job_id=message.job_id,
            status_code=500,
            error_message=error_msg,
        )
        COUNTER_FAILED_QUERIES.add(1, {
            "task": task,
            "synthetic_query": str(message.query_type == gcst.SYNTHETIC),
        })
        return False

    async with await config.psql_db.connection() as connection:
        contenders_to_query = await get_contenders_for_task(connection, task, 5, message.query_type)

    if contenders_to_query is None:
        raise ValueError("No contenders to query! :(")

    COUNTER_TOTAL_QUERIES.add(1, {"task": task, "synthetic_query": str(message.query_type == gcst.SYNTHETIC)})
    
    try:
        if task_config.is_stream:
            return await _handle_stream_query(config, message, contenders_to_query)
        else:
            return await _handle_nonstream_query(config=config, message=message, contenders_to_query=contenders_to_query)
    
    except Exception as e:
        logger.error(f"Error processing task {task}: {e}")
        await _handle_error(
            config=config,
            synthetic_query=message.query_type == gcst.SYNTHETIC,
            job_id=message.job_id,
            status_code=500,
            error_message=f"Error processing task {task}: {e}",
        )
        COUNTER_FAILED_QUERIES.add(1, {
            "task": task,
            "synthetic_query": str(message.query_type == gcst.SYNTHETIC),
        })
        if message.query_type == gcst.SYNTHETIC:
            return False
        raise