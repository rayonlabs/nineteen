import json
import time
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
from validator.db.src.sql.contenders import get_contenders_for_task, update_total_requests_made
from validator.db.src.sql.nodes import get_node
from validator.utils.generic import generic_constants as gcst
from opentelemetry import metrics


logger = get_logger(__name__)

MAX_CONCURRENT_TASKS = 10


COUNTER_TOTAL_QUERIES = metrics.get_meter(__name__).create_counter(
    name="validator.query_node.process.total_queries",
    description="Number of total queries sent to `process_task`",
)

COUNTER_FAILED_QUERIES= metrics.get_meter(__name__).create_counter(
    name="validator.query_node.process.failed_queries",
    description="Number of failed queries within `process_task`",
)

COUNTER_CONTENDER_SELECTIONS = metrics.get_meter(__name__).create_counter(
    name="validator.query_node.contender.selections",
    description="Number of times a contender is selected for organic queries",
)

HISTOGRAM_TIME_TO_SUCCESS = metrics.get_meter(__name__).create_histogram(
    name="validator.query_node.time_to_success",
    description="Time taken to get a successful response from any contender",
    unit="s",
)

COUNTER_CONTENDER_ERRORS = metrics.get_meter(__name__).create_counter(
    name="validator.query_node.contender.errors",
    description="Number of errors (400s, 500s) per contender",
)

GAUGE_CONTENDER_SCORE = metrics.get_meter(__name__).create_gauge(
    name="validator.query_node.contender.normalized_score",
    description="Normalized net score for each contender",
)

async def _decrement_requests_remaining(redis_db: Redis, task: str):
    key = f"task_synthetics_info:{task}:requests_remaining"
    await redis_db.decr(key)


async def _acknowledge_job(redis_db: Redis, job_id: str):
    logger.debug(f"Acknowledging job id : {job_id}")
    await redis_db.publish(f"{gcst.ACKNLOWEDGED}:{job_id}", json.dumps({gcst.ACKNLOWEDGED: True}))


async def _handle_stream_query(config: Config, message: rdc.QueryQueueMessage, contenders_to_query: list[Contender]) -> bool:
    success = False
    start_attempt_time = time.time()

    for contender in contenders_to_query[:5]:
        node = await get_node(config.psql_db, contender.node_id, config.netuid)
        if node is None:
            logger.error(f"Node {contender.node_id} not found in database for netuid {config.netuid}")
            continue

        logger.debug(f"Querying node {contender.node_id} for task {contender.task} with payload: {message.query_payload}")
        start_time = time.time()

        await update_total_requests_made(config.psql_db, contender)
        generator = await streaming.query_node_stream(
            config=config, contender=contender, payload=message.query_payload, node=node
        )
        # TODO: Make sure we still punish if generator is None
        if generator is None:
            if message.query_type == gcst.ORGANIC:
                COUNTER_CONTENDER_ERRORS.add(1, {
                    "task": contender.task,
                    "contender_id": str(contender.node_id),
                    "error_type": "500",
                    "hotkey": contender.hotkey
                })
            continue

        success = await streaming.consume_generator(
            config=config,
            generator=generator,
            job_id=message.job_id,
            synthetic_query=message.query_type == gcst.SYNTHETIC,
            contender=contender,
            node=node,
            payload=message.query_payload,
            start_time=start_time,
        )

        if success and message.query_type == gcst.ORGANIC:
            time_to_success = time.time() - start_attempt_time
            HISTOGRAM_TIME_TO_SUCCESS.record(
                time_to_success,
                {"task": contender.task}
            )
            break

    if not success:
        logger.error(
            f"All Contenders {[contender.node_id for contender in contenders_to_query]} for task {message.task} failed to respond! :("
        )

        await _handle_error(
            config=config,
            synthetic_query=message.query_type == gcst.SYNTHETIC,
            job_id=message.job_id,
            status_code=500,
            error_message=f"Service for task {message.task} is not responding, please try again",
        )

    return success


async def _handle_nonstream_query(config: Config, message: rdc.QueryQueueMessage, contenders_to_query: list[Contender]) -> bool:
    success = False
    for contender in contenders_to_query:
        node = await get_node(config.psql_db, contender.node_id, config.netuid)
        if node is None:
            logger.error(f"Node {contender.node_id} not found in database for netuid {config.netuid}")
            continue

        await update_total_requests_made(config.psql_db, contender)
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
        logger.error(
            f"All Contenders {[contender.node_id for contender in contenders_to_query]} for task {message.task} failed to respond! :("
        )
        await _handle_error(
            config=config,
            synthetic_query=message.query_type == gcst.SYNTHETIC,
            job_id=message.job_id,
            status_code=500,
            error_message=f"Service for task {message.task} is not responding, please try again",
        )
    return success


async def _handle_error(config: Config, synthetic_query: bool, job_id: str, status_code: int, error_message: str) -> None:
    if not synthetic_query:
        await config.redis_db.publish(
            f"{rcst.JOB_RESULTS}:{job_id}",
            gutils.get_error_event(job_id=job_id, error_message=error_message, status_code=status_code),
        )


async def process_task(config: Config, message: rdc.QueryQueueMessage):
    task = message.task

    if message.query_type == gcst.ORGANIC:
        logger.debug(f"Acknowledging job id : {message.job_id}")
        await _acknowledge_job(config.redis_db, message.job_id)
        logger.debug(f"Successfully acknowledged job id : {message.job_id} ✅")
        await _decrement_requests_remaining(config.redis_db, task)
    else:
        message.query_payload = await putils.get_synthetic_payload(config.redis_db, task)

    task_config = tcfg.get_enabled_task_config(task)
    if task_config is None:
        logger.error(f"Can't find the task {task} in the query node!")
        await _handle_error(
            config=config,
            synthetic_query=message.query_type == gcst.SYNTHETIC,
            job_id=message.job_id,
            status_code=500,
            error_message=f"Can't find the task {task}, please try again later",
        )

        COUNTER_FAILED_QUERIES.add(1, {
            "task": task,
            "synthetic_query": str(message.query_type == gcst.SYNTHETIC),
        })

        return

    stream = task_config.is_stream

    contenders_to_query = await get_contenders_for_task(config.psql_db, task, 5, message.query_type)

    if contenders_to_query is None:
        raise ValueError("No contenders to query! :(")

    COUNTER_TOTAL_QUERIES.add(1, {"task": task, "synthetic_query": str(message.query_type == gcst.SYNTHETIC)})

    if message.query_type == gcst.ORGANIC:
        for contender in contenders_to_query:
            COUNTER_CONTENDER_SELECTIONS.add(1, {
                "task": contender.task,
                "contender_id": str(contender.node_id),
                "hotkey": contender.hotkey
            })


    try:
        if stream:
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
