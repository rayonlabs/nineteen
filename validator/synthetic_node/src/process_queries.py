import time
from opentelemetry import metrics
from fiber.logging_utils import get_logger

from core.models.payload_models import ImageResponse
from core import task_config as tcfg
from validator.models import Contender
from validator.common.query_config import Config
from validator.utils.generic import generic_utils as gutils
from validator.utils.redis import redis_constants as rcst
from validator.utils.redis import redis_dataclasses as rdc
from validator.common.query import nonstream, streaming
from validator.db.src.sql.contenders import get_contenders_for_task, update_total_requests_made
from validator.db.src.sql.nodes import get_node
from validator.utils.generic import generic_constants as gcst
from validator.utils.contender import contender_utils as putils


logger = get_logger(__name__)

MAX_CONCURRENT_TASKS = 10

COUNTER_TOTAL_QUERIES = metrics.get_meter(__name__).create_counter(
    name="validator.synthetic_node.process.total_queries",
    description="Number of total queries sent to `process_task`",
)

COUNTER_FAILED_QUERIES= metrics.get_meter(__name__).create_counter(
    name="validator.synthetic_node.process.failed_queries",
    description="Number of failed queries within `process_task`",
)


async def _handle_stream_query(config: Config, message: rdc.QueryQueueMessage, contenders_to_query: list[Contender]) -> bool:
    success = False
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


async def _handle_nonstream_query(config: Config, message: rdc.QueryQueueMessage, contenders_to_query: list[Contender]) -> bool:
    success = False
    for contender in contenders_to_query:
        node = await get_node(config.psql_db, contender.node_id, config.netuid)
        if node is None:
            logger.error(f"Node {contender.node_id} not found in database for netuid {config.netuid}")
            continue

        await update_total_requests_made(config.psql_db, contender)
        success, _ = await nonstream.query_nonstream(
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

    logger.info(f"Received : {message}")

    if not message.query_type == gcst.SYNTHETIC:
        await _handle_error(
            config=config,
            synthetic_query=False,
            job_id=message.job_id,
            status_code=500,
            error_message=f"{message.query_type} type is not supported! This node only processes synthetic queries!",
        )

    message.query_payload = await putils.get_synthetic_payload(config.redis_db, task)

    logger.info(f"Processing synth task : {task}")

    task_config = tcfg.get_enabled_task_config(task)
    if task_config is None:
        logger.error(f"Can't find the task {task} in the query node!")
        await _handle_error(
            config=config,
            synthetic_query=True,
            job_id=message.job_id,
            status_code=500,
            error_message=f"Can't find the task {task}, please try again later",
        )

        COUNTER_FAILED_QUERIES.add(1, {
            "task": task,
            "synthetic_query": str(True),
        })

        return

    stream = task_config.is_stream

    contenders_to_query = await get_contenders_for_task(config.psql_db, task, 5, gcst.SYNTHETIC)

    if contenders_to_query is None:
        raise ValueError("No contenders to query! :(")

    COUNTER_TOTAL_QUERIES.add(1, {"task": task, "synthetic_query": str(True)})

    try:
        if stream:
            return await _handle_stream_query(config, message, contenders_to_query)
        else:
            return await _handle_nonstream_query(config=config, message=message, contenders_to_query=contenders_to_query)

    except Exception as e:
        logger.error(f"Error processing task {task}: {e}")
        await _handle_error(
            config=config,
            synthetic_query=True,
            job_id=message.job_id,
            status_code=500,
            error_message=f"Error processing task {task}: {e}",
        )

        COUNTER_FAILED_QUERIES.add(1, {
            "task": task,
            "synthetic_query": str(True),
        })
