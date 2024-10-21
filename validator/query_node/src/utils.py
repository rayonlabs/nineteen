from core.models import utility_models
from validator.models import Contender
from validator.query_node.src.query_config import Config
from validator.utils import work_and_speed_functions
from core import task_config as tcfg
from fiber.logging_utils import get_logger
from validator.db.src import functions as db_functions
from validator.db.src.sql.contenders import (
    update_contender_429_count,
    update_contender_500_count,
    update_contender_capacities,
)

logger = get_logger(__name__)


async def adjust_contender_from_result(
    config: Config,
    query_result: utility_models.QueryResult,
    contender: Contender,
    synthetic_query: bool,
    payload: dict,
) -> utility_models.QueryResult:
    """
    Update the db with consumed volume
    Store the task result in the db for checking (potentially)
    """

    if query_result.status_code == 200 and query_result.success:
        logger.debug(f"✅ Adjusting node {contender.node_id} for task {query_result.task}")
        task_config = tcfg.get_enabled_task_config(query_result.task)
        if task_config is None:
            logger.error(f"Task {query_result.task} is not enabled")
            return query_result

        capacity_consumed = work_and_speed_functions.calculate_work(
            task_config=task_config, result=query_result.model_dump(), steps=payload.get("steps")
        )
        logger.debug(f"Capacity consumed: {capacity_consumed}")

        await update_contender_capacities(config.psql_db, contender, capacity_consumed)

        await db_functions.potentially_store_result_in_db(
            config.psql_db, query_result, query_result.task, synthetic_query=synthetic_query, payload=payload
        )
        logger.debug(f"Adjusted node {contender.node_id} for task {query_result.task}.")

    elif query_result.status_code == 429:
        logger.debug(f"❌ 💔 429 error;  Adjusting node {contender.node_id} for task {query_result.task}.")
        await update_contender_429_count(config.psql_db, contender)
    else:
        logger.debug(f"❌ 💔 500 error; Adjusting node {contender.node_id} for task {query_result.task}.")
        await update_contender_500_count(config.psql_db, contender)
    return query_result
