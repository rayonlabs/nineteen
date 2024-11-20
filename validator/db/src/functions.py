import asyncio
from datetime import datetime, timedelta
import json
from typing import List, Any


from core.models import utility_models
from core import task_config as tcfg

from validator.db.src.database import PSQLDB
from validator.db.src.sql.rewards_and_scores import (
    insert_task,
    select_count_of_rows_in_tasks,
    delete_oldest_rows_from_tasks,
    select_count_rows_of_task_stored_for_scoring,
    select_task_for_deletion,
    select_recent_reward_data_for_a_task,
    select_recent_reward_data,
    delete_specific_task,
)
from asyncpg import Connection

from validator.models import RewardData


MAX_TASKS_IN_DB_STORE = 1000
db_lock = asyncio.Lock()


async def insert_task_results(
    connection: Connection, task: str, result: utility_models.QueryResult, synthetic_query: bool, payload: dict
) -> None:
    row_count = await select_count_of_rows_in_tasks(connection)

    if row_count >= MAX_TASKS_IN_DB_STORE + 10:
        await delete_oldest_rows_from_tasks(connection, limit=10)

    data_to_store = {
        "query_result": result.model_dump(mode="json"),
        "payload": json.dumps(payload),
        "synthetic_query": synthetic_query,
    }
    hotkey = result.node_hotkey
    if hotkey is None:
        return None
    data = json.dumps(data_to_store)
    await insert_task(connection, task, data, hotkey)


async def potentially_store_result_in_db(
    psql_db: PSQLDB, result: utility_models.QueryResult, task: str, synthetic_query: bool, payload: dict
) -> None:
    task_config = tcfg.get_enabled_task_config(task)
    if task_config is None:
        return
    target_percentage = task_config.weight
    target_number_of_tasks_to_store = int(MAX_TASKS_IN_DB_STORE * target_percentage)
    async with await psql_db.connection() as connection:
        number_of_these_tasks_already_stored = await select_count_rows_of_task_stored_for_scoring(connection, task)
        if number_of_these_tasks_already_stored <= target_number_of_tasks_to_store:
            await insert_task_results(
                connection=connection, task=task, result=result, payload=payload, synthetic_query=synthetic_query
            )


async def select_and_delete_task_result(psql_db: PSQLDB, task: str) -> tuple[list[dict[str, Any]], str] | None:
    async with await psql_db.connection() as connection:
        row = await select_task_for_deletion(connection, task)
        if row is None:
            return None
        checking_data, node_hotkey = row
        checking_data_loaded = json.loads(checking_data)

        await delete_specific_task(connection, task, checking_data)

    return checking_data_loaded, node_hotkey


# TODO: Implement this
async def clean_tables_of_hotkeys(connection: Connection, node_hotkeys: List[str]) -> None: ...
async def delete_tasks_older_than_date(connection: Connection, minutes: int) -> None: ...


async def delete_data_older_than_date(connection: Connection, minutes: int) -> None: ...


async def fetch_recent_most_rewards(
    connection: Connection, task: str, node_hotkey: str | None = None, quality_tasks_to_fetch: int = 50
) -> List[RewardData]:
    date = datetime.now() - timedelta(hours=72)
    priority_results = await select_recent_reward_data_for_a_task(connection, task, date, node_hotkey) or []

    y = len(priority_results or [])
    if y < quality_tasks_to_fetch:
        fill_results = await select_recent_reward_data(connection, date, node_hotkey, quality_tasks_to_fetch - y) or []
    else:
        fill_results = []

    reward_data_list = [
        RewardData(
            id=str(row[0]),
            task=row[1],
            node_id=row[2],
            quality_score=row[3],
            validator_hotkey=row[4],
            node_hotkey=row[5],
            synthetic_query=row[6],
            metric=row[7],
            response_time=row[8],
            volume=row[9],
            created_at=row[10],
        )
        for row in priority_results + fill_results
    ]

    return reward_data_list
