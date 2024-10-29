from dataclasses import asdict
import json
from validator.db.src.sql.contenders import fetch_all_contenders, fetch_contender
from validator.db.src.database import PSQLDB
from validator.models import Contender
from validator.utils.redis import redis_constants as rcst, redis_utils as rutils, redis_dataclasses as rdc
from redis.asyncio import Redis
from fiber.logging_utils import get_logger
import uuid
from validator.utils.generic import generic_constants as gcst

logger = get_logger(__name__)


def construct_synthetic_query_message(task: str) -> str:
    return json.dumps(
        asdict(rdc.QueryQueueMessage(query_payload={}, query_type=gcst.SYNTHETIC, task=task, job_id=uuid.uuid4().hex))
    )


# Consistently about 1ms
async def load_contender(psql_db: PSQLDB, contender_id: str) -> Contender | None:
    async with await psql_db.connection() as connection:
        return await fetch_contender(connection, contender_id)


async def load_contenders(psql_db: PSQLDB) -> list[Contender]:
    async with await psql_db.connection() as connection:
        return await fetch_all_contenders(connection)


async def add_synthetic_query_to_queue(redis_db: Redis, task: str, max_length: int) -> None:
    message = construct_synthetic_query_message(task)
    await rutils.add_str_to_redis_list(redis_db, rcst.QUERY_QUEUE_KEY, message, max_length)


async def load_query_queue(redis_db: Redis) -> list[str]:
    return await rutils.get_redis_list(redis_db, rcst.QUERY_QUEUE_KEY)


async def load_synthetic_scheduling_queue(redis_db: Redis) -> list[str]:
    return await rutils.get_sorted_set(redis_db, rcst.SYNTHETIC_SCHEDULING_QUEUE_KEY)

