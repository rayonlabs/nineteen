from validator.db.src.sql.contenders import fetch_all_contenders, fetch_contender
from validator.db.src.database import PSQLDB
from validator.models import Contender
from validator.utils.redis import redis_constants as rcst, redis_utils as rutils
from redis.asyncio import Redis
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


# Consistently about 1ms
async def load_contender(psql_db: PSQLDB, contender_id: str) -> Contender | None:
    async with await psql_db.connection() as connection:
        return await fetch_contender(connection, contender_id)


async def load_contenders(psql_db: PSQLDB) -> list[Contender]:
    async with await psql_db.connection() as connection:
        return await fetch_all_contenders(connection)

async def load_query_queue(redis_db: Redis) -> list[str]:
    return await rutils.get_redis_list(redis_db, rcst.QUERY_QUEUE_KEY)


async def load_synthetic_scheduling_queue(redis_db: Redis) -> list[str]:
    return await rutils.get_sorted_set(redis_db, rcst.SYNTHETIC_SCHEDULING_QUEUE_KEY)


async def get_synthetic_payload(redis_db: Redis, task: str) -> dict:
    return await rutils.json_load_from_redis(redis_db, rcst.SYNTHETIC_DATA_KEY + ":" + task, default={})
