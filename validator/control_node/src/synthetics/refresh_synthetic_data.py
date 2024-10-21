import asyncio
import datetime
import json
import time
from pydantic import BaseModel
from core import task_config as tcfg
from redis.asyncio import Redis
from validator.control_node.src.control_config import Config
from validator.utils.redis import redis_constants as rcst
from validator.utils.synthetic import synthetic_constants as scst
from validator.utils.synthetic import synthetic_utils as sutils
from validator.control_node.src.synthetics import synthetic_generation_funcs
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


# TODO: Change to mapping
async def _store_synthetic_data_in_redis(redis_db: Redis, task: str, synthetic_data: BaseModel) -> None:
    pipe = redis_db.pipeline(transaction=True)

    task_key = sutils.construct_synthetic_data_task_key(task)
    await pipe.set(task_key, json.dumps(synthetic_data.model_dump()))

    current_time = int(time.time() * 1000) / 1000
    await pipe.hset(rcst.SYNTHETIC_DATA_VERSIONS_KEY, task, current_time)  # type: ignore

    await pipe.execute()


async def update_tasks_synthetic_data(redis_db: Redis, slow_sync: bool = True, fixed_task: str | None = None) -> None:
    if fixed_task is not None:
        now = datetime.datetime.now().timestamp()
        synthetic_data_version = await sutils.get_synthetic_data_version(redis_db, fixed_task)
        if synthetic_data_version is None or now - synthetic_data_version > scst.SYNTHETIC_DATA_EXPIRATION_TIME:
            new_synthetic_data = await synthetic_generation_funcs.generate_synthetic_data(fixed_task)
            if new_synthetic_data is not None:
                await _store_synthetic_data_in_redis(redis_db, fixed_task, new_synthetic_data)

    else:
        task_configs = tcfg.get_task_configs()
        for task in task_configs:
            now = datetime.datetime.now().timestamp()
            synthetic_data_version = await sutils.get_synthetic_data_version(redis_db, task)
            if synthetic_data_version is None or now - synthetic_data_version > scst.SYNTHETIC_DATA_EXPIRATION_TIME:
                new_synthetic_data = await synthetic_generation_funcs.generate_synthetic_data(task)
                if new_synthetic_data is not None:
                    await _store_synthetic_data_in_redis(redis_db, task, new_synthetic_data)
            if slow_sync:
                await asyncio.sleep(0.1)


async def continuously_fetch_synthetic_data_for_tasks(redis_db: Redis) -> None:
    await update_tasks_synthetic_data(redis_db, slow_sync=False)

    while True:
        await update_tasks_synthetic_data(redis_db, slow_sync=True)


async def main(config: Config):
    await continuously_fetch_synthetic_data_for_tasks(config.redis_db)
