import asyncio
import random
import requests
import os
import datetime
import json
import nltk
import time
import fcntl
from pydantic import BaseModel
from core import task_config as tcfg
from redis.asyncio import Redis
from validator.control_node.src.control_config import Config
from validator.utils.redis import redis_constants as rcst
from validator.utils.synthetic import synthetic_constants as scst
from validator.db.src.database import PSQLDB
from validator.utils.database import database_constants as dcst
from validator.utils.synthetic import synthetic_utils as sutils
from validator.control_node.src.synthetics import synthetic_generation_funcs
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


# TOOD: Change to mapping
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

async def fetch_random_text():
    n_paragraphes = random.randint(2, 4)
    n_sentences = random.randint(1, 6)
    url = f'http://metaphorpsum.com/paragraphs/{n_paragraphes}/{n_sentences}'
    response = requests.get(url)
    if response.status_code == 200:
        return response.text, n_paragraphes, n_sentences
    else:
        raise logger.error(f"Failed to fetch text from metaphorpsum.com: {response.status_code}")

async def get_save_random_text() -> None:
    file_path = 'random_text_queue.txt'
    if not os.path.exists(file_path):
        with open(file_path, 'w') as file:
            pass
    while True:
        try:
            with open(file_path, 'r') as file:
                lines = file.readlines()
            queue_size = len(lines)
            if queue_size < 500:
                text, n_paragraphes, n_sentences = await fetch_random_text()
                n_words = len(text.split())
                with open(file_path, 'a') as file:
                    fcntl.flock(file, fcntl.LOCK_EX)
                    file.write(text + '\n')
                    fcntl.flock(file, fcntl.LOCK_UN)
                logger.debug(f"Pushed text with {n_words} words, {n_paragraphes} paragraphs, and {n_sentences} sentences to text file '{file_path}'")
            else:
                logger.debug(f"Text file '{file_path}' is full. Skipping text insertion.")
            await asyncio.sleep(1)
        except Exception as e:
            await asyncio.sleep(60)
            logger.error(f"Error fetching and saving synthetic data: {e} - sleeping for 60s")

async def continuously_fetch_synthetic_data_for_tasks(redis_db: Redis) -> None:
    asyncio.create_task(get_save_random_text())
    await update_tasks_synthetic_data(redis_db, slow_sync=False)
    while True:
        await update_tasks_synthetic_data(redis_db, slow_sync=True)

async def main(config: Config):
    nltk.download('punkt_tab')
    await continuously_fetch_synthetic_data_for_tasks(config.redis_db)
