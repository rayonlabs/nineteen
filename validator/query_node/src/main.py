from dotenv import load_dotenv
import os
import nltk
import heapq

# Must be done straight away, bit ugly
load_dotenv(os.getenv("ENV_FILE", ".vali.env"))

import asyncio
from redis.asyncio import Redis
from validator.utils.generic import generic_constants as gcst
import time
import uuid
from fiber.logging_utils import get_logger
import json
from validator.query_node.src.query_config import Config
from validator.utils.redis import redis_constants as rcst, redis_dataclasses as rdc
from validator.query_node.src.process_queries import process_task
from validator.db.src.sql.nodes import get_vali_ss58_address
from validator.db.src.database import PSQLDB
from validator.utils.synthetic import synthetic_utils as sutils
from core import constants as ccst
import random
from typing import Dict, List
from validator.models import Contender
from validator.utils.contender import contender_utils as putils
from core import task_config as tcfg
from dataclasses import dataclass


from fiber.chain import chain_utils
from opentelemetry import metrics

logger = get_logger(__name__)

MAX_CONCURRENT_TASKS = 100

QUERY_NODE_REQUESTS_PROCESSING_GAUGE = metrics.get_meter(__name__).create_gauge(
    name="validator.query_node.src.concurrent_queries_processing",
    description="concurrent number of requests currently being processed by query node's `listen_for_tasks`",
    unit="1"
)

QUERY_NODE_FAILED_POPS_COUNTER = metrics.get_meter(__name__).create_counter(
    name="validator.query_node.src.query_node_failed_pops",
    description="number of failed pops from redis QUERY_QUEUE_KEY",
    unit="1"
)

QUERY_NODE_FAILED_TASKS_COUNTER = metrics.get_meter(__name__).create_counter(
    name="validator.query_node.src.query_node_failed_tasks",
    description="number of failed `process_task` instances",
    unit="1"
)

GAUGE_SCHEDULE_REMAINING_REQUESTS = metrics.get_meter(__name__).create_gauge(
    "validator.control_node.synthetic.cycle.schedule_remaining_requests",
    description="Number of remaining synthetic requests for scheduled task"
)

@dataclass
class TaskScheduleInfo:
    task: str
    total_requests: int
    interval: float
    next_schedule_time: float
    remaining_requests: int

    def __lt__(self, other: "TaskScheduleInfo"):
        return self.next_schedule_time < other.next_schedule_time




async def load_config() -> Config:
    wallet_name = os.getenv("WALLET_NAME", "default")
    hotkey_name = os.getenv("HOTKEY_NAME", "default")

    netuid = os.getenv("NETUID")
    if netuid is None:
        raise ValueError("NETUID must be set")
    else:
        netuid = int(netuid)

    localhost = bool(os.getenv("LOCALHOST", "false").lower() == "true")
    if localhost:
        redis_host = "localhost"
        os.environ["POSTGRES_HOST"] = "localhost"
    else:
        redis_host = os.getenv("REDIS_HOST", "redis")

    replace_with_docker_localhost = bool(os.getenv("REPLACE_WITH_DOCKER_LOCALHOST", "false").lower() == "true")

    psql_db = PSQLDB()
    await psql_db.connect()

    ss58_address = None
    while ss58_address is None:
        ss58_address = await get_vali_ss58_address(psql_db, netuid)
        await asyncio.sleep(0.1)

    keypair = chain_utils.load_hotkey_keypair(wallet_name=wallet_name, hotkey_name=hotkey_name)
    scoring_period_time_multiplier = float(os.getenv("SCORING_PERIOD_TIME_MULTIPLIER", 1.0))


    return Config(
        redis_db=Redis(host=redis_host),
        psql_db=psql_db,
        netuid=netuid,
        ss58_address=ss58_address,
        replace_with_docker_localhost=replace_with_docker_localhost,
        replace_with_localhost=localhost,
        keypair=keypair,
        scoring_period_time_multiplier=scoring_period_time_multiplier
    )


async def _load_contenders(psql_db: PSQLDB) -> List[Contender]:
    return await putils.load_contenders(psql_db)


async def _group_contenders_by_task(contenders: List[Contender]) -> Dict[str, List[Contender]]:
    task_groups: Dict[str, List[Contender]] = {}
    task_configs = tcfg.get_task_configs()
    for contender in contenders:
        if contender.task not in task_configs:
            continue
        if contender.task not in task_groups:
            task_groups[contender.task] = []
        task_groups[contender.task].append(contender)
    return task_groups


def _calculate_task_requests(task: str, contenders: List[Contender], config: Config) -> int:
    task_config = tcfg.get_enabled_task_config(task)
    if task_config is None:
        return 0
    total_capacity = sum(c.capacity_to_score for c in contenders) * config.scoring_period_time_multiplier
    return int(total_capacity / task_config.volume_to_requests_conversion)


def _get_initial_schedule_time(current_time: float, interval: float) -> float:
    return current_time + random.random() * interval


async def _initialize_task_schedules(task_groups: Dict[str, List[Contender]], config: Config) -> List[TaskScheduleInfo]:
    scoring_period_time = ccst.SCORING_PERIOD_TIME * config.scoring_period_time_multiplier
    schedules = []
    for task, contenders in task_groups.items():
        total_requests = _calculate_task_requests(task, contenders, config)
        if total_requests > 0:
            interval = scoring_period_time / (total_requests + 1)
            current_time = time.time()
            first_schedule_time = _get_initial_schedule_time(current_time, interval)
            schedule = TaskScheduleInfo(
                task=task,
                total_requests=total_requests,
                interval=interval,
                next_schedule_time=first_schedule_time,
                remaining_requests=total_requests,
            )
            heapq.heappush(schedules, schedule)
    return schedules


async def listen_for_tasks_and_schedule_synth(config: Config):
    tasks: set[asyncio.Task] = set()

    # init synthetic task scheduling
    contenders = await _load_contenders(config.psql_db)
    task_groups = await _group_contenders_by_task(contenders)
    task_schedules = await _initialize_task_schedules(task_groups, config)

    logger.info("Listening for tasks and scheduling synthetic queries.")
    while True:
        done = {t for t in tasks if t.done()}
        tasks.difference_update(done)
        for t in done:
            await t

        QUERY_NODE_REQUESTS_PROCESSING_GAUGE.set(len(tasks))

        while len(tasks) < MAX_CONCURRENT_TASKS:
            # try to get an organic task from Redis
            message_json = await config.redis_db.blpop(rcst.QUERY_QUEUE_KEY, timeout=1)  # type: ignore

            if message_json:
                try:
                    task = asyncio.create_task(process_task(config, rdc.QueryQueueMessage(**json.loads(message_json[1]))))
                    tasks.add(task)
                    continue
                except TypeError:
                    QUERY_NODE_FAILED_TASKS_COUNTER.add(1)
                    logger.error(f"Failed to process message: {message_json}")

            # if no organic task, check for synthetic tasks
            if task_schedules:
                schedule = heapq.heappop(task_schedules)
                current_time = time.time()

                if current_time >= schedule.next_schedule_time:
                    if schedule.remaining_requests > 0:
                        synthetic_query = rdc.QueryQueueMessage(
                            job_id=uuid.uuid4().hex,
                            query_payload={},
                            task=schedule.task,
                            query_type=gcst.SYNTHETIC
                        )

                        task = asyncio.create_task(process_task(config, synthetic_query))
                        tasks.add(task)

                        schedule.remaining_requests -= 1
                        schedule.next_schedule_time = current_time + schedule.interval

                        if schedule.remaining_requests > 0:
                            heapq.heappush(task_schedules, schedule)

                        GAUGE_SCHEDULE_REMAINING_REQUESTS.set(schedule.remaining_requests, {"task": schedule.task})
                        continue
                else:
                    heapq.heappush(task_schedules, schedule)

            break

        await asyncio.sleep(0.01)


async def main() -> None:
    nltk.download('punkt_tab')

    config = await load_config()
    logger.debug(f"config: {config}")

    logger.info("Waiting for control node....")
    attempts = 0
    while True:
        try:
            control_node_ready = await config.redis_db.get(ccst.CONTROL_NODE_READY_KEY)
            logger.info(f"Control node ready status (raw): {control_node_ready!r}")

            if control_node_ready is None:
                logger.info("Control node key not found in Redis")
            elif str(control_node_ready) == '1':
                logger.info("Control node is ready, proceeding...")
                break
            else:
                logger.info(f"Unexpected value for control node ready key: {control_node_ready!r}")

            attempts += 1
            if attempts % 10 == 0:
                logger.info(f"Still waiting for control node... (attempt {attempts})")

            await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Error checking control node status: {e}")
            await asyncio.sleep(1)

    await asyncio.gather(
        sutils.get_save_random_text(),
        listen_for_tasks_and_schedule_synth(config),
    )

if __name__ == "__main__":
    asyncio.run(main())
