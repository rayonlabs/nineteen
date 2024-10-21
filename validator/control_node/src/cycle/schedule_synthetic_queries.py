import asyncio
from dataclasses import dataclass
import json
import random
import time
from typing import Dict, List
import heapq

from redis.asyncio import Redis
from validator.db.src.database import PSQLDB
from validator.control_node.src.control_config import Config
from validator.models import Contender
from core import task_config as tcfg
from validator.utils.contender import contender_utils as putils
from validator.utils.generic import generic_constants as gcst
from validator.utils.redis import redis_constants as rcst
from core import constants as ccst
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


@dataclass
class TaskScheduleInfo:
    task: str
    total_requests: int
    interval: float
    next_schedule_time: float
    remaining_requests: int

    def __lt__(self, other: "TaskScheduleInfo"):
        return self.next_schedule_time < other.next_schedule_time


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


async def _update_redis_remaining_requests(redis_db: Redis, task: str, remaining_requests: int):
    key = f"task_synthetics_info:{task}:requests_remaining"
    await redis_db.set(key, remaining_requests)


async def _get_redis_remaining_requests(redis_db: Redis, task: str) -> int:
    key = f"task_synthetics_info:{task}:requests_remaining"
    value = await redis_db.get(key)
    return int(value) if value is not None else 0


async def _schedule_synthetic_query(redis_db: Redis, task: str, max_len: int):
    await putils.add_synthetic_query_to_queue(redis_db, task, max_len)


async def _clear_old_synthetic_queries(redis_db: Redis):
    all_items = await redis_db.lrange(rcst.QUERY_QUEUE_KEY, 0, -1)  # type: ignore

    non_synthetic_items = [item for item in all_items if json.loads(item).get("query_type") != gcst.SYNTHETIC]
    await redis_db.delete(rcst.QUERY_QUEUE_KEY)

    if non_synthetic_items:
        await redis_db.rpush(rcst.QUERY_QUEUE_KEY, *non_synthetic_items)  # type: ignore

    logger.info(f"Cleared {len(all_items) - len(non_synthetic_items)} synthetic queries from the queue")


async def schedule_synthetics_until_done(config: Config):
    scoring_period_time = ccst.SCORING_PERIOD_TIME * config.scoring_period_time_multiplier
    logger.info(f"Scheduling synthetics; this will take {scoring_period_time // 60} minutes ish...")

    start_time = time.time()

    contenders = await _load_contenders(config.psql_db)
    task_groups = await _group_contenders_by_task(contenders)
    task_schedules = await _initialize_task_schedules(task_groups, config)
    await _clear_old_synthetic_queries(config.redis_db)

    for schedule in task_schedules:
        await _update_redis_remaining_requests(config.redis_db, schedule.task, schedule.total_requests)

    i = 0
    while task_schedules:
        i += 1
        schedule = heapq.heappop(task_schedules)
        time_to_sleep = schedule.next_schedule_time - time.time()

        if time_to_sleep > 0:
            if time.time() - start_time + time_to_sleep > scoring_period_time:
                break

            logger.debug(
                f"Sleeping for {time_to_sleep} seconds while waiting for task {schedule.task} to be scheduled;"
                f"{schedule.remaining_requests} requests remaining - estimated to take {schedule.remaining_requests  * schedule.interval} more seconds"
            )
            sleep_chunk = 2  # Sleep in 2-second chunks to make debugging easier
            while time_to_sleep > 0:
                await asyncio.sleep(min(sleep_chunk, time_to_sleep))
                time_to_sleep -= sleep_chunk

        latest_remaining_requests = await _get_redis_remaining_requests(config.redis_db, schedule.task)
        if latest_remaining_requests <= 0:
            logger.info(f"No more requests remaining for task {schedule.task}")
            continue
        requests_to_skip = schedule.remaining_requests - latest_remaining_requests

        if requests_to_skip > 0:
            logger.info(f"Skipping {requests_to_skip} requests for task {schedule.task}")

            schedule.next_schedule_time += schedule.interval * requests_to_skip
            schedule.remaining_requests = latest_remaining_requests
            heapq.heappush(task_schedules, schedule)
            continue
        else:
            await _schedule_synthetic_query(config.redis_db, schedule.task, max_len=100)

            remaining_requests = latest_remaining_requests - 1
            await _update_redis_remaining_requests(config.redis_db, schedule.task, remaining_requests)
            schedule.next_schedule_time = time.time() + schedule.interval
            schedule.remaining_requests = remaining_requests

        if schedule.remaining_requests > 0:
            heapq.heappush(task_schedules, schedule)
        else:
            logger.info(f"No more requests remaining for task {schedule.task}")

        if time.time() - start_time > scoring_period_time:
            break

        if i % 100 == 0:
            # Print full stats of all tasks
            schedules_left = [heapq.heappop(task_schedules) for _ in range(len(task_schedules))]
            
            task_info = []
            for schedule in schedules_left:
                task_info.append(
                    f"Task {schedule.task}:\n"
                    f"  - Scheduled: {schedule.total_requests - schedule.remaining_requests} / {schedule.total_requests}\n"
                    f"  - Next request: {schedule.next_schedule_time - time.time():.2f} seconds from now\n"
                    f"  - Estimated completion: {schedule.remaining_requests * schedule.interval:.2f} seconds"
                )
                heapq.heappush(task_schedules, schedule)

            logger.info(
                f"Synthetic query scheduling status:\n"
                f"Iterations: {i}\n"
                f"Time elapsed: {time.time() - start_time:.2f} / {scoring_period_time:.2f} seconds\n"
                f"Task details:\n" + "\n\n".join(task_info)
            )


    schedules_left = [heapq.heappop(task_schedules) for _ in range(len(task_schedules))]
    logger.info(
        f"Some info:\n iterations: {i}\n time elapsed: {time.time() - start_time} - max time: {scoring_period_time}\n"
        f"schedules left: {[s.task for s in schedules_left]}"
    )
    logger.info("All tasks completed. Waiting for 10 seconds to take a breath...")
    await asyncio.sleep(10)
