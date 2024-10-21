from typing import Any
from redis.asyncio import Redis
import json
from enum import Enum
import copy
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


def _remove_enums(map: dict[Any, Any]) -> dict[Any, Any]:
    map_copy = copy.copy(map)
    for k, v in zip(list(map.keys()), list(map.values())):
        if isinstance(k, Enum):
            map_copy[k.value] = v
            del map_copy[k]
        if isinstance(v, Enum):
            map_copy[k] = v.value
    return map_copy


async def delete_key_from_redis(redis_db: Redis, key: str) -> None:
    await redis_db.delete(key)


async def json_load_from_redis(redis_db: Redis, key: str, default: Any) -> dict[Any, Any]:
    raw_json = await redis_db.get(key)
    if raw_json is None:
        json_obj = default
    else:
        json_obj = json.loads(raw_json)

    return json_obj


async def add_json_to_redis_list(redis_db: Redis, queue: str, json_to_add: dict[Any, Any]) -> None:
    json_to_add = _remove_enums(json_to_add)
    json_string = json.dumps(json_to_add)

    await redis_db.rpush(queue, json_string)  # type: ignore


async def add_str_to_redis_list(redis_db: Redis, queue: str, value_to_add: str, max_len: int | None = None) -> None:
    await redis_db.rpush(queue, value_to_add)  # type: ignore
    if max_len is not None:
        await redis_db.ltrim(queue, 0, max_len - 1)  # type: ignore


async def get_redis_list(redis_db: Redis, queue: str) -> list[str]:
    return await redis_db.lrange(queue, 0, -1)  # type: ignore


async def get_sorted_set(redis_db: Redis, name: str) -> list[str]:
    return await redis_db.zrevrange(name, 0, -1)  # type: ignore


async def check_value_is_in_set(redis_db: Redis, name: str, value) -> bool:
    return await redis_db.sismember(name, value)  # type: ignore


async def remove_value_from_set(redis_db: Redis, name: str, value: str) -> None:
    await redis_db.srem(name, value)  # type: ignore
