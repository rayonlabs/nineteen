import os
from dotenv import load_dotenv
from typing import TypeVar
import httpx
from pydantic import BaseModel
from redis.asyncio import Redis
from redis.asyncio.connection import ConnectionPool
from aiocache import cached
from validator.db.src.database import PSQLDB

T = TypeVar("T", bound=BaseModel)

load_dotenv()

from dataclasses import dataclass


@dataclass
class Config:
    redis_db: Redis
    psql_db: PSQLDB
    prod: bool
    httpx_client: httpx.AsyncClient

async def create_config() -> Config:
    localhost = bool(os.getenv("LOCALHOST", "false").lower() == "true")
    if localhost:
        redis_host = "localhost"
        os.environ["POSTGRES_HOST"] = "localhost"
    else:
        redis_host = os.getenv("REDIS_HOST", "redis")

    psql_db = PSQLDB()
    await psql_db.connect()

    pool = ConnectionPool(
        host=redis_host,
        max_connections=32,
        retry_on_timeout=True,
        decode_responses=True,
        socket_timeout=5.0,
        socket_connect_timeout=5.0,
        socket_keepalive=True,
        health_check_interval=30,
        retry_on_error=[ConnectionError, TimeoutError]
    )

    redis_db = Redis(connection_pool=pool)

    prod = bool(os.getenv("ENV", "prod").lower() == "prod")

    return Config(
        psql_db=psql_db,
        redis_db=redis_db,
        prod=prod,
        httpx_client=httpx.AsyncClient(),
    )

_config = None

async def factory_config() -> Config:
    global _config 
    if not _config:
        _config = await create_config()
    return _config