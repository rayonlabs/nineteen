import os
from dotenv import load_dotenv
from typing import TypeVar
import httpx
from pydantic import BaseModel
from redis.asyncio import Redis
from aiocache import cached
from dataclasses import dataclass

from validator.db.src.database import PSQLDB


T = TypeVar("T", bound=BaseModel)

load_dotenv()

@dataclass
class Config:
    redis_db: Redis
    psql_db: PSQLDB
    prod: bool
    httpx_client: httpx.AsyncClient
    netuid: int


@cached(ttl=60 * 5)
async def factory_config() -> Config:
    localhost = bool(os.getenv("LOCALHOST", "false").lower() == "true")
    if localhost:
        redis_host = "localhost"
        os.environ["POSTGRES_HOST"] = "localhost"
    else:
        redis_host = os.getenv("REDIS_HOST", "redis")

    psql_db = PSQLDB()
    await psql_db.connect()
    redis_db = Redis(host=redis_host)

    prod = bool(os.getenv("ENV", "prod").lower() == "prod")

    netuid = os.getenv("NETUID")
    if netuid is None:
        raise ValueError("NETUID must be set")
    else:
        netuid = int(netuid)

    return Config(
        psql_db=psql_db,
        redis_db=redis_db,
        prod=prod,
        httpx_client=httpx.AsyncClient(),
        netuid=netuid
    )
