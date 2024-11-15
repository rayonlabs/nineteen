from validator.db.src.database import PSQLDB

from dataclasses import dataclass
import httpx
from fiber import Keypair
from fiber.logging_utils import get_logger
from redis.asyncio import Redis
from typing import Any

logger = get_logger(__name__)


@dataclass
class Config:
    keypair: Keypair
    psql_db: PSQLDB
    redis_db: Redis
    ss58_address: str
    netuid: int
    httpx_client: httpx.AsyncClient = httpx.AsyncClient()
    replace_with_localhost: bool = False
    replace_with_docker_localhost: bool = True
    prod: bool = True
