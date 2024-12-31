from dataclasses import dataclass
from fiber import Keypair
import httpx
from fiber.logging_utils import get_logger
from validator.db.src.database import PSQLDB
from redis.asyncio import Redis

logger = get_logger(__name__)


@dataclass
class Config:
    keypair: Keypair
    psql_db: PSQLDB
    redis_db: Redis
    ss58_address: str
    netuid: int
    httpx_client: httpx.AsyncClient = httpx.AsyncClient()
    scoring_period_time_multiplier: float = 1
    replace_with_localhost: bool = False
    replace_with_docker_localhost: bool = True
