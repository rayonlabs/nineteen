import os
import asyncio
from redis.asyncio import Redis, BlockingConnectionPool

from validator.query_node.src.query_config import Config
from validator.db.src.sql.nodes import get_vali_ss58_address
from validator.db.src.database import PSQLDB
from fiber.chain import chain_utils
from fiber.logging_utils import get_logger


logger = get_logger(__name__)

def create_redis_pool(host: str) -> BlockingConnectionPool:
    return BlockingConnectionPool(
        host=host,
        max_connections=300,
        timeout=20 
    )

async def load_config_once() -> Config:
    wallet_name = os.getenv("WALLET_NAME", "default")
    hotkey_name = os.getenv("HOTKEY_NAME", "default")

    netuid = os.getenv("NETUID")
    if netuid is None:
        raise ValueError("NETUID must be set")
    else:
        netuid = int(netuid)

    prod = bool(os.getenv("ENV", "prod").lower() == "prod")
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
    redis_pool = create_redis_pool(redis_host)

    return Config(
        redis_db=Redis(connection_pool=redis_pool),
        psql_db=psql_db,
        netuid=netuid,
        ss58_address=ss58_address,
        replace_with_docker_localhost=replace_with_docker_localhost,
        replace_with_localhost=localhost,
        keypair=keypair,
        prod=prod
    )

_config = None

async def load_config():
    global _config
    if not _config:
        _config = await load_config_once()
    return _config

