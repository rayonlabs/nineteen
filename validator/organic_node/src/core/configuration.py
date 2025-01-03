import os
from dotenv import load_dotenv
from typing import TypeVar
import asyncio
from pydantic import BaseModel
from substrateinterface import Keypair
from fiber.chain import chain_utils
from fiber.logging_utils import get_logger


from redis.asyncio import Redis, BlockingConnectionPool

from validator.db.src.database import PSQLDB
from validator.common.query_config import Config
from validator.db.src.sql.nodes import get_vali_ss58_address



T = TypeVar("T", bound=BaseModel)

load_dotenv()

logger = get_logger(__name__)

def load_hotkey_keypair_from_seed(secret_seed: str) -> Keypair:
    try:
        keypair = Keypair.create_from_seed(secret_seed)
        logger.info("Loaded keypair from seed directly!")
        return keypair
    except Exception as e:
        raise ValueError(f"Failed to load keypair: {str(e)}")

def create_redis_pool(
    host: str,
    max_connections: int = 100,
    timeout: float = 20.0,
    idle_timeout: int = 20,
    max_idle_connections: int | None = None,
    retry_on_timeout: bool = True,
    socket_keepalive: bool = True,
    socket_timeout: float = 10.0,
) -> BlockingConnectionPool:
    pool_kwargs = {
        "max_connections": max_connections,
        "timeout": timeout,
        "retry_on_timeout": retry_on_timeout,
        "socket_timeout": socket_timeout,
        "socket_keepalive": socket_keepalive,
    }

    if max_idle_connections is not None:
        pool_kwargs["max_idle_connections"] = max_idle_connections

    if "://" in host:
        return BlockingConnectionPool.from_url(
            host,
            **pool_kwargs
        )
    else:
        pool_kwargs["host"] = host
        pool_kwargs["timeout"] = idle_timeout
        return BlockingConnectionPool(**pool_kwargs)

async def load_config_once() -> Config:

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

    prod = bool(os.getenv("ENV", "prod").lower() == "prod")

    wallet_name = os.getenv("WALLET_NAME", "default")
    hotkey_name = os.getenv("HOTKEY_NAME", "default")

    try:
        keypair = chain_utils.load_hotkey_keypair(wallet_name=wallet_name, hotkey_name=hotkey_name)

    except (ValueError, FileNotFoundError) as e:
        logger.info("Attempting to use WALLET_SECRET_SEED environment variable")
        secret_seed = os.getenv("WALLET_SECRET_SEED", None)
        if secret_seed:
            try:
                keypair = load_hotkey_keypair_from_seed(secret_seed)
            except Exception as e:
                logger.error(f"Failed to load keypair from seed: {str(e)}")
                raise ValueError(f"Invalid secret seed provided: {str(e)}")
        else:
            logger.error("WALLET_SECRET_SEED environment variable not set")
            raise ValueError(f"Could not load wallet from path and WALLET_SECRET_SEED env var is not set. Original error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error loading hotkey from wallet: {str(e)}")
        raise


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

async def factory_config():
    global _config
    if not _config:
        _config = await load_config_once()
    return _config
