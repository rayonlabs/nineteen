import os
from dotenv import load_dotenv
from typing import TypeVar
import asyncio
from pydantic import BaseModel
from aiocache import cached
from substrateinterface import Keypair
from fiber.chain import chain_utils
from fiber.logging_utils import get_logger

from redis.asyncio import Redis

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

@cached(ttl=60 * 5)
async def factory_config() -> Config:

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
    except ValueError:
        logger.info("Wallet doesn't exist, trying with secret seed env variable")
        secret_seed = os.getenv("WALLET_SECRET_SEED", None)
        if secret_seed:
            keypair = load_hotkey_keypair_from_seed(secret_seed)
        else:
            raise ValueError("WALLET_SECRET_SEED env var is not set")

    return Config(
        redis_db=Redis(host=redis_host),
        psql_db=psql_db,
        netuid=netuid,
        ss58_address=ss58_address,
        replace_with_docker_localhost=replace_with_docker_localhost,
        replace_with_localhost=localhost,
        keypair=keypair,
        prod=prod
    )
