import os
from dotenv import load_dotenv
from typing import TypeVar
import asyncio
from pydantic import BaseModel
from aiocache import cached

from validator.db.src.database import PSQLDB
from validator.common.query_config import Config
from validator.db.src.sql.nodes import get_vali_ss58_address


T = TypeVar("T", bound=BaseModel)

load_dotenv()

@cached(ttl=60 * 5)
async def factory_config() -> Config:

    netuid = os.getenv("NETUID")
    if netuid is None:
        raise ValueError("NETUID must be set")
    else:
        netuid = int(netuid)

    localhost = bool(os.getenv("LOCALHOST", "false").lower() == "true")
    if localhost:
        os.environ["POSTGRES_HOST"] = "localhost"

    replace_with_docker_localhost = bool(os.getenv("REPLACE_WITH_DOCKER_LOCALHOST", "false").lower() == "true")

    psql_db = PSQLDB()
    await psql_db.connect()

    ss58_address = None
    while ss58_address is None:
        ss58_address = await get_vali_ss58_address(psql_db, netuid)
        await asyncio.sleep(0.1)

    prod = bool(os.getenv("ENV", "prod").lower() == "prod")

    return Config(
        redis_db=None,
        psql_db=psql_db,
        netuid=netuid,
        ss58_address=ss58_address,
        replace_with_docker_localhost=replace_with_docker_localhost,
        replace_with_localhost=localhost,
        keypair=None,
        prod=prod
    )
