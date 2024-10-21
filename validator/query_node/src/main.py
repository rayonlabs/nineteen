from dotenv import load_dotenv
import os

# Must be done straight away, bit ugly
load_dotenv(os.getenv("ENV_FILE", ".vali.env"))

import asyncio
from redis.asyncio import Redis

from fiber.logging_utils import get_logger
import json
from validator.query_node.src.query_config import Config
from validator.utils.redis import redis_constants as rcst, redis_dataclasses as rdc
from validator.query_node.src.process_queries import process_task
from validator.db.src.sql.nodes import get_vali_ss58_address
from validator.db.src.database import PSQLDB
from fiber.chain import chain_utils

logger = get_logger(__name__)

MAX_CONCURRENT_TASKS = 100


async def load_config() -> Config:
    wallet_name = os.getenv("WALLET_NAME", "default")
    hotkey_name = os.getenv("HOTKEY_NAME", "default")

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

    keypair = chain_utils.load_hotkey_keypair(wallet_name=wallet_name, hotkey_name=hotkey_name)

    return Config(
        redis_db=Redis(host=redis_host),
        psql_db=psql_db,
        netuid=netuid,
        ss58_address=ss58_address,
        replace_with_docker_localhost=replace_with_docker_localhost,
        replace_with_localhost=localhost,
        keypair=keypair,
    )


async def listen_for_tasks(config: Config):
    tasks: set[asyncio.Task] = set()

    logger.info("Listening for tasks.")
    while True:
        done = {t for t in tasks if t.done()}
        tasks.difference_update(done)
        for t in done:
            await t

        while len(tasks) < MAX_CONCURRENT_TASKS:
            message_json = await config.redis_db.blpop(rcst.QUERY_QUEUE_KEY, timeout=1)  # type: ignore

            if not message_json:
                break
            try:
                task = asyncio.create_task(process_task(config, rdc.QueryQueueMessage(**json.loads(message_json[1]))))
                tasks.add(task)
            except TypeError:
                logger.error(f"Failed to process message: {message_json}")

        await asyncio.sleep(0.01)


async def main() -> None:
    config = await load_config()
    logger.debug(f"config: {config}")

    await asyncio.gather(
        listen_for_tasks(config),
    )


if __name__ == "__main__":
    asyncio.run(main())
