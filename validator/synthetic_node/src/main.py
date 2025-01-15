from dotenv import load_dotenv
import os

# Must be done straight away, bit ugly
load_dotenv(os.getenv("ENV_FILE", ".vali.env"))

from fiber.chain import chain_utils
from opentelemetry import metrics
import asyncio
from redis.asyncio import Redis, BlockingConnectionPool
from redis.retry import Retry
from redis.backoff import ExponentialBackoff
from fiber.logging_utils import get_logger
import json

from validator.common.query_config import Config
from validator.utils.redis import redis_constants as rcst, redis_dataclasses as rdc
from validator.synthetic_node.src.process_queries import process_task
from validator.db.src.sql.nodes import get_vali_ss58_address
from validator.db.src.database import PSQLDB

logger = get_logger(__name__)

MAX_CONCURRENT_TASKS = 100

QUERY_NODE_REQUESTS_PROCESSING_GAUGE = metrics.get_meter(__name__).create_gauge(
    name="validator.synthetic_node.src.concurrent_queries_processing",
    description="concurrent number of requests currently being processed by query node's `listen_for_tasks`",
    unit="1"
)

QUERY_NODE_FAILED_POPS_COUNTER = metrics.get_meter(__name__).create_counter(
    name="validator.synthetic_node.src.synthetic_node_failed_pops",
    description="number of failed pops from redis QUERY_QUEUE_KEY",
    unit="1"
)

QUERY_NODE_FAILED_TASKS_COUNTER = metrics.get_meter(__name__).create_counter(
    name="validator.synthetic_node.src.synthetic_node_failed_tasks",
    description="number of failed `process_task` instances",
    unit="1"
)


def create_redis_pool(host: str) -> BlockingConnectionPool:
    pool_config = {
        "max_connections": 10,
        "socket_keepalive": True,
        "retry_on_timeout": True,
        "health_check_interval": 30,
        "retry": Retry(ExponentialBackoff(cap=10, base=1), 5),
        "timeout": 20,
        "socket_connect_timeout": 10,
        "socket_timeout": 10
    }
    if "://" in host:
        return BlockingConnectionPool.from_url(host, **pool_config)
    else:
        return BlockingConnectionPool(host=host, **pool_config)

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
    prod = bool(os.getenv("ENV", "prod").lower() == "prod")

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


async def listen_for_tasks(config: Config):
    tasks: set[asyncio.Task] = set()

    logger.info("Listening for tasks.")
    while True:
        done = {t for t in tasks if t.done()}
        tasks.difference_update(done)
        for t in done:
            await t

        QUERY_NODE_REQUESTS_PROCESSING_GAUGE.set(len(tasks))
        while len(tasks) < MAX_CONCURRENT_TASKS:
            message_json = await config.redis_db.blpop(rcst.QUERY_QUEUE_KEY, timeout=1)  # type: ignore

            if not message_json:
                # QUERY_NODE_FAILED_POPS_COUNTER.add(1)
                break
            try:
                task = asyncio.create_task(process_task(config, rdc.QueryQueueMessage(**json.loads(message_json[1]))))
                tasks.add(task)
            except TypeError:
                QUERY_NODE_FAILED_TASKS_COUNTER.add(1)
                logger.error(f"Failed to process message: {message_json}")
            except (ConnectionError, TimeoutError) as e:
                logger.warning(f"Redis connection error, retrying: {e}")
                QUERY_NODE_FAILED_POPS_COUNTER.add(1)
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Unexpected error in listen_for_tasks: {e}")
                await asyncio.sleep(1)


        await asyncio.sleep(0.01)


async def main() -> None:
    config = await load_config()
    logger.debug(f"config: {config}")

    await asyncio.gather(
        listen_for_tasks(config),
    )


if __name__ == "__main__":
    asyncio.run(main())
