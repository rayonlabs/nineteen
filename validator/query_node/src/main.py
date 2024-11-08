from dotenv import load_dotenv
import os

# Must be done straight away, bit ugly
load_dotenv(os.getenv("ENV_FILE", ".vali.env"))

import asyncio
from redis.asyncio import Redis, BlockingConnectionPool

from fiber.logging_utils import get_logger
import json
from validator.query_node.src.query_config import Config
from validator.utils.redis import redis_constants as rcst, redis_dataclasses as rdc
from validator.query_node.src.process_queries import process_task
from validator.db.src.sql.nodes import get_vali_ss58_address
from validator.db.src.database import PSQLDB
from fiber.chain import chain_utils
from opentelemetry import metrics

logger = get_logger(__name__)

MAX_CONCURRENT_TASKS = 1000
BATCH_SIZE = 10

QUERY_NODE_REQUESTS_PROCESSING_GAUGE = metrics.get_meter(__name__).create_gauge(
    name="validator.query_node.src.concurrent_queries_processing",
    description="concurrent number of requests currently being processed by query node's `listen_for_tasks`",
    unit="1"
)

QUERY_NODE_FAILED_POPS_COUNTER = metrics.get_meter(__name__).create_counter(
    name="validator.query_node.src.query_node_failed_pops",
    description="number of failed pops from redis QUERY_QUEUE_KEY",
    unit="1"
)

QUERY_NODE_FAILED_TASKS_COUNTER = metrics.get_meter(__name__).create_counter(
    name="validator.query_node.src.query_node_failed_tasks",
    description="number of failed `process_task` instances",
    unit="1"
)

def create_redis_pool(host: str) -> BlockingConnectionPool:
    return BlockingConnectionPool(
        host=host,
        max_connections=300
    )

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
        redis_db=Redis(connection_pool=create_redis_pool(redis_host)),
        psql_db=psql_db,
        netuid=netuid,
        ss58_address=ss58_address,
        replace_with_docker_localhost=replace_with_docker_localhost,
        replace_with_localhost=localhost,
        keypair=keypair,
    )


from redis.asyncio import Redis, BlockingConnectionPool
from redis.backoff import ExponentialBackoff
from redis.retry import Retry
import asyncio
from typing import Set

class OptimizedRedisPool:
    def __init__(
        self, 
        host: str,
        max_connections: int = 500,
        timeout: int = 5,
        backoff_enabled: bool = True
    ):
        # Use exponential backoff for retries
        retry_backoff = ExponentialBackoff(cap=10, base=1)
        retry = Retry(backoff=retry_backoff, retries=3) if backoff_enabled else None
        
        # Use blocking pool with optimized settings
        self.pool = BlockingConnectionPool(
            host=host,
            max_connections=max_connections,
            timeout=timeout,
            decode_responses=True,
            socket_timeout=timeout,
            socket_connect_timeout=timeout,
            socket_keepalive=True,  # Keep connections alive
            health_check_interval=30,
            retry=retry,
            # Connection pooling optimizations
            max_idle_time=300,  # 5 minutes
            connection_class=None,  # Use default
        )
        
        self.redis: Redis = Redis(connection_pool=self.pool)
        
    async def batch_blpop(
        self,
        key: str,
        batch_size: int,
        timeout: float = 1.0
    ) -> list:
        """
        Optimized batch BLPOP with connection management
        """
        tasks = set()
        results = []
        
        # Create batch of BLPOP operations
        for _ in range(batch_size):
            task = asyncio.create_task(self.redis.blpop(key, timeout=timeout))
            tasks.add(task)
        
        # Wait for all tasks with timeout
        done, pending = await asyncio.wait(
            tasks,
            timeout=timeout,
            return_when=asyncio.ALL_COMPLETED
        )
        
        # Cancel any pending tasks
        for task in pending:
            task.cancel()
        
        # Gather results
        for task in done:
            try:
                result = await task
                if result:
                    results.append(result)
            except Exception as e:
                logger.error(f"Error in BLPOP: {e}")
                
        return results

    async def pipeline_ops(self, operations: list) -> list:
        """
        Execute multiple non-blocking operations in a pipeline
        """
        async with self.redis.pipeline(transaction=False) as pipe:
            # Queue operations
            for op, *args in operations:
                getattr(pipe, op)(*args)
            # Execute with automatic retry
            return await pipe.execute()
            
    async def close(self):
        """Cleanup connections"""
        await self.redis.close()
        await self.pool.disconnect()

# Usage in your code:
async def listen_for_tasks(config: Config):
    localhost = bool(os.getenv("LOCALHOST", "false").lower() == "true")
    if localhost:
        redis_host = "localhost"
        os.environ["POSTGRES_HOST"] = "localhost"
    else:
        redis_host = os.getenv("REDIS_HOST", "redis")
    redis_pool = OptimizedRedisPool(
        host=redis_host,
        max_connections=MAX_CONCURRENT_TASKS * 2
    )
    
    tasks: Set[asyncio.Task] = set()
    
    try:
        while True:
            # Clean up completed tasks
            done = {t for t in tasks if t.done()}
            tasks.difference_update(done)
            for t in done:
                try:
                    await t
                except Exception as e:
                    logger.error(f"Task failed with error: {e}")
                    
            available_slots = MAX_CONCURRENT_TASKS - len(tasks)
            if available_slots > 0:
                # Get batch of messages
                batch_size = min(BATCH_SIZE, available_slots)
                messages = await redis_pool.batch_blpop(
                    rcst.QUERY_QUEUE_KEY,
                    batch_size=batch_size
                )
                
                # Process messages
                for msg in messages:
                    if msg:
                        try:
                            task = asyncio.create_task(
                                process_task(config, rdc.QueryQueueMessage(**json.loads(msg[1])))
                            )
                            tasks.add(task)
                        except TypeError:
                            logger.error(f"Failed to process message: {msg}")
                            
            await asyncio.sleep(0.01)  # Prevent CPU spinning
            
    finally:
        await redis_pool.close()


async def main() -> None:
    config = await load_config()
    logger.debug(f"config: {config}")

    await asyncio.gather(
        listen_for_tasks(config),
    )


if __name__ == "__main__":
    asyncio.run(main())
