from validator.utils.generic import generic_constants as gcst
from validator.hybrid_node.src.query_config import Config
from validator.utils.redis import redis_constants as rcst, redis_dataclasses as rdc
from validator.hybrid_node.src.handlers.process_queries import process_synthetic_task

import asyncio
import json
from opentelemetry import metrics
from fiber.logging_utils import get_logger


logger = get_logger(__name__)

# Metrics
QUERY_NODE_REQUESTS_PROCESSING_GAUGE = metrics.get_meter(__name__).create_gauge(
    name="validator.hybrid_node.src.concurrent_synthetic_queries_processing",
    description="concurrent number of synthetic requests currently being processed",
    unit="1"
)

QUERY_NODE_FAILED_SYNTHETIC_TASKS_COUNTER = metrics.get_meter(__name__).create_counter(
    name="validator.hybrid_node.src.hybrid_node_failed_synthetic_tasks",
    description="number of failed synthetic `process_task` instances",
    unit="1"
)

class SyntheticTaskProcessor:    
    def __init__(self, config: Config):
        self.config = config
        self.tasks: set[asyncio.Task] = set()
        self.MAX_CONCURRENT_TASKS = 200
        self.running = True
        
    async def process_synthetic_message(self, message_data: bytes):
        try:
            message = rdc.QueryQueueMessage(**json.loads(message_data))
            if message.query_type != gcst.SYNTHETIC:
                logger.warning(f"Non-synthetic message in synthetic queue: {message.query_type}")
                return
                
            logger.info(f"Processing synthetic query for task: {message.task}")
            success = await process_synthetic_task(self.config, message)
            if not success:
                logger.error(f"Failed to process synthetic task: {message.task}")
                
        except Exception as e:
            logger.error(f"Error processing synthetic message: {e}")
            QUERY_NODE_FAILED_SYNTHETIC_TASKS_COUNTER.add(1, {
                "error": type(e).__name__
            })

    async def cleanup_done_tasks(self):
        done = {t for t in self.tasks if t.done()}
        for task in done:
            try:
                await task
            except Exception as e:
                logger.error(f"Task failed with error: {e}")
            finally:
                self.tasks.remove(task)

    async def listen(self):
        logger.info("Starting synthetic query listener")
        
        while self.running:
            try:
                await self.cleanup_done_tasks()
                QUERY_NODE_REQUESTS_PROCESSING_GAUGE.set(len(self.tasks))                
                if len(self.tasks) >= self.MAX_CONCURRENT_TASKS:
                    await asyncio.sleep(0.1)
                    continue
                
                message_data = await self.config.redis_db.blpop(rcst.QUERY_QUEUE_KEY, timeout=1)
                if not message_data:
                    continue
                    
                _, data = message_data
                task = asyncio.create_task(self.process_synthetic_message(data))
                self.tasks.add(task)
                
            except asyncio.CancelledError:
                logger.info("Synthetic task listener cancelled")
                self.running = False
                break
            except Exception as e:
                logger.error(f"Error in synthetic task listener: {e}")
                await asyncio.sleep(1)

    async def stop(self):
        """Stop processing and cleanup."""
        self.running = False
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)