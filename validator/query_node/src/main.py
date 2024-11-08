from dotenv import load_dotenv
import os
import sys
import signal
import asyncio
from redis.asyncio import Redis, BlockingConnectionPool
from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
import uvicorn
from typing import AsyncGenerator
import time

from fiber.logging_utils import get_logger
import json
from validator.utils.generic import generic_constants as gcst
from validator.query_node.src.query_config import Config
from validator.utils.redis import redis_constants as rcst, redis_dataclasses as rdc
from validator.query_node.src.process_queries import process_task, process_organic_task
from validator.db.src.sql.nodes import get_vali_ss58_address
from validator.db.src.database import PSQLDB
from fiber.chain import chain_utils
from opentelemetry import metrics
from validator.query_node.src import request_models
from validator.utils.query.query_utils import load_sse_jsons
import validator.utils.redis.redis_utils as rutils

logger = get_logger(__name__)

# Metrics
QUERY_NODE_REQUESTS_PROCESSING_GAUGE = metrics.get_meter(__name__).create_gauge(
    name="validator.query_node.src.concurrent_synthetic_queries_processing",
    description="concurrent number of synthetic requests currently being processed",
    unit="1"
)

QUERY_NODE_FAILED_SYNTHETIC_TASKS_COUNTER = metrics.get_meter(__name__).create_counter(
    name="validator.query_node.src.query_node_failed_synthetic_tasks",
    description="number of failed synthetic `process_task` instances",
    unit="1"
)

COUNTER_TEXT_GENERATION_ERROR = metrics.get_meter(__name__).create_counter("validator.query_node.text.error")
COUNTER_TEXT_GENERATION_SUCCESS = metrics.get_meter(__name__).create_counter("validator.query_node.text.success")
GAUGE_TOKENS_PER_SEC = metrics.get_meter(__name__).create_gauge(
    "validator.query_node.text.tokens_per_sec",
    description="Average tokens per second metric for LLM streaming"
)

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
    )

_config = None

async def load_config():
    global _config
    if not _config:
        _config = await load_config_once()
    return _config

async def _handle_no_stream(text_generator: AsyncGenerator[str, None]) -> JSONResponse:
    """Handle non-streaming response by accumulating content."""
    all_content = ""
    try:
        async for chunk in text_generator:
            try:
                chunks = load_sse_jsons(chunk)
                if not isinstance(chunks, list):
                    raise HTTPException(status_code=500, detail="Invalid response format")

                for chunk in chunks:
                    content = chunk["choices"][0]["delta"]["content"]
                    all_content += content
                    if content == "":
                        break

            except (json.JSONDecodeError, KeyError, IndexError) as e:
                logger.error(f"Error processing chunk: {e}")
                raise HTTPException(status_code=500, detail="Invalid response format")

        return JSONResponse({
            "choices": [{
                "delta": {"content": all_content}
            }]
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in non-streaming response: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to process response")

async def process_organic_stream(
    config: Config,
    message: rdc.QueryQueueMessage,
    start_time: float
) -> AsyncGenerator[str, None]:
    """Process organic stream request and track metrics."""
    try:
        num_tokens = 0
        async for chunk in process_organic_task(config, message):
            num_tokens += 1
            yield chunk

        COUNTER_TEXT_GENERATION_SUCCESS.add(1, {"task": message.task})
        if num_tokens > 0:
            completion_time = time.time() - start_time
            GAUGE_TOKENS_PER_SEC.set(num_tokens / completion_time)

    except Exception as e:
        logger.error(f"Error in stream processing: {str(e)}")
        COUNTER_TEXT_GENERATION_ERROR.add(1, {
            "task": message.task,
            "error": type(e).__name__
        })
        raise

@app.post("/v1/chat/completions", response_model=None)
async def chat(
    chat_request: request_models.ChatRequest,
    config: Config = Depends(load_config),
) -> StreamingResponse | JSONResponse:
    """Handle chat completion requests."""
    payload = request_models.chat_to_payload(chat_request)
    job_id = rutils.generate_job_id()
    start_time = time.time()

    try:
        message = rdc.QueryQueueMessage(
            task=payload.model,
            query_type=gcst.ORGANIC,
            job_id=job_id,
            query_payload=payload.model_dump()
        )

        text_generator = process_organic_stream(config, message, start_time)

        if chat_request.stream:
            return StreamingResponse(text_generator, media_type="text/event-stream")
        else:
            return await _handle_no_stream(text_generator)

    except HTTPException as http_exc:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in chat endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred")

class SyntheticTaskProcessor:
    """Process synthetic tasks from Redis queue."""
    
    def __init__(self, config: Config):
        self.config = config
        self.tasks: set[asyncio.Task] = set()
        self.MAX_CONCURRENT_TASKS = 1000
        self.running = True
        
    async def process_synthetic_message(self, message_data: bytes):
        """Process single synthetic message."""
        try:
            message = rdc.QueryQueueMessage(**json.loads(message_data))
            if message.query_type != gcst.SYNTHETIC:
                logger.warning(f"Non-synthetic message in synthetic queue: {message.query_type}")
                return
                
            logger.info(f"Processing synthetic query for task: {message.task}")
            success = await process_task(self.config, message)
            if not success:
                logger.warning(f"Failed to process synthetic task: {message.task}")
                
        except Exception as e:
            logger.error(f"Error processing synthetic message: {e}")
            QUERY_NODE_FAILED_SYNTHETIC_TASKS_COUNTER.add(1, {
                "error": type(e).__name__
            })

    async def cleanup_done_tasks(self):
        """Cleanup completed tasks."""
        done = {t for t in self.tasks if t.done()}
        for task in done:
            try:
                await task
            except Exception as e:
                logger.error(f"Task failed with error: {e}")
            finally:
                self.tasks.remove(task)

    async def listen(self):
        """Listen for synthetic tasks on Redis queue."""
        logger.info("Starting synthetic query listener")
        
        while self.running:
            try:
                await self.cleanup_done_tasks()
                QUERY_NODE_REQUESTS_PROCESSING_GAUGE.set(len(self.tasks))                
                if len(self.tasks) >= self.MAX_CONCURRENT_TASKS:
                    await asyncio.sleep(0.1)
                    continue
                
                message_data = await self.config.redis_db.blpop(rcst.QUERY_QUEUE_KEY, timeout=0.1)
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

def run_api_server():
    """Run FastAPI server."""
    try:
        #config = asyncio.run(load_config())
        port = int(os.getenv("API_PORT", "6919"))
        uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
    except Exception as e:
        logger.error(f"Error starting API server: {e}")
        sys.exit(1)

async def main() -> None:
    """Main entry point."""
    pass
    #task_processor = SyntheticTaskProcessor(config)
    
    #import multiprocessing
    #api_process = multiprocessing.Process(target=run_api_server)
    #api_process.start()
    
    #try:
    #    await task_processor.listen()
    #except asyncio.CancelledError:
    #    logger.info("Shutting down query node...")
    #finally:
    #    await task_processor.stop()
    #    api_process.terminate()

def signal_handler(signum, frame):
    """Handle shutdown signals."""
    logger.info(f"Received signal {signum}")
    sys.exit(0)

if __name__ == "__main__":
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Load the configuration
    config = asyncio.run(load_config())

    # Create the task processor
    task_processor = SyntheticTaskProcessor(config)

    # Run the task processor and Uvicorn server concurrently
    async def main():
        task = asyncio.create_task(task_processor.listen())
        await asyncio.gather(
            task,
            uvicorn.run(app, host="0.0.0.0", port=port, log_level="info"),
        )

    port = int(os.getenv("API_PORT", "6919"))
    asyncio.run(main())