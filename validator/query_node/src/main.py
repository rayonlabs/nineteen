from dotenv import load_dotenv
import os

load_dotenv(os.getenv("ENV_FILE", ".vali.env"))

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
from validator.query_node.src.process_queries import process_task
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

async def _handle_no_stream(text_generator: AsyncGenerator[str, None]) -> JSONResponse:
    all_content = ""
    try:
        async for chunk in text_generator:
            logger.debug(f"Received chunk: {chunk}")
            
            try:
                chunks = load_sse_jsons(chunk)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode chunk: {e}")
                raise HTTPException(status_code=500, detail="Invalid response format")

            if not isinstance(chunks, list):
                logger.error(f"Unexpected chunk format: {chunks}")
                raise HTTPException(status_code=500, detail="Invalid response format")

            for chunk in chunks:
                try:
                    content = chunk["choices"][0]["delta"]["content"]
                    all_content += content
                    if content == "":
                        break
                except (KeyError, IndexError) as e:
                    logger.error(f"Malformed chunk structure: {e}")
                    raise HTTPException(status_code=500, detail="Invalid response structure")

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

async def _process_stream(
    config: Config,
    message: rdc.QueryQueueMessage,
    start_time: float
) -> AsyncGenerator[str, None]:
    try:
        success = await process_task(config, message)
        if not success:
            raise HTTPException(status_code=500, detail="Failed to process request")

        response_queue = await rutils.get_response_queue_key(message.job_id)
        received_done = False
        num_tokens = 0

        try:
            while True:
                result = await config.redis_db.blpop(response_queue, timeout=2)
                if result is None:
                    logger.error(f"Timeout waiting for response in queue {response_queue}")
                    raise HTTPException(status_code=500, detail="Request timed out")

                _, data = result
                try:
                    if not data:
                        continue

                    content = json.loads(data.decode())
                    if gcst.STATUS_CODE in content and content[gcst.STATUS_CODE] >= 400:
                        raise HTTPException(
                            status_code=content[gcst.STATUS_CODE],
                            detail=content.get(gcst.ERROR_MESSAGE, "Unknown error")
                        )

                    if gcst.CONTENT in content:
                        content_str = content[gcst.CONTENT]
                        num_tokens += 1
                        yield content_str

                        if "[DONE]" in content_str:
                            received_done = True
                            break

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode message: {e}")
                    continue

            COUNTER_TEXT_GENERATION_SUCCESS.add(1, {"task": message.task})
            completion_time = time.time() - start_time
            if num_tokens > 0:
                GAUGE_TOKENS_PER_SEC.set(num_tokens / completion_time)

        finally:
            await rutils.ensure_queue_clean(config.redis_db, message.job_id)
            if not received_done:
                raise HTTPException(status_code=500, detail="Incomplete response")

    except Exception as e:
        logger.error(f"Error in stream processing: {str(e)}")
        COUNTER_TEXT_GENERATION_ERROR.add(1, {
            "task": message.task,
            "error": type(e).__name__
        })
        raise

async def get_config() -> Config:
    return app.state.config

@app.post("/v1/chat/completions")
async def chat(
    chat_request: request_models.ChatRequest,
    config: Config = Depends(get_config),
) -> StreamingResponse | JSONResponse:
    payload = request_models.chat_to_payload(chat_request)
    payload.temperature = 0.5
    job_id = rutils.generate_job_id()
    start_time = time.time()

    try:
        message = rdc.QueryQueueMessage(
            task=payload.model,
            query_type=gcst.ORGANIC,
            job_id=job_id,
            query_payload=payload.model_dump()
        )

        text_generator = _process_stream(config, message, start_time)

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
    def __init__(self, config: Config):
        self.config = config
        self.tasks: set[asyncio.Task] = set()
        self.MAX_CONCURRENT_TASKS = 1000
        self.running = True
        
    async def process_synthetic_message(self, message_data: bytes):
        try:
            message = rdc.QueryQueueMessage(**json.loads(message_data))
            if message.query_type != gcst.SYNTHETIC:
                logger.warning(f"Non-synthetic message in synthetic queue: {message.query_type}")
                return
                
            logger.info(f"Processing synthetic query for task: {message.task}")
            await process_task(self.config, message)
            
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
        self.running = False
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)

async def run_api_server(config: Config):
    app.state.config = config
    
    uvicorn_config = uvicorn.Config(
        app=app,
        host="0.0.0.0",
        port=8000,
        loop="asyncio"
    )
    server = uvicorn.Server(uvicorn_config)
    await server.serve()

async def main() -> None:
    config = await load_config()
    logger.debug(f"config: {config}")

    task_processor = SyntheticTaskProcessor(config)
    
    try:
        api_server = asyncio.create_task(run_api_server(config))
        synthetic_listener = asyncio.create_task(task_processor.listen())
        
        await asyncio.gather(api_server, synthetic_listener)
        
    except asyncio.CancelledError:
        logger.info("Shutting down query node...")
    finally:
        await task_processor.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Query node shutdown complete")