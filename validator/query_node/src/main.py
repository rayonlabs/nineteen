import os
import sys
import signal
import asyncio
from redis.asyncio import Redis, BlockingConnectionPool
from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
import uvicorn
import traceback
from typing import AsyncGenerator, Any
import time
from fiber.logging_utils import get_logger
import json
from core import task_config as tcfg
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
from core.models import payload_models
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi import Security
from validator.db.src.sql.api import get_api_key_rate_limit

logger = get_logger(__name__)
auth_scheme = HTTPBearer()

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
COUNTER_IMAGE_ERROR = metrics.get_meter(__name__).create_counter("validator.query_node.image.error")
COUNTER_IMAGE_SUCCESS = metrics.get_meter(__name__).create_counter("validator.query_node.image.success")
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

async def verify_api_key_rate_limit(config: Config, api_key: str):
    # remove before prod
    if api_key == "39236004-5935-46de-bedf-7f66cc12be35":
            return True
    # NOTE: abit dangerous but very useful
    if not config.prod:
        if api_key == "test":
            return True

    rate_limit_key = f"rate_limit:{api_key}"
    rate_limit = await config.redis_db.get(rate_limit_key)
    if rate_limit is None:
        async with await config.psql_db.connection() as connection:
            rate_limit = await get_api_key_rate_limit(connection, api_key)
            if rate_limit is None:
                raise HTTPException(status_code=403, detail="Invalid API key")
        await config.redis_db.set(rate_limit_key, rate_limit, ex=30)
    else:
        rate_limit = int(rate_limit)

    minute = time.time() // 60
    current_rate_limit_key = f"current_rate_limit:{api_key}:{minute}"
    current_rate_limit = await config.redis_db.get(current_rate_limit_key)
    if current_rate_limit is None:
        current_rate_limit = 0
        await config.redis_db.expire(current_rate_limit_key, 60)
    else:
        current_rate_limit = int(current_rate_limit)

    await config.redis_db.incr(current_rate_limit_key)
    if current_rate_limit >= rate_limit:
        raise HTTPException(status_code=429, detail="Too many requests")


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

async def _handle_no_stream(text_generator: AsyncGenerator[str, None]) -> JSONResponse:
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
    try:
        num_tokens = 0
        async for chunk in process_organic_task(config, message):
            num_tokens += 1
            yield chunk

        COUNTER_TEXT_GENERATION_SUCCESS.add(1, {"task": message.task})
        if num_tokens > 0:
            completion_time = time.time() - start_time
            GAUGE_TOKENS_PER_SEC.set(num_tokens / completion_time, {"task": message.task})

    except Exception as e:
        logger.error(f"Error in stream processing: {str(e)}")
        COUNTER_TEXT_GENERATION_ERROR.add(1, {
            "task": message.task,
            "error": type(e).__name__
        })
        raise

async def process_image_request(
    config: Config,
    payload: payload_models.TextToImagePayload | payload_models.ImageToImagePayload | payload_models.InpaintPayload | payload_models.AvatarPayload,
    task: str
) -> JSONResponse:
    task = task.replace("_", "-")
    task_config = tcfg.get_enabled_task_config(task)
    if task_config is None:
        COUNTER_IMAGE_ERROR.add(1, {"reason": "no_task_config"})
        logger.error(f"Task config not found for task: {task}")
        raise HTTPException(status_code=400, detail=f"Invalid model {task}")
        
    job_id = rutils.generate_job_id()
    message = rdc.QueryQueueMessage(
        task=task,
        query_type=gcst.ORGANIC,
        job_id=job_id,
        query_payload=payload.model_dump()
    )
        
    try:
        generator = process_organic_task(config, message)
        response_content = None
        
        async for chunk in generator:
            try:
                chunks = load_sse_jsons(chunk)                
                if not isinstance(chunks, list):
                    logger.warning(f"Expected list of chunks but got {type(chunks)}")
                    continue
                for chunk_data in chunks:
                    try:
                        content = chunk_data["choices"][0]["delta"]["content"]
                        try:
                            response_json = json.loads(content)
                            response_content = response_json
                            break
                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to parse content as JSON: {e}")
                            continue
                    except (KeyError, IndexError) as e:
                        logger.error(f"Error extracting content from chunk_data: {e}")
                        continue
                if response_content:
                    break
                    
            except (json.JSONDecodeError, KeyError, IndexError) as e:
                logger.error(f"Error processing chunk: {e}")
                continue
                
        if not response_content:
            logger.error(f"No valid response content received for job {job_id}")
            raise HTTPException(status_code=500, detail="No response received")
            
        try:
            logger.debug(f"Attempting to create ImageResponse from: {response_content}")
            image_response = payload_models.ImageResponse(**response_content)
        except Exception as e:
            logger.error(f"Failed to create ImageResponse: {e}")
            logger.error(f"Response content that caused error: {response_content}")
            raise HTTPException(status_code=500, detail="Invalid response format")
            
        if image_response.is_nsfw:
            logger.warning(f"NSFW content detected for job {job_id}")
            COUNTER_IMAGE_ERROR.add(1, {"task": task, "kind": "nsfw"})
            raise HTTPException(status_code=403, detail="NSFW content detected")
            
        if not image_response.image_b64:
            logger.error(f"No image data received for job {job_id}")
            COUNTER_IMAGE_ERROR.add(1, {"task": task, "kind": "no_image"})
            raise HTTPException(status_code=500, detail="No image generated")
            
        COUNTER_IMAGE_SUCCESS.add(1, {"task": task})
        return JSONResponse(content={"image_b64": image_response.image_b64})
        
    except Exception as e:
        logger.error(f"Error processing image request for job {job_id}: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Traceback: {''.join(traceback.format_tb(e.__traceback__))}")
        COUNTER_IMAGE_ERROR.add(1, {"task": task, "error": str(e)})
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/v1/image-to-image", response_model=None)
async def image_to_image(
    request: request_models.ImageToImageRequest,
    config: Config = Depends(load_config),
    credentials: HTTPAuthorizationCredentials = Security(auth_scheme)
) -> JSONResponse:
    await verify_api_key_rate_limit(config, credentials.credentials)
    payload = await request_models.image_to_image_to_payload(
        request,
        httpx_client=config.httpx_client,
        prod=config.prod,
    )
    return await process_image_request(config, payload, payload.model)

@app.post("/v1/inpaint", response_model=None)
async def inpaint(
    request: request_models.InpaintRequest,
    config: Config = Depends(load_config),
    credentials: HTTPAuthorizationCredentials = Security(auth_scheme)
) -> JSONResponse:
    await verify_api_key_rate_limit(config, credentials.credentials)
    payload = await request_models.inpaint_to_payload(
        request, 
        httpx_client=config.httpx_client, 
        prod=config.prod
    )
    return await process_image_request(config, payload, "inpaint")

@app.post("/v1/avatar", response_model=None)
async def avatar(
    request: request_models.AvatarRequest,
    config: Config = Depends(load_config),
    credentials: HTTPAuthorizationCredentials = Security(auth_scheme)
) -> JSONResponse:
    await verify_api_key_rate_limit(config, credentials.credentials)
    payload = await request_models.avatar_to_payload(
        request, 
        httpx_client=config.httpx_client, 
        prod=config.prod
    )
    return await process_image_request(config, payload, "avatar")

@app.post("/v1/text-to-image", response_model=None)
async def text_to_image(
    request: request_models.TextToImageRequest,
    config: Config = Depends(load_config),
    credentials: HTTPAuthorizationCredentials = Security(auth_scheme)
) -> JSONResponse:
    await verify_api_key_rate_limit(config, credentials.credentials)
    payload = request_models.text_to_image_to_payload(request)
    return await process_image_request(config, payload, payload.model)

@app.get("/v1/models", response_model=None)
async def models() -> list[dict[str, Any]]:
    models = tcfg.get_public_task_configs()
    new_models = []
    for model in models:
        new_model = {"model_name": model["task"]} 
        new_model.update({k: v for k, v in model.items() if k != "task"})
        new_models.append(new_model)
    return new_models


@app.post("/v1/chat/completions", response_model=None)
async def chat(
    chat_request: request_models.ChatRequest,
    config: Config = Depends(load_config),
) -> StreamingResponse | JSONResponse:
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
            success = await process_task(self.config, message)
            if not success:
                logger.warning(f"Failed to process synthetic task: {message.task}")
                
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

def signal_handler(signum, frame):
    """Handle shutdown signals."""
    logger.info(f"Received signal {signum}")
    sys.exit(0)

async def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    config = await load_config()

    task_processor = SyntheticTaskProcessor(config)

    port = int(os.getenv("API_PORT", "6919"))
    app_config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="info")
    server = uvicorn.Server(app_config)

    await asyncio.gather(
        task_processor.listen(),
        server.serve(),
    )

if __name__ == "__main__":
    asyncio.run(main())
