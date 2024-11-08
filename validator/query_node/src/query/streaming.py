import json
import time
from typing import AsyncGenerator

import httpx
from core.models import utility_models
from validator.query_node.src.query_config import Config
from validator.query_node.src import utils
import validator.utils.redis.redis_utils as rutils

from validator.models import Contender
from fiber.validator import client
from fiber.networking.models import NodeWithFernet as Node
from core import task_config as tcfg
from validator.utils.generic import generic_constants as gcst
from validator.utils.redis import redis_constants as rcst

from fiber.logging_utils import get_logger
from validator.utils.query.query_utils import load_sse_jsons

logger = get_logger(__name__)


def _get_formatted_payload(content: str, first_message: bool, add_finish_reason: bool = False) -> str:
    delta_payload = {"content": content}
    if first_message:
        delta_payload["role"] = "assistant"
    choices_payload: dict[str, str | dict[str, str]] = {"delta": delta_payload}
    if add_finish_reason:
        choices_payload["finish_reason"] = "stop"
    payload = {
        "choices": [choices_payload],
    }
    return json.dumps(payload)

async def _handle_event(
    config: Config,
    content: str | None,
    synthetic_query: bool,
    job_id: str,
    status_code: int,
    error_message: str | None = None,
    ttl: int = rcst.RESPONSE_QUEUE_TTL
) -> None:
    if synthetic_query:
        return

    response_queue = await rutils.get_response_queue_key(job_id)
    
    if content is not None:
        if isinstance(content, dict):
            content = json.dumps(content)
        event_data = json.dumps({
            gcst.CONTENT: content,
            gcst.STATUS_CODE: status_code
        })
    else:
        event_data = json.dumps({
            gcst.ERROR_MESSAGE: error_message,
            gcst.STATUS_CODE: status_code
        })
    
    # Use pipeline for atomic operations
    async with config.redis_db.pipeline() as pipe:
        await pipe.rpush(response_queue, event_data)
        await pipe.expire(response_queue, ttl)
        await pipe.execute()

async def async_chain(first_chunk, async_gen):
    yield first_chunk
    async for item in async_gen:
        yield item

def construct_500_query_result(node: Node, task: str) -> utility_models.QueryResult:
    query_result = utility_models.QueryResult(
        node_id=node.node_id,
        task=task,
        success=False,
        node_hotkey=node.hotkey,
        formatted_response=None,
        status_code=500,
        response_time=None,
    )
    return query_result

async def consume_generator(
    config: Config,
    generator: AsyncGenerator,
    job_id: str,
    synthetic_query: bool,
    contender: Contender,
    node: Node,
    payload: dict,
    start_time: float,
    debug: bool = False,
) -> bool:
    assert job_id
    task = contender.task
    query_result = None
    response_queue = await rutils.get_response_queue_key(job_id)

    try:
        first_chunk = await generator.__anext__()
    except (StopAsyncIteration, httpx.ConnectError, httpx.ReadError, httpx.HTTPError, httpx.ReadTimeout, Exception) as e:
        error_type = type(e).__name__
        error_details = str(e)

        logger.error(f"Error when querying node: {node.node_id} for task: {task}. Error: {error_type} - {error_details}")
        query_result = construct_500_query_result(node, task)
        await utils.adjust_contender_from_result(config, query_result, contender, synthetic_query, payload=payload)
        return False

    text_jsons, status_code, first_message = [], 200, True
    
    # Pipeline for batching redis operations
    pipe = config.redis_db.pipeline()
    pipeline_size = 0
    MAX_PIPELINE_SIZE = 10  # Adjust based on your needs
    
    try:
        async for text in async_chain(first_chunk, generator):
            if isinstance(text, bytes):
                text = text.decode()
            if isinstance(text, str):
                try:
                    loaded_jsons = load_sse_jsons(text)
                    if isinstance(loaded_jsons, dict):
                        status_code = loaded_jsons.get(gcst.STATUS_CODE)
                        break

                except (IndexError, json.JSONDecodeError) as e:
                    logger.warning(f"Error {e} when trying to load text: {text}")
                    break

                for text_json in loaded_jsons:
                    if not isinstance(text_json, dict):
                        logger.debug(f"Invalid text_json because its not a dict?: {text_json}")
                        first_message = True
                        break
                    try:
                        _ = text_json["choices"][0]["delta"]["content"]
                    except KeyError:
                        logger.debug(f"Invalid text_json because there's not delta content: {text_json}")
                        first_message = True
                        break

                    text_jsons.append(text_json)
                    dumped_payload = json.dumps(text_json)
                    first_message = False
                    
                    s = time.time()
                    
                    # Add commands to pipeline
                    event_data = json.dumps({
                        gcst.CONTENT: f"data: {dumped_payload}\n\n",
                        gcst.STATUS_CODE: 200
                    })
                    pipe.rpush(response_queue, event_data)
                    pipe.expire(response_queue, rcst.RESPONSE_QUEUE_TTL)
                    pipeline_size += 1

                    # Execute pipeline when batch size is reached
                    if pipeline_size >= MAX_PIPELINE_SIZE:
                        await pipe.execute()
                        pipeline_size = 0
                        pipe = config.redis_db.pipeline()
                    
                    e = time.time()
                    logger.info(f"time to send chunk : {round(e-s, 4)}")

        # Execute any remaining pipeline commands
        if pipeline_size > 0:
            await pipe.execute()

        if len(text_jsons) > 0:
            # Handle final messages with a single pipeline
            async with config.redis_db.pipeline() as final_pipe:
                # Last payload
                last_payload = _get_formatted_payload("", False, add_finish_reason=True)
                event_data = json.dumps({
                    gcst.CONTENT: f"data: {last_payload}\n\n",
                    gcst.STATUS_CODE: 200
                })
                final_pipe.rpush(response_queue, event_data)
                final_pipe.expire(response_queue, rcst.RESPONSE_QUEUE_TTL)

                # Done message
                done_data = json.dumps({
                    gcst.CONTENT: "data: [DONE]\n\n",
                    gcst.STATUS_CODE: 200
                })
                final_pipe.rpush(response_queue, done_data)
                final_pipe.expire(response_queue, 1)
                
                s = time.time()
                await final_pipe.execute()
                e = time.time()
                logger.info(f"time to send final messages: {round(e-s, 4)}")

            logger.info(f" 👀  Queried node: {node.node_id} for task: {task}. Success: {not first_message}.")

        response_time = time.time() - start_time
        query_result = utility_models.QueryResult(
            formatted_response=text_jsons if len(text_jsons) > 0 else None,
            node_id=node.node_id,
            response_time=response_time,
            task=task,
            success=not first_message,
            node_hotkey=node.hotkey,
            status_code=200,
        )
        success = not first_message
    except Exception as e:
        logger.error(f"Unexpected exception when querying node: {node.node_id} for task: {task}. Payload: {payload}. Error: {e}")
        query_result = construct_500_query_result(node, task)
        success = False
    finally:
        if query_result is not None:
            await utils.adjust_contender_from_result(config, query_result, contender, synthetic_query, payload=payload)

    character_count = sum([len(text_json["choices"][0]["delta"]["content"]) for text_json in text_jsons])
    logger.debug(f"Success: {success}; Node: {node.node_id}; Task: {task}; response_time: {response_time}; first_message: {first_message}; character_count: {character_count}")
    logger.info(f"Success: {success}")
    return success

async def query_node_stream(config: Config, contender: Contender, node: Node, payload: dict):
    address = client.construct_server_address(
        node,
        replace_with_docker_localhost=config.replace_with_docker_localhost,
        replace_with_localhost=config.replace_with_localhost,
    )
    task_config = tcfg.get_enabled_task_config(contender.task)
    if task_config is None:
        logger.error(f"Task config not found for task: {contender.task}")
        return

    assert node.fernet is not None
    assert node.symmetric_key_uuid is not None

    return client.make_streamed_post(
        httpx_client=httpx.AsyncClient(),
        server_address=address,
        keypair=config.keypair,
        validator_ss58_address=config.ss58_address,
        miner_ss58_address=node.hotkey,
        fernet=node.fernet,
        symmetric_key_uuid=node.symmetric_key_uuid,
        payload=payload,
        endpoint=task_config.endpoint,
        timeout=task_config.timeout,
    )