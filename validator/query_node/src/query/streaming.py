import json
import time
from typing import AsyncGenerator, Optional

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

async def async_chain(first_chunk, async_gen):
    yield first_chunk
    async for item in async_gen:
        yield item

def construct_500_query_result(node: Node, task: str) -> utility_models.QueryResult:
    return utility_models.QueryResult(
        node_id=node.node_id,
        task=task,
        success=False,
        node_hotkey=node.hotkey,
        formatted_response=None,
        status_code=500,
        response_time=None,
    )

async def _handle_synthetic_event(
    config: Config,
    content: str,
    job_id: str,
    status_code: int = 200,
    ttl: int = rcst.RESPONSE_QUEUE_TTL
) -> None:
    response_queue = await rutils.get_response_queue_key(job_id)
    event_data = json.dumps({
        gcst.CONTENT: content,
        gcst.STATUS_CODE: status_code
    })
    
    async with config.redis_db.pipeline() as pipe:
        await pipe.rpush(response_queue, event_data)
        await pipe.expire(response_queue, ttl)
        await pipe.execute()

async def consume_synthetic_generator(
    config: Config,
    generator: AsyncGenerator,
    job_id: str,
    contender: Contender,
    node: Node,
    payload: dict,
    start_time: float,
) -> bool:
    task = contender.task
    query_result = None

    try:
        first_chunk = await generator.__anext__()
    except (StopAsyncIteration, httpx.ConnectError, httpx.ReadError, httpx.HTTPError, httpx.ReadTimeout, Exception) as e:
        error_type = type(e).__name__
        logger.error(f"Error when querying node: {node.node_id} for task: {task}. Error: {error_type} - {str(e)}")
        query_result = construct_500_query_result(node, task)
        await utils.adjust_contender_from_result(config, query_result, contender, True, payload=payload)
        return False

    text_jsons, status_code, first_message = [], 200, True
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

                    for text_json in loaded_jsons:
                        if not isinstance(text_json, dict):
                            first_message = True
                            break

                        try:
                            _ = text_json["choices"][0]["delta"]["content"]
                        except KeyError:
                            first_message = True
                            break

                        text_jsons.append(text_json)
                        dumped_payload = json.dumps(text_json)
                        first_message = False
                        
                        await _handle_synthetic_event(
                            config=config,
                            content=f"data: {dumped_payload}\n\n",
                            job_id=job_id,
                        )

                except (IndexError, json.JSONDecodeError) as e:
                    logger.warning(f"Error {e} when trying to load text: {text}")
                    break

        if len(text_jsons) > 0:
            last_payload = _get_formatted_payload("", False, add_finish_reason=True)
            await _handle_synthetic_event(
                config=config,
                content=f"data: {last_payload}\n\n",
                job_id=job_id,
            )
            await _handle_synthetic_event(
                config=config,
                content="data: [DONE]\n\n",
                job_id=job_id,
                ttl=1
            )
            logger.info(f"Queried node: {node.node_id} for task: {task}. Success: {not first_message}.")

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
        logger.error(f"Unexpected exception when querying node: {node.node_id} for task: {task}. Error: {e}")
        query_result = construct_500_query_result(node, task)
        success = False

    finally:
        if query_result is not None:
            await utils.adjust_contender_from_result(config, query_result, contender, True, payload=payload)

        if success:
            character_count = sum([len(text_json["choices"][0]["delta"]["content"]) for text_json in text_jsons])
            logger.debug(f"Success: {success}; Node: {node.node_id}; Task: {task}; response_time: {response_time}; character_count: {character_count}")
    
    return success

async def consume_organic_generator(
    config: Config,
    generator: AsyncGenerator,
    job_id: str,
    contender: Contender,
    node: Node,
    payload: dict,
    start_time: float,
) -> AsyncGenerator[str, None]:
    task = contender.task
    query_result = None

    try:
        first_chunk = await generator.__anext__()
    except (StopAsyncIteration, httpx.ConnectError, httpx.ReadError, httpx.HTTPError, httpx.ReadTimeout, Exception) as e:
        error_type = type(e).__name__
        logger.error(f"Error when querying node: {node.node_id} for task: {task}. Error: {error_type} - {str(e)}")
        query_result = construct_500_query_result(node, task)
        await utils.adjust_contender_from_result(config, query_result, contender, False, payload=payload)
        return

    text_jsons, first_message = [], True
    try:
        async for text in async_chain(first_chunk, generator):
            if isinstance(text, bytes):
                text = text.decode()
            if isinstance(text, str):
                try:
                    loaded_jsons = load_sse_jsons(text)
                    if isinstance(loaded_jsons, dict):
                        break

                    for text_json in loaded_jsons:
                        if not isinstance(text_json, dict):
                            first_message = True
                            break

                        try:
                            _ = text_json["choices"][0]["delta"]["content"]
                        except KeyError:
                            first_message = True
                            break

                        text_jsons.append(text_json)
                        dumped_payload = json.dumps(text_json)
                        first_message = False
                        yield f"data: {dumped_payload}\n\n"

                except (IndexError, json.JSONDecodeError) as e:
                    logger.warning(f"Error {e} when trying to load text: {text}")
                    break

        if len(text_jsons) > 0:
            last_payload = _get_formatted_payload("", False, add_finish_reason=True)
            yield f"data: {last_payload}\n\n"
            yield "data: [DONE]\n\n"
            logger.info(f"Queried node: {node.node_id} for task: {task}. Success: {not first_message}.")

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

    except Exception as e:
        logger.error(f"Unexpected exception when querying node: {node.node_id} for task: {task}. Error: {e}")
        query_result = construct_500_query_result(node, task)

    finally:
        if query_result is not None:
            await utils.adjust_contender_from_result(config, query_result, contender, False, payload=payload)

async def query_node_stream(config: Config, contender: Contender, node: Node, payload: dict) -> Optional[AsyncGenerator]:
    address = client.construct_server_address(
        node,
        replace_with_docker_localhost=config.replace_with_docker_localhost,
        replace_with_localhost=config.replace_with_localhost,
    )
    task_config = tcfg.get_enabled_task_config(contender.task)
    if task_config is None:
        logger.error(f"Task config not found for task: {contender.task}")
        return None

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