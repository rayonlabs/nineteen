import json
import time
from typing import AsyncGenerator

import httpx
from core.models import utility_models
from validator.query_node.src.query_config import Config
from validator.query_node.src import utils

from validator.models import Contender
from fiber.validator import client
from fiber.networking.models import NodeWithFernet as Node
from core import task_config as tcfg
from validator.utils.generic import generic_constants as gcst, generic_utils
from validator.utils.redis import redis_constants as rcst

from fiber.logging_utils import get_logger

from validator.utils.query.query_utils import load_sse_jsons

logger = get_logger(__name__)


async def _handle_event(
    config: Config,
    content: str | None,
    synthetic_query: bool,
    job_id: str,
    status_code: int,
    error_message: str | None = None,
) -> None:
    if synthetic_query:
        return
    if content is not None:
        if isinstance(content, dict):
            content = json.dumps(content)
        await config.redis_db.publish(
            f"{rcst.JOB_RESULTS}:{job_id}",
            generic_utils.get_success_event(content=content, job_id=job_id, status_code=status_code),
        )
    else:
        await config.redis_db.publish(
            f"{rcst.JOB_RESULTS}:{job_id}",
            generic_utils.get_error_event(job_id=job_id, error_message=error_message, status_code=status_code),
        )


async def async_chain(first_chunk, async_gen):
    yield first_chunk  # manually yield the first chunk
    async for item in async_gen:
        yield item  # then yield from the original generator


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

    try:
        first_chunk = await generator.__anext__()
    except (StopAsyncIteration, httpx.ConnectError, httpx.ReadError, httpx.HTTPError, httpx.ReadTimeout, Exception) as e:
        error_type = type(e).__name__
        error_details = str(e)

        logger.error(f"Error when querying node: {node.node_id} for task: {task}. Error: {error_type} - {error_details}")
        query_result = construct_500_query_result(node, task)
        await utils.adjust_contender_from_result(config, query_result, contender, synthetic_query, payload=payload)
        return False

    text_jsons, status_code = [], 200
    # flag to check if we have _at least_ a valid first chunk
    is_first_chunk = True
    # on first chunk we want to set "role" = "assistant"
    add_role = True
    # need to check that at least one chunk has "finish_reason": "stop", if not, add it
    chunk_with_finish_reason = False

    try:
        async for text in async_chain(first_chunk, generator):
            if isinstance(text, bytes):
                text = text.decode()
            if isinstance(text, str):
                try:
                    loaded_jsons = load_sse_jsons(text)
                    if isinstance(loaded_jsons, dict):
                        status_code = loaded_jsons.get(gcst.STATUS_CODE)  # noqa
                        break

                except (IndexError, json.JSONDecodeError) as e:
                    logger.warning(f"Error {e} when trying to load text: {text}")
                    break

                for text_json in loaded_jsons:
                    if not isinstance(text_json, dict):
                        logger.debug(f"Invalid text_json because its not a dict?: {text_json}")
                        is_first_chunk = True  # NOTE: Janky, but so we mark it as a fail
                        break
                    try:
                        _ = text_json["choices"][0]["delta"]["content"]
                        if add_role:
                            text_json["choices"][0]["delta"]["role"] = "assistant"
                            add_role = False
                        if text_json["choices"][0]["finish_reason"] == "stop":
                            chunk_with_finish_reason = True
                    except KeyError:
                        logger.debug(f"Invalid text_json because there's not delta content: {text_json}")
                        is_first_chunk = True  # NOTE: Janky, but so we mark it as a fail
                        break

                    text_jsons.append(text_json)
                    dumped_payload = json.dumps(text_json)

                    # we have at least one valid first chunk, so consider this run a "success"
                    is_first_chunk = False

                    await _handle_event(
                        config,
                        content=f"data: {dumped_payload}\n\n",
                        synthetic_query=synthetic_query,
                        job_id=job_id,
                        status_code=200,
                    )

        if len(text_jsons) > 0:
            if not chunk_with_finish_reason:
                last_payload = {
                    "choices": [
                        {"delta": {"content": ""}, "finish_reason": "stop"}
                    ]
                }
                await _handle_event(
                    config,
                    content=f"data: {json.dumps(last_payload)}\n\n",
                    synthetic_query=synthetic_query,
                    job_id=job_id,
                    status_code=200
                )
            await _handle_event(
                config, content="data: [DONE]\n\n", synthetic_query=synthetic_query, job_id=job_id, status_code=200
            )
            logger.info(f" 👀  Queried node: {node.node_id} for task: {task}. Success: {not first_message}.")

        response_time = time.time() - start_time
        query_result = utility_models.QueryResult(
            formatted_response=text_jsons if len(text_jsons) > 0 else None,
            node_id=node.node_id,
            response_time=response_time,
            task=task,
            success=not is_first_chunk,
            node_hotkey=node.hotkey,
            status_code=200,
        )
        success = not is_first_chunk
    except Exception as e:
        logger.error(f"Unexpected exception when querying node: {node.node_id} for task: {task}. Payload: {payload}. Error: {e}")
        query_result = construct_500_query_result(node, task)
        success = False
    finally:
        if query_result is not None:
            await utils.adjust_contender_from_result(config, query_result, contender, synthetic_query, payload=payload)
            await config.redis_db.expire(rcst.QUERY_RESULTS_KEY + ":" + job_id, 10)

    character_count = sum([len(text_json["choices"][0]["delta"]["content"]) for text_json in text_jsons])
    logger.debug(f"Success: {success}; Node: {node.node_id}; Task: {task}; response_time: {response_time}; first_message: {is_first_chunk}; character_count: {character_count}")
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
        httpx_client=config.httpx_client,
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
