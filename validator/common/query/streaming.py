import json
import time
import traceback
from typing import AsyncGenerator
from fiber.encrypted.validator import client
from fiber.encrypted.networking.models import NodeWithFernet as Node
from fiber.logging_utils import get_logger

import httpx
from opentelemetry import metrics

from core.models import utility_models
from validator.common.query_config import Config
from validator.common import utils
from validator.models import Contender
from core import task_config as tcfg
from validator.utils.generic import generic_constants as gcst, generic_utils
from validator.utils.redis import redis_constants as rcst
from validator.utils.query.query_utils import load_sse_jsons


logger = get_logger(__name__)

GAUGE_ORGANIC_TOKENS_PER_SEC = metrics.get_meter(__name__).create_gauge(
    "validator.synthetic_node.query.streaming.organic.tokens_per_sec",
    description="Average tokens per second metric for LLM streaming for any organic query"
)
GAUGE_ORGANIC_TOKENS = metrics.get_meter(__name__).create_gauge(
    "validator.synthetic_node.query.streaming.organic.tokens",
    description="Total tokens for LLM streaming for an organic LLM query"
)
GAUGE_SYNTHETIC_TOKENS_PER_SEC = metrics.get_meter(__name__).create_gauge(
    "validator.synthetic_node.query.streaming.synthetic.tokens_per_sec",
    description="Average tokens per second metric for LLM streaming for any synthetic query"
)
GAUGE_SYNTHETIC_TOKENS = metrics.get_meter(__name__).create_gauge(
    "validator.synthetic_node.query.streaming.synthetic.tokens",
    description="Total tokens for LLM streaming for a synthetic LLM query"
)

def _get_formatted_payload(content: str, first_message: bool, add_finish_reason: bool = False, task: str = "") -> str:
    if 'comp' in task:
        choices_payload: dict[str, str | dict[str, str]] = {"text": content}
        choices_payload["finish_reason"] = "stop"
    else:
        delta_payload = {"content": content}
        if first_message:
            delta_payload["role"] = "assistant"
        choices_payload: dict[str, str | dict[str, str]] = {"delta": delta_payload}
        if add_finish_reason:
            choices_payload["finish_reason"] = "stop"
    payload = {
        "choices": [choices_payload],
    }
    dumped_payload = json.dumps(payload)
    return dumped_payload


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
        stream_time=None
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
    debug: bool = False,  # TODO: remove unused variable
) -> bool:
    assert job_id
    task = contender.task
    query_result = None
    tokens = 0

    try:
        first_chunk = await generator.__anext__()  # TODO: use `anext(generator)`

    except (StopAsyncIteration, httpx.ConnectError, httpx.ReadError, httpx.HTTPError, httpx.ReadTimeout, Exception) as e:
        logger.error(f"Error when querying node: {node.node_id} for task: {task}.")

        # drop the stacktrace while we're here (otel doesn't like logger.exception)
        logger.error("\n".join(traceback.format_exception(e)))

        query_result = construct_500_query_result(node, task)
        await utils.adjust_contender_from_result(config, query_result, contender, synthetic_query, payload=payload)

        return False

    text_jsons, status_code, first_message = [], 200, True  # TODO: remove unused variable

    stream_time_init = None
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
                        first_message = True  # NOTE: Janky, but so we mark it as a fail
                        break

                    try:
                        if "comp" in task:
                            _ = text_json["choices"][0]["text"]
                        else:
                            _ = text_json["choices"][0]["delta"]["content"]
                    except KeyError:
                        logger.debug(f"Invalid text_json because there's not delta content: {text_json}")
                        first_message = True  # NOTE: Janky, but so we mark it as a fail
                        break

                    text_jsons.append(text_json)
                    dumped_payload = json.dumps(text_json)
                    first_message = False

                    await _handle_event(
                        config,
                        content=f"data: {dumped_payload}\n\n",
                        synthetic_query=synthetic_query,
                        job_id=job_id,
                        status_code=200,
                    )

                    if stream_time_init is None:
                        stream_time_init = time.time()

                    tokens += 1

        if len(text_jsons) > 0:
            last_payload = _get_formatted_payload("", False, add_finish_reason=True, task = task)
            await _handle_event(
                config, content=f"data: {last_payload}\n\n", synthetic_query=synthetic_query, job_id=job_id, status_code=200
            )
            await _handle_event(
                config, content="data: [DONE]\n\n", synthetic_query=synthetic_query, job_id=job_id, status_code=200
            )
            logger.info(f" 👀  Queried node: {node.node_id} for task: {task}. Success: {not first_message}.")

        response_time = time.time() - start_time

        if stream_time_init is not None:
            stream_time = time.time() - stream_time_init
        else:
            logger.info("Error in calculating stream_time; setting stream_time as response_time")
            stream_time = response_time

        query_result = utility_models.QueryResult(
            formatted_response=text_jsons if len(text_jsons) > 0 else None,
            node_id=node.node_id,
            response_time=response_time,
            stream_time=stream_time,
            task=task,
            success=not first_message,
            node_hotkey=node.hotkey,
            status_code=200,
        )
        success = not first_message
        if success:
            if synthetic_query:
                GAUGE_SYNTHETIC_TOKENS.set(tokens, {"task": task})
                GAUGE_SYNTHETIC_TOKENS_PER_SEC.set(tokens / response_time, {"task": task})
            else:
                GAUGE_ORGANIC_TOKENS.set(tokens, {"task": task})
                GAUGE_ORGANIC_TOKENS_PER_SEC.set(tokens / response_time, {"task": task})
    except Exception as e:
        logger.error(f"Unexpected exception when querying node: {node.node_id} for task: {task}. Payload: {payload}. Error: {e}")
        query_result = construct_500_query_result(node, task)
        success = False
    finally:
        if query_result is not None:
            await utils.adjust_contender_from_result(config, query_result, contender, synthetic_query, payload=payload)
            await config.redis_db.expire(rcst.QUERY_RESULTS_KEY + ":" + job_id, 10)

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
