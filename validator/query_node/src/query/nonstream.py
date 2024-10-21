import json
import time
from httpx import Response
from pydantic import ValidationError
from core.models import utility_models
from core.models.payload_models import ImageResponse
from validator.query_node.src.query_config import Config

from validator.models import Contender
from fiber.networking.models import NodeWithFernet as Node
from fiber.validator import client
from core import task_config as tcfg
from fiber.logging_utils import get_logger
from validator.query_node.src import utils
from validator.utils.redis import redis_constants as rcst
from validator.utils.generic import generic_utils

logger = get_logger(__name__)


def _get_500_query_result(node_id: int, contender: Contender) -> utility_models.QueryResult:
    query_result = utility_models.QueryResult(
        formatted_response=None,
        node_id=node_id,
        node_hotkey=contender.node_hotkey,
        response_time=None,
        task=contender.task,
        status_code=500,
        success=False,
    )
    return query_result


def get_formatted_response(
    response: Response,
    response_model: type[ImageResponse],
) -> ImageResponse | None:
    if response and response.status_code == 200:
        formatted_response = _extract_response(response, response_model)
        return formatted_response
    else:
        return None


def _extract_response(response: Response, response_model: type[ImageResponse]) -> ImageResponse | None:
    try:
        formatted_response = response_model(**response.json())

        # If we're expecting a result (i.e. not nsfw), then try to deserialize
        if (hasattr(formatted_response, "is_nsfw") and not formatted_response.is_nsfw) or not hasattr(
            formatted_response, "is_nsfw"
        ):
            if hasattr(formatted_response, "image_b64"):
                if not formatted_response.image_b64:
                    return None

        return formatted_response
    except ValidationError as e:
        logger.debug(f"Failed to deserialize for some reason: {e}")
        return None


async def handle_nonstream_event(
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


async def query_nonstream(
    config: Config,
    contender: Contender,
    node: Node,
    payload: dict,
    response_model: type[ImageResponse],
    synthetic_query: bool,
    job_id: str,
) -> bool:
    node_id = contender.node_id

    assert node.fernet is not None
    assert node.symmetric_key_uuid is not None
    task_config = tcfg.get_enabled_task_config(contender.task)
    time_before_query = time.time()
    if task_config is None:
        logger.error(f"Task config not found for task: {contender.task}")
        return False

    try:
        response = await client.make_non_streamed_post(
            httpx_client=config.httpx_client,
            server_address=client.construct_server_address(
                node,
                replace_with_docker_localhost=config.replace_with_docker_localhost,
                replace_with_localhost=config.replace_with_localhost,
            ),
            validator_ss58_address=config.ss58_address,
            miner_ss58_address=node.hotkey,
            fernet=node.fernet,
            keypair=config.keypair,
            symmetric_key_uuid=node.symmetric_key_uuid,
            endpoint=task_config.endpoint,
            payload=payload,
            timeout=task_config.timeout,
        )
    except Exception as e:
        logger.error(f"Error when querying node: {node.node_id} for task: {contender.task}. Error: {e}")
        query_result = _get_500_query_result(node_id=node_id, contender=contender)
        await utils.adjust_contender_from_result(
            config=config, query_result=query_result, contender=contender, synthetic_query=synthetic_query, payload=payload
        )
        return False

    response_time = time.time() - time_before_query
    try:
        formatted_response = get_formatted_response(response, response_model)
    except Exception as e:
        logger.error(f"Error when deserializing response for task: {contender.task}. Error: {e}")
        query_result = _get_500_query_result(node_id=node_id, contender=contender)
        await utils.adjust_contender_from_result(
            config=config, query_result=query_result, contender=contender, synthetic_query=synthetic_query, payload=payload
        )
        return False
    

    if formatted_response is not None:
        query_result = utility_models.QueryResult(
            formatted_response=formatted_response,
            node_id=node_id,
            node_hotkey=contender.node_hotkey,
            response_time=response_time,
            task=contender.task,
            status_code=response.status_code,
            success=True,
        )

        logger.info(f"✅ Queried node: {node_id} for task: {contender.task} - time: {response_time}")
        await handle_nonstream_event(
            config, formatted_response.model_dump_json(), synthetic_query, job_id, status_code=response.status_code
        )
        await utils.adjust_contender_from_result(
            config=config, query_result=query_result, contender=contender, synthetic_query=synthetic_query, payload=payload
        )
        return True
    else:
        query_result = utility_models.QueryResult(
            formatted_response=None,
            node_id=node_id,
            node_hotkey=contender.node_hotkey,
            response_time=None,
            task=contender.task,
            status_code=response.status_code,
            success=False,
        )
        logger.debug(
            f"❌ queried node: {node_id} for task: {contender.task}. Response: {response.text}, status code: {response.status_code}"
        )
        await utils.adjust_contender_from_result(
            config=config, query_result=query_result, contender=contender, synthetic_query=synthetic_query, payload=payload
        )
        return False
