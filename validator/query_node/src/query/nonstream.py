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
import traceback


logger = get_logger(__name__)

def _get_500_query_result(node_id: int, contender: Contender) -> utility_models.QueryResult:
    return utility_models.QueryResult(
        formatted_response=None,
        node_id=node_id,
        node_hotkey=contender.node_hotkey,
        response_time=None,
        task=contender.task,
        status_code=500,
        success=False,
    )

def get_formatted_response(
    response: Response,
    response_model: type[ImageResponse],
) -> ImageResponse | None:
    if response and response.status_code == 200:
        return _extract_response(response, response_model)
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
        logger.error(f"Failed to deserialize response: {e}")
        return None

async def query_nonstream_img(
    config: Config,
    contender: Contender,
    node: Node,
    payload: dict,
    response_model: type[ImageResponse],
    synthetic_query: bool,
) -> ImageResponse | None:
    node_id = contender.node_id

    assert node.fernet is not None
    assert node.symmetric_key_uuid is not None
    task_config = tcfg.get_enabled_task_config(contender.task)
    time_before_query = time.time()
    
    if task_config is None:
        logger.error(f"Task config not found for task: {contender.task}")
        return None

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
        logger.error(f"Error when querying node: {node.node_id} for task: {contender.task}. Error: {e} - \n{traceback.format_exc()}")
        query_result = _get_500_query_result(node_id=node_id, contender=contender)
        await utils.adjust_contender_from_result(
            config=config, query_result=query_result, contender=contender, synthetic_query=synthetic_query, payload=payload
        )
        return None

    response_time = time.time() - time_before_query
    try:
        formatted_response = get_formatted_response(response, response_model)
    except Exception as e:
        logger.error(f"Error when deserializing response for task: {contender.task}. Error: {e} - \n{traceback.format_exc()}")
        query_result = _get_500_query_result(node_id=node_id, contender=contender)
        await utils.adjust_contender_from_result(
            config=config, query_result=query_result, contender=contender, synthetic_query=synthetic_query, payload=payload
        )
        return None

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
        
        await utils.adjust_contender_from_result(
            config=config, query_result=query_result, contender=contender, synthetic_query=synthetic_query, payload=payload
        )

        return formatted_response
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
        return None