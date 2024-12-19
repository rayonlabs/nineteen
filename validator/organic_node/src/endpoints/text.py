from typing import Any, AsyncGenerator
from fastapi import Depends, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from opentelemetry import metrics
from fiber.logging_utils import get_logger
from fastapi.routing import APIRouter
import time
import json

from validator.organic_node.src.core.configuration import Config
from validator.organic_node.src.core.dependencies import get_config
from validator.organic_node.src.core.middleware import verify_api_key_rate_limit
from validator.organic_node.src.models import request_models
from validator.utils.query.query_utils import load_sse_jsons
from validator.db.src.sql.contenders import get_contenders_for_task, update_total_requests_made
from validator.db.src.sql.nodes import get_node
from validator.utils.generic import generic_constants as gcst
from validator.common.query import streaming
from validator.common import utils as cutils
from validator.organic_node.src import utils  as orutils
from core.models import utility_models


logger = get_logger(__name__)


# Metrics
COUNTER_TEXT_GENERATION_ERROR = metrics.get_meter(__name__).create_counter("validator.organic_node.text.error")
COUNTER_TEXT_GENERATION_SUCCESS = metrics.get_meter(__name__).create_counter("validator.organic_node.text.success")
GAUGE_TOKENS_PER_SEC = metrics.get_meter(__name__).create_gauge(
    "validator.organic_node.text.tokens_per_sec",
    description="Average tokens per second metric for LLM streaming for an organic LLM query"
)
GAUGE_TOKENS = metrics.get_meter(__name__).create_gauge(
    "validator.organic_node.text.tokens",
    description="Total tokens for LLM streaming for an organic LLM query"
)

async def _process_stream_query(
    config: Config,
    payload: dict[str, Any],
    task: str,
) -> AsyncGenerator[str, None]:
    contenders = await get_contenders_for_task(config.psql_db, task, 5, gcst.ORGANIC)
    if not contenders:
        COUNTER_TEXT_GENERATION_ERROR.add(1, {"task": task, "kind": "no_contenders", "status_code": 500})
        raise HTTPException(status_code=500, detail="No available nodes to process request")

    start_time = time.time()
    num_tokens = 0
    query_result = None

    for contender in contenders[:5]:
        node = await get_node(config.psql_db, contender.node_id, config.netuid)
        if not node:
            logger.error(f"Node {contender.node_id} not found")
            continue

        await update_total_requests_made(config.psql_db, contender)

        generator = await streaming.query_node_stream(
            config=config,
            contender=contender,
            payload=payload,
            node=node
        )

        if not generator:
            continue

        stream_time_init = None
        try:
            text_jsons = []
            async for chunk in generator:
                if stream_time_init is None:
                        stream_time_init = time.time()

                if isinstance(chunk, bytes):
                    chunk = chunk.decode()

                chunks = load_sse_jsons(chunk)
                if not isinstance(chunks, list):
                    continue

                for chunk_data in chunks:
                    try:
                        num_tokens += 1
                        yield f"data: {json.dumps(chunk_data)}\n\n"
                    except Exception as e:
                        logger.error(f"Error processing chunk: {e}")
                        continue

                    text_jsons.append(chunk_data)

            yield "data: [DONE]\n\n"

            completion_time = time.time() - start_time
            tps = num_tokens / completion_time
            if stream_time_init:
                stream_time = time.time() - stream_time_init
            else:
                stream_time = completion_time

            GAUGE_TOKENS.set(num_tokens, {"task": task})
            GAUGE_TOKENS_PER_SEC.set(tps, {"task": task})
            COUNTER_TEXT_GENERATION_SUCCESS.add(1, {"task": task, "status_code": 200})

            logger.info(f"Tokens per second for task {task}: {tps}")
            query_result = utility_models.QueryResult(
                formatted_response=text_jsons if len(text_jsons) > 0 else None,
                node_id=node.node_id,
                response_time=completion_time,
                stream_time=stream_time,
                task=task,
                success=True,
                node_hotkey=node.hotkey,
                status_code=200,
            )

        except Exception as e:
            logger.error(f"Error streaming from node {node.node_id}: {e}")
            query_result = streaming.construct_500_query_result(node, task)
            continue

        finally:
            if query_result is not None:
                await cutils.adjust_contender_from_result(config, query_result, contender, False, payload=payload)
            return

    COUNTER_TEXT_GENERATION_ERROR.add(1, {"task": task, "kind": "all_contenders_failed", "status_code": 500})
    raise HTTPException(status_code=500, detail="No available nodes could process the request")


async def _handle_nonstream_response(generator: AsyncGenerator[str, None]) -> JSONResponse:
    all_content = ""
    first_chunk = True
    role = "assistant"

    async for chunk in generator:
        chunks = load_sse_jsons(chunk)
        if not isinstance(chunks, list):
            continue

        for chunk_data in chunks:
            if not isinstance(chunk_data, dict) or "choices" not in chunk_data:
                continue

            choice = chunk_data["choices"][0]

            if first_chunk:
                if "delta" in choice:
                    if "role" in choice["delta"]:
                        role = choice["delta"]["role"]
                elif "role" in choice:
                    role = choice["role"]
                first_chunk = False

            content = ""
            if "delta" in choice and "content" in choice["delta"]:
                content = choice["delta"]["content"]
            elif "text" in choice:
                content = choice["text"]
            elif "content" in choice:
                content = choice["content"]

            if content is not None:
                all_content += content

    # format response based on request type
    if role == "assistant":
        return JSONResponse({
            "choices": [{
                "message": {
                    "content": all_content,
                    "role": role
                }
            }]
        })
    else:
        # completion-style responses
        return JSONResponse({
            "choices": [{
                "text": all_content
            }]
        })

async def chat(
    chat_request: request_models.ChatRequest,
    config: Config = Depends(get_config),
) -> StreamingResponse | JSONResponse:

    payload = orutils.chat_to_payload(chat_request)
    payload.temperature = 0.5

    await cutils._decrement_requests_remaining(config.redis_db, payload.model)

    try:
        generator = _process_stream_query(
            config=config,
            payload=payload.model_dump(),
            task=payload.model
        )

        if chat_request.stream:
            return StreamingResponse(generator, media_type="text/event-stream")
        else:
            return await _handle_nonstream_response(generator)

    except HTTPException as http_exc:
        COUNTER_TEXT_GENERATION_ERROR.add(1, {
            "task": payload.model,
            "kind": type(http_exc).__name__,
            "status_code": http_exc.status_code
        })
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error in chat endpoint: {str(e)}")
        logger.exception(e)
        COUNTER_TEXT_GENERATION_ERROR.add(1, {
            "task": payload.model,
            "kind": type(e).__name__,
            "status_code": 500
        })
        raise HTTPException(status_code=500, detail="An unexpected error occurred")

async def chat_comp(
    chat_request: request_models.CompletionRequest,
    config: Config = Depends(get_config),
) -> StreamingResponse | JSONResponse:

    payload = orutils.chat_comp_to_payload(chat_request)
    payload.temperature = 0.5
    await cutils._decrement_requests_remaining(config.redis_db, payload.model)

    try:
        generator = _process_stream_query(
            config=config,
            payload=payload.model_dump(),
            task=payload.model
        )

        if chat_request.stream:
            return StreamingResponse(generator, media_type="text/event-stream")
        else:
            return await _handle_nonstream_response(generator)

    except HTTPException as http_exc:
        COUNTER_TEXT_GENERATION_ERROR.add(1, {
            "task": payload.model,
            "kind": type(http_exc).__name__,
            "status_code": http_exc.status_code
        })
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error in completion endpoint: {str(e)}")
        COUNTER_TEXT_GENERATION_ERROR.add(1, {
            "task": payload.model,
            "kind": type(e).__name__,
            "status_code": 500
        })
        raise HTTPException(status_code=500, detail="An unexpected error occurred")

router = APIRouter()
router.add_api_route(
    "/v1/chat/completions",
    chat,
    methods=["POST", "OPTIONS"],
    tags=["Text"],
    response_model=None,
    dependencies=[Depends(verify_api_key_rate_limit)],
)
router.add_api_route(
    "/v1/completions",
    chat_comp,
    methods=["POST", "OPTIONS"],
    tags=["Text"],
    response_model=None,
    dependencies=[Depends(verify_api_key_rate_limit)],
)
