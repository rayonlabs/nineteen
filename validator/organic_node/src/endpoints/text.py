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
from validator.db.src.sql.contenders import get_contenders_for_task
from validator.db.src.sql.nodes import get_node
from validator.utils.generic import generic_constants as gcst
from validator.common.query import streaming
from validator.organic_node.src import utils


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

    for contender in contenders[:5]:
        node = await get_node(config.psql_db, contender.node_id, config.netuid)
        if not node:
            logger.error(f"Node {contender.node_id} not found")
            continue

        generator = await streaming.query_node_stream(
            config=config,
            contender=contender,
            payload=payload,
            node=node
        )

        if not generator:
            continue

        try:
            async for chunk in generator:
                if isinstance(chunk, bytes):
                    chunk = chunk.decode()

                chunks = load_sse_jsons(chunk)
                if not isinstance(chunks, list):
                    continue

                for chunk_data in chunks:
                    try:
                        num_tokens += 1
                        yield json.dumps(chunk_data) + "\n"
                    except Exception as e:
                        logger.error(f"Error processing chunk: {e}")
                        continue

            completion_time = time.time() - start_time
            tps = num_tokens / completion_time

            GAUGE_TOKENS.set(num_tokens, {"task": task})
            GAUGE_TOKENS_PER_SEC.set(tps, {"task": task})
            COUNTER_TEXT_GENERATION_SUCCESS.add(1, {"task": task, "status_code": 200})

            logger.info(f"Tokens per second for task {task}: {tps}")
            return

        except Exception as e:
            logger.error(f"Error streaming from node {node.node_id}: {e}")
            continue

    # If we get here, all contenders failed
    COUNTER_TEXT_GENERATION_ERROR.add(1, {"task": task, "kind": "all_contenders_failed", "status_code": 500})
    raise HTTPException(status_code=500, detail="No available nodes could process the request")

async def _handle_nonstream_response(generator: AsyncGenerator[str, None]) -> JSONResponse:
    all_content = ""
    async for chunk in generator:
        chunks = load_sse_jsons(chunk)
        if isinstance(chunks, list):
            for chunk_data in chunks:
                if "choices" in chunk_data:
                    if "text" in chunk_data["choices"][0]:  # Completion
                        content = chunk_data["choices"][0]["text"]
                    else:  # Chat
                        content = chunk_data["choices"][0]["delta"].get("content", "")
                    all_content += content
                    if content == "":
                        break

    return JSONResponse({
        "choices": [{
            "message": {"content": all_content, "role": "assistant"}
        }]
    })

async def chat(
    chat_request: request_models.ChatRequest,
    config: Config = Depends(get_config),
) -> StreamingResponse | JSONResponse:

    payload = utils.chat_to_payload(chat_request)
    payload.temperature = 0.5

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

    payload = utils.chat_comp_to_payload(chat_request)
    payload.temperature = 0.5

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
