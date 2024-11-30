from functools import partial
from fastapi import Depends, HTTPException

from fastapi.responses import StreamingResponse
from fiber.miner.security.encryption import decrypt_general_payload
import httpx
from core.models import payload_models
from fastapi.routing import APIRouter
from fiber.logging_utils import get_logger

from miner.logic.chat import chat_stream
from fiber.miner.core.configuration import Config
from fiber.miner.dependencies import blacklist_low_stake, get_config, verify_request
from miner.config import WorkerConfig
from miner.dependencies import get_worker_config
from typing import Union
from validator.utils.generic.generic_utils import async_chain

logger = get_logger(__name__)


async def chat_completions(
    decrypted_payload: payload_models.ChatCompletionPayload = Depends(partial(decrypt_general_payload, payload_models.ChatCompletionPayload)),
    config: Config = Depends(get_config),
    worker_config: WorkerConfig = Depends(get_worker_config),
) -> StreamingResponse:
    try:
        generator = chat_stream(config.httpx_client, decrypted_payload, worker_config)
        first_chunk = await generator.__anext__()  # TODO: use `anext(generator)`
        if first_chunk is None:
            raise HTTPException(status_code=500, detail="Error in streaming text from the server")
        else:
            return StreamingResponse(async_chain(first_chunk, generator), media_type="text/event-stream")
    except httpx.HTTPStatusError as e:
        logger.error(f"Error in streaming text from the server: {e}. ")
        raise HTTPException(status_code=500, detail=f"Error in streaming text from the server: {e}")

async def completions(
    decrypted_payload: payload_models.CompletionPayload = Depends(partial(decrypt_general_payload, payload_models.CompletionPayload)),
    config: Config = Depends(get_config),
    worker_config: WorkerConfig = Depends(get_worker_config),
) -> StreamingResponse:
    try:
        generator = chat_stream(config.httpx_client, decrypted_payload, worker_config)
        first_chunk = await generator.__anext__()  # TODO: use `anext(generator)`
        if first_chunk is None:
            raise HTTPException(status_code=500, detail="Error in streaming text from the server")
        else:
            return StreamingResponse(async_chain(first_chunk, generator), media_type="text/event-stream")
    except httpx.HTTPStatusError as e:
        logger.error(f"Error in streaming text from the server: {e}. ")
        raise HTTPException(status_code=500, detail=f"Error in streaming text from the server: {e}")

# This route takes arbitrary decrypted_payload and decides whether to call /chat/completions or /completions
async def chat(
    decrypted_payload: payload_models.ChatPayload = Depends(partial(decrypt_general_payload, payload_models.ChatPayload)),
    config: Config = Depends(get_config),
    worker_config: WorkerConfig = Depends(get_worker_config),
) -> StreamingResponse:
    if decrypted_payload.messages is None and decrypted_payload.prompt is None or\
        decrypted_payload.messages is not None and decrypted_payload.prompt is not None:
        raise HTTPException(status_code=500, detail="Invalid payload type, strictly either messages or prompt must be provided")
    elif decrypted_payload.messages is not None:
        return await chat_completions(decrypted_payload, config, worker_config)
    elif decrypted_payload.prompt is not None:
        return await completions(decrypted_payload, config, worker_config)

def factory_router() -> APIRouter:
    router = APIRouter()
    router.add_api_route(
        "/chat/completions",
        chat_completions,
        tags=["Subnet"],
        methods=["POST"],
        dependencies=[Depends(blacklist_low_stake), Depends(verify_request)],
    )
    router.add_api_route(
        "/completions",
        completions,
        tags=["Subnet"],
        methods=["POST"],
        dependencies=[Depends(blacklist_low_stake), Depends(verify_request)],
    )
    router.add_api_route(
        "/chat",
        chat,
        tags=["Subnet"],
        methods=["POST"],
        dependencies=[Depends(blacklist_low_stake), Depends(verify_request)],
    )
    return router
