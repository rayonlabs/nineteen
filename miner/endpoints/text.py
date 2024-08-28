from functools import partial
from fastapi import Depends

from fastapi.responses import StreamingResponse
from fiber.miner.security.encryption import decrypt_general_payload
from core.models import payload_models
from fastapi.routing import APIRouter
from fiber.logging_utils import get_logger

from miner.logic.chat import chat_stream
from fiber.miner.core.configuration import Config
from fiber.miner.dependencies import get_config

logger = get_logger(__name__)


async def chat_completions(
    decrypted_payload: payload_models.ChatPayload = Depends(
        partial(decrypt_general_payload, payload_models.ChatPayload)
    ),
    config: Config = Depends(get_config),
) -> StreamingResponse:
    decrypted_payload.model = "NousResearch/Hermes-3-Llama-3.1-8B"
    generator = chat_stream(config.httpx_client, decrypted_payload)

    return StreamingResponse(generator, media_type="text/event-stream")


def factory_router() -> APIRouter:
    router = APIRouter()
    router.add_api_route("/chat/completions", chat_completions, tags=["Subnet"], methods=["POST"])
    return router