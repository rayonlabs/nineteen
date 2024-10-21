import json
import httpx
from fiber.logging_utils import get_logger

from core.models import payload_models
from core import task_config as tcfg
from miner.config import WorkerConfig

logger = get_logger(__name__)


async def chat_stream(
    httpx_client: httpx.AsyncClient, decrypted_payload: payload_models.ChatPayload, worker_config: WorkerConfig
):
    task_config = tcfg.get_enabled_task_config(decrypted_payload.model)
    if task_config is None:
        raise ValueError(f"Task config not found for model: {decrypted_payload.model}")
    assert task_config.orchestrator_server_config.load_model_config is not None

    model_name = task_config.orchestrator_server_config.load_model_config["model"]

    # NOTE: you will probably need a smarter way to do this
    if task_config.task == "chat-llama-3-1-8b":
        address = worker_config.LLAMA_3_1_8B_TEXT_WORKER_URL
    elif task_config.task == "chat-llama-3-1-70b":
        address = worker_config.LLAMA_3_1_70B_TEXT_WORKER_URL
    elif task_config.task == "chat-llama-3-2-3b":
        address = worker_config.LLAMA_3_2_3B_TEXT_WORKER_URL
    # NOTE: adjust on validator UID basis by adding custom endpoints in worker_config init
    else:
        raise ValueError(f"Invalid model: {decrypted_payload.model}")

    decrypted_payload.model = model_name

    assert address is not None, f"Address for model: {decrypted_payload.model} is not set in your miner config!"

    if True:
        # NOTE: review timeout?
        async with httpx_client.stream("POST", address, json=decrypted_payload.model_dump(), timeout=5) as resp:
            try:
                resp.raise_for_status()
            except httpx.HTTPStatusError as e:
                await resp.aread()
                logger.error(f"HTTP Error {e.response.status_code}: {e.response.text}")
                raise
            async for chunk in resp.aiter_lines():
                received_event_chunks = chunk.split("\n\n")
                for event in received_event_chunks:
                    if event == "":
                        continue
                    prefix, _, data = event.partition(":")
                    if data.strip() == "[DONE]":
                        break
                    # This is quite ineffecient but needed
                    # To work with base vllm image
                    # I would recommended optimising this in some way
                    # print(data)
                    data2 = json.loads(data)
                    if (
                        data2["choices"][0]["logprobs"] is None
                        or data2["choices"][0]["logprobs"]["content"][0]["logprob"] is None
                    ):
                        continue

                    yield f"data: {data}\n\n"

    else:
        for i in range(100):
            data = {"choices": [{"delta": {"content": f"{i}"}, "logprobs": {"content": [{"logprob": 0.0}]}}]}
            yield f"data: {json.dumps(data)}\n\n"
        yield "data: [DONE]\n\n"
