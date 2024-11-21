import json
from typing import Any
from fastapi import HTTPException
from core.models import utility_models
from core import task_config as tcfg
from aiocache import cached


@cached(ttl=None)
async def get_max_model_len(task: str) -> int:
    task_config = tcfg.get_enabled_task_config(task)
    if task_config:
        return task_config.orchestrator_server_config.load_model_config['max_model_len']
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Task {task} is not supported"
        )

async def check_prompt_length(messages: list[utility_models.Message],
                              n_output_tokens : int,
                              task: str,
                              char_to_token: float = 4) -> bool:
    max_len = await get_max_model_len(task)
    return sum([len(message.content) for message in messages])/char_to_token + n_output_tokens < max_len


def load_sse_jsons(chunk: str) -> list[dict[str, Any]] | dict[str, str]:
    try:
        jsons = []
        received_event_chunks = chunk.split("\n\n")
        for event in received_event_chunks:
            if event == "":
                continue
            prefix, _, data = event.partition(":")
            if data.strip() == "[DONE]":
                break
            loaded_chunk = json.loads(data)
            jsons.append(loaded_chunk)
        return jsons
    except json.JSONDecodeError:
        ...

    return []
