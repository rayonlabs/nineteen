
import json
from typing import Any




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