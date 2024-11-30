from dataclasses import dataclass
from functools import lru_cache


from dotenv import load_dotenv
import os
from typing import TypeVar
from pydantic import BaseModel

load_dotenv()


T = TypeVar("T", bound=BaseModel)


@dataclass
class WorkerConfig:
    LLAMA_3_2_3B_TEXT_WORKER_URL: str | None
    LLAMA_3_1_8B_TEXT_WORKER_URL: str | None
    LLAMA_3_1_70B_TEXT_WORKER_URL: str | None
    IMAGE_WORKER_URL: str | None


@lru_cache
def factory_worker_config() -> WorkerConfig:
    return WorkerConfig(
        LLAMA_3_2_3B_TEXT_WORKER_URL=os.getenv("LLAMA_3_2_3B_TEXT_WORKER_URL"),
        LLAMA_3_1_8B_TEXT_WORKER_URL=os.getenv("LLAMA_3_1_8B_TEXT_WORKER_URL"),
        LLAMA_3_1_70B_TEXT_WORKER_URL=os.getenv("LLAMA_3_1_70B_TEXT_WORKER_URL"),
        IMAGE_WORKER_URL=os.getenv("IMAGE_WORKER_URL"),
    )
