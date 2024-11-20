import random
from typing import Any

from core import task_config as tcfg
from validator.utils.redis import (
    redis_utils as rutils,
    redis_constants as rcst,
)
from validator.utils.synthetic import synthetic_constants as scst
from redis.asyncio import Redis
from core.models import config_models as cmodels

import base64
from io import BytesIO

import aiohttp
import diskcache
from PIL import Image
import uuid
import numpy as np
from fiber.logging_utils import get_logger


logger = get_logger(__name__)


def _get_random_text_prompt() -> str:
    nouns = ["king", "man", "woman", "joker", "queen", "child", "doctor", "teacher", "soldier", "merchant"]  # fmt: off
    locations = [
        "forest",
        "castle",
        "city",
        "village",
        "desert",
        "oceanside",
        "mountain",
        "garden",
        "library",
        "market",
    ]  # fmt: off
    looks = [
        "happy",
        "sad",
        "angry",
        "worried",
        "curious",
        "lost",
        "busy",
        "relaxed",
        "fearful",
        "thoughtful",
    ]  # fmt: off
    actions = [
        "running",
        "walking",
        "reading",
        "talking",
        "sleeping",
        "dancing",
        "working",
        "playing",
        "watching",
        "singing",
    ]  # fmt: off
    times = [
        "in the morning",
        "at noon",
        "in the afternoon",
        "in the evening",
        "at night",
        "at midnight",
        "at dawn",
        "at dusk",
        "during a storm",
        "during a festival",
    ]  # fmt: off

    noun = random.choice(nouns)
    location = random.choice(locations)
    look = random.choice(looks)
    action = random.choice(actions)
    time = random.choice(times)

    text = f"{noun} in a {location}, looking {look}, {action} {time}"
    return text


async def _get_random_picsum_image(x_dim: int, y_dim: int) -> str:
    """
    Generate a random image with the specified dimensions, by calling unsplash api.

    Args:
        x_dim (int): The width of the image.
        y_dim (int): The height of the image.

    Returns:
        str: The base64 encoded representation of the generated image.
    """
    async with aiohttp.ClientSession() as session:
        url = f"https://picsum.photos/{x_dim}/{y_dim}"
        async with session.get(url) as resp:
            data = await resp.read()

    img = Image.open(BytesIO(data))
    buffered = BytesIO()
    img.save(buffered, format="JPEG")
    img_b64 = base64.b64encode(buffered.getvalue()).decode()

    return img_b64


async def get_random_image_b64(cache: diskcache.Cache) -> str:
    for key in cache.iterkeys():
        image_b64: str | None = cache.get(key, None)  # type: ignore
        if image_b64 is None:
            cache.delete(key)
            continue

        if random.random() < 0.01:
            cache.delete(key)
        return image_b64

    random_picsum_image = await _get_random_picsum_image(1024, 1024)
    cache.add(key=str(uuid.uuid4()), value=random_picsum_image)
    return random_picsum_image


def generate_mask_with_circle(image_b64: str) -> str:
    image = Image.open(BytesIO(base64.b64decode(image_b64)))

    height, width = image.size

    center_x = np.random.randint(0, width)
    center_y = np.random.randint(0, height)
    radius = np.random.randint(20, 100)

    y, x = np.ogrid[:height, :width]

    mask = ((x - center_x) ** 2 + (y - center_y) ** 2 <= radius**2).astype(np.uint8)

    mask_bytes = mask.tobytes()
    mask_b64 = base64.b64encode(mask_bytes).decode("utf-8")

    return mask_b64


def construct_synthetic_data_task_key(task: str) -> str:
    return rcst.SYNTHETIC_DATA_KEY + ":" + task


async def get_synthetic_data_version(redis_db: Redis, task: str) -> float | None:
    version = await redis_db.hget(rcst.SYNTHETIC_DATA_VERSIONS_KEY, task)  # type: ignore
    if version is not None:
        return float(version.decode("utf-8"))  # type: ignore
    return None


# Takes anywhere from 1ms to 10ms
async def fetch_synthetic_data_for_task(redis_db: Redis, task: str) -> dict[str, Any]:
    synthetic_data = await rutils.json_load_from_redis(redis_db, key=construct_synthetic_data_task_key(task), default=None)
    if synthetic_data is None:
        raise ValueError(f"No synthetic data found for task: {task}")
    task_config = tcfg.get_enabled_task_config(task)
    if task_config is None:
        raise ValueError(f"No task config found for task: {task}")
    task_type = task_config.task_type
    if task_type == cmodels.TaskType.IMAGE:
        synthetic_data[scst.SEED] = random.randint(1, 1_000_000_000)
        synthetic_data[scst.TEXT_PROMPTS] = _get_random_text_prompt()
    elif task_type == cmodels.TaskType.TEXT:
        synthetic_data[scst.SEED] = random.randint(1, 1_000_000_000)
        synthetic_data[scst.TEMPERATURE] = round(random.uniform(0, 1), 2)
    else:
        raise ValueError(f"Unknown task type: {task_type}")

    return synthetic_data
