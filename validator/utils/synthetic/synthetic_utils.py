import random
import base64
from io import BytesIO
import aiohttp
from PIL import Image
import uuid
import numpy as np
from fiber.logging_utils import get_logger
from validator.utils.synthetic import synthetic_constants as scst
from redis.asyncio import Redis

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


async def _get_random_cached_image(redis_db: Redis, cache_key: str) -> str | None:
    if await redis_db.scard(cache_key) > 0:
        random_key = await redis_db.srandmember(cache_key)
        return await redis_db.get(random_key)
    return None

async def _remove_random_cached_image(redis_db: Redis, cache_key: str) -> None:
    if await redis_db.scard(cache_key) > 0:
        random_key = await redis_db.srandmember(cache_key)
        await redis_db.srem(cache_key, random_key)
        await redis_db.delete(random_key)

async def _add_image_to_cache(redis_db: Redis, cache_key: str, image_b64: str) -> None:
    new_key = str(uuid.uuid4())
    await redis_db.set(new_key, image_b64)
    await redis_db.sadd(cache_key, new_key)

async def _manage_cache_size(redis_db: Redis, cache_key: str, cache_size: int) -> None:
    if await redis_db.scard(cache_key) >= cache_size:
        await _remove_random_cached_image(redis_db, cache_key)

async def get_random_image_b64(redis_db: Redis) -> str:
    cache_key = scst.IMAGE_CACHE_KEY
    cache_size = scst.IMAGE_CACHE_SIZE

    logger.debug(f"Starting get_random_image_b64. Cache key: {cache_key}, Cache size: {cache_size}")

    if random.random() < 0.01:
        await _remove_random_cached_image(redis_db, cache_key)
    else:
        cached_image = await _get_random_cached_image(redis_db, cache_key)
        if cached_image:
            return cached_image

    # If cache is empty or we decided to get a new image
    random_picsum_image = await _get_random_picsum_image(1024, 1024)
    await _manage_cache_size(redis_db, cache_key, cache_size)
    await _add_image_to_cache(redis_db, cache_key, random_picsum_image)
    return random_picsum_image


# TODO: Pass image size instead of image b64 to avoid decoding each time. Or cache the mask if randomization is not needed.
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




