import random
from typing import Any, Tuple
from core import task_config as tcfg
from validator.utils.redis import (
    redis_utils as rutils,
    redis_constants as rcst,
)
from validator.utils.synthetic import synthetic_constants as scst
from redis.asyncio import Redis
from core.models import config_models as cmodels
from nltk.tokenize import sent_tokenize, word_tokenize
import numpy as np
from time import time
import re
import base64
from io import BytesIO
import asyncio
import aiohttp
import diskcache
from PIL import Image
import uuid
import numpy as np
from fiber.logging_utils import get_logger
import random
import json
from functools import lru_cache

logger = get_logger(__name__)

random_text_queue = asyncio.Queue(maxsize=scst.RANDOM_TEXT_QUEUE_MAX_SIZE)

@lru_cache(maxsize=None)
def get_synth_corpus():
    try:
        with open("assets/synth_corpus.json", "r") as fh:
            synth_corpus = json.load(fh)
    except FileNotFoundError:
        with open("validator/control_node/assets/synth_corpus.json", "r") as fh:
            synth_corpus = json.load(fh)
    return synth_corpus


def split_sentences(text):
    fragments = sent_tokenize(text)
    return [frag for frag in fragments if len(frag.split()) > 2]

async def get_random_text_from_queue(): 
    try:
        if not random_text_queue.empty():
            return await random_text_queue.get()
        return None
    except Exception as e:
        logger.error(f"Error retrieving text from queue: {e}")
        return None

async def generate_text(corpus, n_words):
    random.seed(time()%10000)
    generated_text_parts = []

    current_word_count = 0
    categories = list(corpus.keys())

    while current_word_count < n_words:
        random.shuffle(categories)
        # randomly select text from random categories, until we reach n_words
        for i, category in enumerate(categories):
            sentence = random.choice(corpus[category]).strip()
            sentences_in_category = split_sentences(sentence)

            if not sentences_in_category:
                continue

            if i > 0 and i%3 == 0:
                sentence_part = await get_random_text_from_queue()
            else:    
                sentence_part = random.choice(sentences_in_category)

            if not sentence_part:
                continue
            
            sentence_word_count = len(word_tokenize(sentence_part))
            if current_word_count + sentence_word_count > n_words:
                remaining_words = n_words - current_word_count
                truncated_part = ' '.join(sentence_part.split()[:remaining_words])
                generated_text_parts.append(truncated_part)
                current_word_count += remaining_words
                break

            generated_text_parts.append(sentence_part)
            current_word_count += sentence_word_count

            if current_word_count >= n_words:
                break

        if not generated_text_parts:
            raise ValueError("Unable to generate text, problem with corpus?")
        
    merged_text = ' '.join(generated_text_parts).strip()
    possible_endings = ['.', '!', '?', '...']

    if merged_text and merged_text[-1] not in possible_endings:
        if random.choice([True, False]):
            merged_text += random.choice(possible_endings)

    merged_text = re.sub(r'[^\x20-\x7E]', '', merged_text).strip()
    return merged_text

def get_random_int_from_dist(size=1, gamma_mean=1000, max_value=8000, gamma_shape=0.5, gaussian_mean=1000, gaussian_weight=0.3, gaussian_std=850):
    gamma_scale = gamma_mean / gamma_shape
    gamma_samples = np.random.gamma(gamma_shape, gamma_scale, size)
    gaussian_samples = np.random.normal(gaussian_mean, gaussian_std, size)
    combined_samples = gaussian_weight * gaussian_samples + (1 - gaussian_weight) * gamma_samples
    combined_samples = combined_samples[combined_samples < max_value]

    return combined_samples

async def fetch_random_text() -> Tuple[str, int, int]:

    n_paragraphes = random.randint(2, 4)
    n_sentences = random.randint(1, 6)
    url = f'http://metaphorpsum.com/paragraphs/{n_paragraphes}/{n_sentences}'
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    text = await response.text()
                    logger.debug(f"Fetched random text with {n_paragraphes} parapgraphes & {n_sentences} sentences")
                    return text, n_paragraphes, n_sentences
                else:
                    error_msg = f"Failed to fetch text from metaphorpsum.com: {response.status}"
                    logger.error(error_msg)
                    raise aiohttp.ClientError(error_msg)
        except aiohttp.ClientError as e:
            logger.error(f"Network error while fetching text: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error while fetching text: {e}")
            raise

async def get_save_random_text() -> None:
    while True:
        try:
            queue_size = random_text_queue.qsize()
            
            if queue_size < scst.RANDOM_TEXT_QUEUE_MAX_SIZE:
                text, n_paragraphes, n_sentences = await fetch_random_text()                
                await random_text_queue.put(text)
                logger.info(f"Pushed random metaphorpsum.com text with {n_paragraphes} paragraphs, and {n_sentences} sentences to queue")
            else:
                logger.info("Queue is full. Skipping text insertion")                
            
            await asyncio.sleep(2)
            
        except Exception as e:
            logger.error(f"Error fetching and saving synthetic data: {e} - sleeping for 60s")
            await asyncio.sleep(60)


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
