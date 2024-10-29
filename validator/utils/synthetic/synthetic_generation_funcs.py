import asyncio
import random
import sys

from core.models import utility_models
from validator.utils.synthetic import synthetic_constants as scst
from core import task_config as tcfg
from core.models import payload_models
from PIL import Image
import io
import base64
import markovify
import datasets
from functools import lru_cache
from fiber.logging_utils import get_logger
from validator.utils.synthetic import synthetic_utils as sutils
import binascii
from redis.asyncio import Redis

logger = get_logger(__name__)


# NOTE: any danger here of massively growing cache?
@lru_cache(maxsize=1)
def get_cached_markov_model():
    logger.info("Loading markov model from caption_data...")
    # This try / except is so we work fine on docker and localhost
    try:
        dataset = datasets.load_dataset("assets/caption_data/data")
    except FileNotFoundError:
        dataset = datasets.load_dataset("validator/control_node/assets/caption_data/data")
    text = [i["query"] for i in dataset["train"]]  # type: ignore
    return markovify.Text(" ".join(text))


# Async wrapper to use the cached model
async def markov_model_factory():
    return await asyncio.to_thread(get_cached_markov_model)



async def _get_markov_sentence(max_words: int = 10) -> str:
    markov_text_generation_model = await markov_model_factory()
    text = None
    while text is None:
        text = markov_text_generation_model.make_sentence(max_words=max_words)
    return text


def base64_to_pil(image_b64: str) -> Image.Image | None:
    try:
        image_data = base64.b64decode(image_b64)
        image = Image.open(io.BytesIO(image_data))
        return image
    except binascii.Error:
        return None


def _load_postie_to_pil(image_path: str) -> Image.Image | None:
    with open(image_path, "rb") as image_file:
        base64_string = base64.b64encode(image_file.read()).decode("utf-8")
    pil_image = base64_to_pil(base64_string)

    return pil_image


def get_randomly_edited_face_picture_for_avatar() -> str | None:
    """
    For avatar we need a face image.

    We must satisfy the criteria: image must not be cacheable

    As long as we satisfy that, we're good - since we score organic queries.

    Hence, we can use a single picture and just edit it to generate 2**(1024*1024) unique images
    """
    try:
        my_boy_postie = _load_postie_to_pil("assets/postie.png")
    except FileNotFoundError:
        my_boy_postie = _load_postie_to_pil("validator/control_node/assets/postie.png")
    return _alter_my_boy_postie(my_boy_postie)


def _alter_my_boy_postie(my_boy_postie: Image.Image | None) -> str | None:
    if my_boy_postie is None:
        return None
    b64_postie_altered = alter_image(my_boy_postie)
    return b64_postie_altered


def pil_to_base64(image: Image.Image, format: str = "JPEG") -> str | None:
    buffered = io.BytesIO()
    image.save(buffered, format=format)
    img_str = base64.b64encode(buffered.getvalue()).decode()
    return img_str


def alter_image(
    pil_image: Image.Image,
) -> str | None:
    for _ in range(3):
        rand_x, rand_y = (
            random.randint(0, pil_image.width - 1),
            random.randint(0, pil_image.height - 1),
        )

        pixel = list(pil_image.getpixel((rand_x, rand_y)))  # type: ignore
        for i in range(3):
            change = random.choice([-1, 1])
            pixel[i] = max(0, min(255, pixel[i] + change))
        pil_image.putpixel((rand_x, rand_y), tuple(pixel))

    if pil_image.mode == "RGBA":
        pil_image = pil_image.convert("RGB")

    new_image = pil_to_base64(pil_image)
    return new_image


async def generate_chat_synthetic(model: str, redis_db: Redis) -> payload_models.ChatPayload:
    user_content = await _get_markov_sentence(max_words=140)
    messages = [utility_models.Message(content=user_content, role=utility_models.Role.user)]

    if random.random() < 0.1:
        messages.append(
            utility_models.Message(
                content=await _get_markov_sentence(max_words=140),
                role=utility_models.Role.assistant,
            )
        )
        messages.append(
            utility_models.Message(
                content=await _get_markov_sentence(max_words=140),
                role=utility_models.Role.user,
            )
        )
    return payload_models.ChatPayload(
        messages=messages,
        temperature=round(random.random(), 1),
        max_tokens=1024,
        seed=random.randint(1, scst.MAX_SEED),
        model=model,
        top_p=1,
    )


async def generate_text_to_image_synthetic(
    model: str,
    redis_db: Redis,
) -> payload_models.TextToImagePayload:
    prompt = await _get_markov_sentence(max_words=20)
    negative_prompt = await _get_markov_sentence(max_words=20)
    # TODO: Fix to be our allowed seeds for the relay mining solution
    seed = random.randint(1, scst.MAX_SEED)

    # NOTE: Needs to be in task config perhaps to make more robust?
    height = 1024
    width = 1024
    cfg_scale = 3.0
    steps = 8

    return payload_models.TextToImagePayload(
        prompt=prompt,
        negative_prompt=negative_prompt,
        seed=seed,
        height=height,
        width=width,
        cfg_scale=cfg_scale,
        steps=steps,
        model=model,
    )


async def generate_image_to_image_synthetic(
    model: str,
    redis_db: Redis,
) -> payload_models.ImageToImagePayload:

    prompt = await _get_markov_sentence(max_words=20)
    negative_prompt = await _get_markov_sentence(max_words=20)
    # TODO: Fix to be our allowed seeds for the relay mining solution
    seed = random.randint(1, scst.MAX_SEED)

    # NOTE: Needs to be in task config perhaps to make more robust?
    height = 1024
    width = 1024
    cfg_scale = 2.0
    steps = 8
    image_strength = 0.5

    init_image = await sutils.get_random_image_b64(redis_db)

    return payload_models.ImageToImagePayload(
        prompt=prompt,
        negative_prompt=negative_prompt,
        seed=seed,
        steps=steps,
        cfg_scale=cfg_scale,
        width=width,
        height=height,
        image_strength=image_strength,
        model=model,
        init_image=init_image,
    )


async def generate_inpaint_synthetic(
    redis_db: Redis,
) -> payload_models.InpaintPayload:

    prompt = await _get_markov_sentence(max_words=20)
    negative_prompt = await _get_markov_sentence(max_words=20)
    seed = random.randint(1, scst.MAX_SEED)

    init_image = await sutils.get_random_image_b64(redis_db)
    mask_image = sutils.generate_mask_with_circle(init_image)

    return payload_models.InpaintPayload(
        prompt=prompt,
        negative_prompt=negative_prompt,
        cfg_scale=2.0,
        seed=seed,
        height=1016,
        width=1016,
        steps=8,
        init_image=init_image,
        mask_image=mask_image,
    )


async def generate_avatar_synthetic(redis_db: Redis) -> payload_models.AvatarPayload:
    prompt = await _get_markov_sentence(max_words=20)
    negative_prompt = await _get_markov_sentence(max_words=20)
    seed = random.randint(1, scst.MAX_SEED)

    init_image = None
    max_retries = 10
    retries = 0
    while init_image is None and retries < max_retries:
        init_image = get_randomly_edited_face_picture_for_avatar()
        if init_image is None:
            logger.warning("Init image is None, regenerating")
            retries += 1
            await asyncio.sleep(0.1)

    if init_image is None:
        raise ValueError("Failed to generate init image")
    return payload_models.AvatarPayload(
        prompt=prompt,
        negative_prompt=negative_prompt,
        ipadapter_strength=0.5,
        control_strength=0.5,
        height=1280,
        width=1280,
        seed=seed,
        steps=8,
        init_image=init_image,
    )


async def generate_synthetic_data(task: str, redis_db: Redis) -> (
    payload_models.ChatPayload |
    payload_models.TextToImagePayload |
    payload_models.ImageToImagePayload |
    payload_models.InpaintPayload |
    payload_models.AvatarPayload |
    None
):
    """
    Gets task config and dynamically calls the synthetic generation function
    Not super clean, but it works
    """
    task_config = tcfg.get_enabled_task_config(task)
    if task_config is None:
        return None
    generative_function_name = task_config.synthetic_generation_config.func

    if generative_function_name not in sys.modules[__name__].__dict__:
        raise ValueError(f"Function {generative_function_name} not found in generate_synthetic_data, some config is wrong")

    # with gutils.log_time(f"Generating synthetic data for {task}", logger):
    #     func = getattr(sys.modules[__name__], generative_function_name)
    #     kwargs = task_config.synthetic_generation_config.kwargs

    func = getattr(sys.modules[__name__], generative_function_name)
    kwargs = task_config.synthetic_generation_config.kwargs
    kwargs['redis_db'] = redis_db # synthetic generation funcs need redis_db injected

    return await func(**kwargs)
