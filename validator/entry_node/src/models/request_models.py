import random
from fastapi import HTTPException
import httpx
from pydantic import BaseModel, Field
from core.models import utility_models
from core.models import payload_models
from fiber.logging_utils import get_logger
from validator.utils.entry_utils import image_b64_is_valid, fetch_image_b64

logger = get_logger(__name__)


class ChatRequest(BaseModel):
    messages: list[utility_models.Message] = Field(...)
    temperature: float = Field(default=0.5, examples=[0.5, 0.4, 0.3], title="Temperature", description="Temperature for text generation.")
    max_tokens: int = Field(500, title="Max Tokens", description="Max tokens for text generation.")
    model: str = Field(..., examples=["chat-llama-3-2-3b"], title="Model")
    top_p: float = Field(default=1.0, title="Top P", description="Top P for text generation.")
    stream: bool = Field(default=True, title="Stream", description="Stream for text generation.")
    logprobs: bool = True

    class Config:
        use_enum_values = True


def chat_to_payload(chat_request: ChatRequest) -> payload_models.ChatPayload:
    return payload_models.ChatPayload(
        messages=chat_request.messages,
        temperature=chat_request.temperature,
        max_tokens=chat_request.max_tokens,
        model=chat_request.model.replace("_", "-"),
        top_p=chat_request.top_p,
        stream=True,
        logprobs=chat_request.logprobs,
        seed=random.randint(1, 100000),
    )


class TextToImageRequest(BaseModel):
    prompt: str = Field(..., description="Prompt for image generation")
    negative_prompt: str = Field("", description="Negative prompt for image generation")
    steps: int = Field(10, description="Steps for image generation")
    cfg_scale: float = Field(3, description="CFG scale for image generation")
    width: int = Field(1024, description="Width for image generation")
    height: int = Field(1024, description="Height for image generation")
    model: str = Field(..., examples=["proteus_text_to_image"], title="Model")


def text_to_image_to_payload(text_to_image_request: TextToImageRequest) -> payload_models.TextToImagePayload:
    return payload_models.TextToImagePayload(
        **text_to_image_request.model_dump(),
        seed=random.randint(1, 100000),
    )


class ImageToImageRequest(BaseModel):
    init_image: str = Field(
        ...,
        description="Base64 encoded image",
        examples=[
            "https://lastfm.freetls.fastly.net/i/u/770x0/443c5e1c35fd38bb5a49a7d00612dab3.jpg#443c5e1c35fd38bb5a49a7d00612dab3",
            "iVBORw0KGgoAAAANSUhEUgAAAAUA",
        ],
    )
    prompt: str = Field(..., examples=["A beautiful landscape with a river and mountains", "A futuristic city with flying cars"])
    negative_prompt: str | None = Field(None, description="Negative prompt for image generation")
    steps: int = Field(10, description="Steps for image generation")
    cfg_scale: float = Field(3, description="CFG scale for image generation")
    width: int = Field(1024, description="Width for image generation")
    height: int = Field(1024, description="Height for image generation")
    model: str = Field(..., examples=["proteus_image_to_image"], title="Model")
    image_strength: float = Field(0.5, description="Image strength for image generation")


async def image_to_image_to_payload(
    image_to_image_request: ImageToImageRequest, httpx_client: httpx.AsyncClient, prod: bool
) -> payload_models.ImageToImagePayload:
    image_b64 = (
        await fetch_image_b64(image_to_image_request.init_image, httpx_client)
        if "https://" in image_to_image_request.init_image
        else image_to_image_request.init_image
    )
    if not image_b64_is_valid(image_b64):
        raise HTTPException(status_code=400, detail="Invalid init image!")
    return payload_models.ImageToImagePayload(
        init_image=image_b64,
        prompt=image_to_image_request.prompt,
        negative_prompt=image_to_image_request.negative_prompt,
        steps=image_to_image_request.steps,
        cfg_scale=image_to_image_request.cfg_scale,
        width=image_to_image_request.width,
        height=image_to_image_request.height,
        model=image_to_image_request.model,
        image_strength=image_to_image_request.image_strength,
        seed=random.randint(1, 100000),
    )


class InpaintRequest(BaseModel):
    prompt: str = Field(..., description="Prompt for inpainting")
    negative_prompt: str | None = Field(None, description="Negative prompt for inpainting")
    steps: int = Field(10, description="Steps for inpainting")
    cfg_scale: float = Field(3, description="CFG scale for inpainting")
    width: int = Field(1024, description="Width for inpainting")
    height: int = Field(1024, description="Height for inpainting")
    init_image: str = Field(
        ...,
        description="Base64 encoded or URL for image",
        examples=[
            "https://lastfm.freetls.fastly.net/i/u/770x0/443c5e1c35fd38bb5a49a7d00612dab3.jpg#443c5e1c35fd38bb5a49a7d00612dab3",
            "iVBORw0KGgoAAAANSUhEUgAAAAUA",
        ],
    )
    mask: str = Field(
        ...,
        description="Base64 encoded or URL for image",
        examples=[
            "https://lastfm.freetls.fastly.net/i/u/770x0/443c5e1c35fd38bb5a49a7d00612dab3.jpg#443c5e1c35fd38bb5a49a7d00612dab3",
            "iVBORw0KGgoAAAANSUhEUgAAAAUA",
        ],
    )


async def inpaint_to_payload(
    inpaint_request: InpaintRequest, httpx_client: httpx.AsyncClient, prod: bool
) -> payload_models.InpaintPayload:
    image_b64 = (
        await fetch_image_b64(inpaint_request.init_image, httpx_client)
        if "https://" in inpaint_request.init_image
        else inpaint_request.init_image
    )
    if not image_b64_is_valid(image_b64):
        raise HTTPException(status_code=400, detail="Invalid init image!")

    mask_b64 = (
        await fetch_image_b64(inpaint_request.mask, httpx_client) if "https://" in inpaint_request.mask else inpaint_request.mask
    )
    if not image_b64_is_valid(mask_b64):
        raise HTTPException(status_code=400, detail="Invalid mask image!")
    return payload_models.InpaintPayload(
        prompt=inpaint_request.prompt,
        negative_prompt=inpaint_request.negative_prompt,
        steps=inpaint_request.steps,
        cfg_scale=inpaint_request.cfg_scale,
        width=inpaint_request.width,
        height=inpaint_request.height,
        init_image=image_b64,
        mask_image=mask_b64,
        seed=random.randint(1, 100000),
    )


class AvatarRequest(BaseModel):
    prompt: str = Field(
        ...,
        description="Prompt for avatar generation",
        examples=["A futuristic Man in a city with flying cars"],
    )
    negative_prompt: str | None = Field(
        None, description="Negative prompt for avatar generation", examples=["wheels", " mountains"]
    )
    steps: int = Field(10, description="Steps for avatar generation")
    cfg_scale: float = Field(3, description="CFG scale for avatar generation")
    width: int = Field(1024, description="Width for avatar generation")
    height: int = Field(1024, description="Height for avatar generation")
    ipadapter_strength: float = Field(0.5, description="Image Adapter Strength for avatar generation")
    control_strength: float = Field(0.5, description="Control Strength for avatar generation")
    init_image: str = Field(
        ...,
        description="Base64 encoded or URL for image",
        examples=[
            "https://lastfm.freetls.fastly.net/i/u/770x0/443c5e1c35fd38bb5a49a7d00612dab3.jpg#443c5e1c35fd38bb5a49a7d00612dab3",
            "iVBORw0KGgoAAAANSUhEUgAAAAUA",
        ],
    )



async def avatar_to_payload(
    avatar_request: AvatarRequest, httpx_client: httpx.AsyncClient, prod: bool
) -> payload_models.AvatarPayload:
    image_b64 = (
        await fetch_image_b64(avatar_request.init_image, httpx_client)
        if "https://" in avatar_request.init_image
        else avatar_request.init_image
    )
    if not image_b64_is_valid(image_b64):
        raise HTTPException(status_code=400, detail="Invalid init image!")
    return payload_models.AvatarPayload(
        init_image=image_b64,
        prompt=avatar_request.prompt,
        negative_prompt=avatar_request.negative_prompt,
        steps=avatar_request.steps,
        width=avatar_request.width,
        height=avatar_request.height,
        seed=random.randint(1, 100000),
        ipadapter_strength=avatar_request.ipadapter_strength,
        control_strength=avatar_request.control_strength,
    )


class ImageResponse(BaseModel):
    image_b64: str
