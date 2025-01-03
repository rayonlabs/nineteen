from typing import Any
from pydantic import BaseModel, Field
from core.models import utility_models
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


class ChatRequest(BaseModel):
    messages: list[utility_models.Message] = Field(...)
    temperature: float = Field(
        default=0.5, examples=[0.5, 0.4, 0.3], title="Temperature", description="Temperature for text generation."
    )
    max_tokens: int = Field(500, title="Max Tokens", description="Max tokens for text generation.")
    model: str = Field(..., examples=["unsloth/Llama-3.2-3B-Instruct"], title="Model")
    top_p: float = Field(default=1.0, title="Top P", description="Top P for text generation.")
    stream: bool = Field(default=True, title="Stream", description="Stream for text generation.")
    logprobs: bool = True

    class Config:
        use_enum_values = True


class CompletionRequest(BaseModel):
    prompt: str = Field(...)
    temperature: float = Field(
        default=0.5, examples=[0.5, 0.4, 0.3], title="Temperature", description="Temperature for text generation."
    )
    max_tokens: int = Field(500, title="Max Tokens", description="Max tokens for text generation.")
    model: str = Field(default=..., examples=["unsloth/Llama-3.2-3B-Instruct"], title="Model")
    top_p: float = Field(default=1.0, title="Top P", description="Top P for text generation.")
    stream: bool = Field(default=True, title="Stream", description="Stream for text generation.")
    logprobs: bool = True

    class Config:
        use_enum_values = True

class TextToImageRequest(BaseModel):
    prompt: str = Field(..., description="Prompt for image generation")
    negative_prompt: str = Field("", description="Negative prompt for image generation")
    steps: int = Field(10, description="Steps for image generation")
    cfg_scale: float = Field(3, description="CFG scale for image generation")
    width: int = Field(1024, description="Width for image generation")
    height: int = Field(1024, description="Height for image generation")
    model: str = Field(..., examples=["proteus_text_to_image"], title="Model")


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


class ImageResponse(BaseModel):
    image_b64: str


class TextModelResponse(BaseModel):
    id: str
    name: str
    created: int
    description: str

    context_length: int
    is_moderated: bool = False

    architecture: dict[str, Any]
    pricing: dict[str, Any]
    endpoints: list[str]

    per_request_limits: None = None


class ImageModelResponse(BaseModel):
    id: str
    name: str
    created: int
    description: str
    pricing: dict[str, Any]
