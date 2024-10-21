from datetime import datetime
import enum
from typing import Optional, Any

from pydantic import BaseModel
from pydantic.fields import Field


class Role(str, enum.Enum):
    """Message is sent by which role?"""

    user = "user"
    assistant = "assistant"
    system = "system"


class Message(BaseModel):
    role: Role = Role.user
    content: str = Field(default=..., examples=["Remind me that I have forgot to set the messages"])

    class Config:
        use_enum_values = True


class QueryResult(BaseModel):
    formatted_response: Any
    node_id: Optional[int]
    node_hotkey: Optional[str]
    response_time: Optional[float]
    task: str
    status_code: Optional[int]
    success: bool
    created_at: datetime = Field(default_factory=datetime.now)


class ImageHashes(BaseModel):
    average_hash: str = ""
    perceptual_hash: str = ""
    difference_hash: str = ""
    color_hash: str = ""
