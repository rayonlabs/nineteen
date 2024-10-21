import base64
from dataclasses import dataclass


@dataclass
class QueryQueueMessage:
    query_type: str
    query_payload: dict
    task: str
    job_id: str


@dataclass
class PublicKeypairInfo:
    ss58_address: str
    ss58_format: int
    crypto_type: str
    public_key: str


@dataclass
class WeightsToSet:
    node_ids: list[int]
    node_weights: list[float]
    version_key: int
    netuid: int = 19


@dataclass
class SigningPayload:
    message: bytes | str
    job_id: str
    is_b64encoded: bool

    def to_dict(self):
        if isinstance(self.message, bytes):
            return {
                "message": base64.b64encode(self.message).decode("utf-8"),
                "job_id": self.job_id,
                "is_b64encoded": True,
            }
        elif isinstance(self.message, str):
            return {"message": self.message, "job_id": self.job_id, "is_b64encoded": False}
        else:
            raise TypeError("message must be either bytes or str")

    @classmethod
    def from_dict(cls, data):
        is_b64encoded = data["is_b64encoded"]
        message = data["message"]
        if is_b64encoded:
            message = base64.b64decode(message)
        return cls(message=message, job_id=data["job_id"], is_b64encoded=is_b64encoded)


@dataclass
class SignedPayload:
    signature: str
    job_id: str
