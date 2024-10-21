import enum
import json
import time
from typing import Any, Dict, List, Optional, Union
import httpx
from pydantic import BaseModel
from fiber.logging_utils import get_logger
from fiber import Keypair
from core import constants as ccst
from datetime import datetime
from validator.models import RewardData

logger = get_logger(__name__)


class DataTypeToPost(enum.Enum):
    REWARD_DATA = 1
    UID_RECORD = 2
    MINER_CAPACITIES = 3
    VALIDATOR_INFO = 4
    MINER_TYPES = 5
    MINER_WEIGHTS = 6
    CONTENDER_WEIGHTS_INFO = 7



data_type_to_url = {
    DataTypeToPost.REWARD_DATA: ccst.BASE_NINETEEN_API_URL + "v1/store/reward_data",
    DataTypeToPost.UID_RECORD: ccst.BASE_NINETEEN_API_URL + "v1/store/uid_records",
    DataTypeToPost.MINER_CAPACITIES: ccst.BASE_NINETEEN_API_URL + "v1/store/miner_capacities",
    DataTypeToPost.VALIDATOR_INFO: ccst.BASE_NINETEEN_API_URL + "v1/store/validator_info",
    DataTypeToPost.MINER_TYPES: ccst.BASE_NINETEEN_API_URL + "v1/store/miner_types",
    DataTypeToPost.MINER_WEIGHTS: ccst.BASE_NINETEEN_API_URL + "v1/store/miner_weights",
    DataTypeToPost.CONTENDER_WEIGHTS_INFO: ccst.BASE_NINETEEN_API_URL + "v1/store/contender_weights_info",
}

# Turn off if you don't wanna post your validator info to nineteen.ai
POST_TO_NINETEEN_AI = True


def _sign_timestamp(keypair: Keypair, timestamp: float) -> str:
    return f"0x{keypair.sign(str(timestamp)).hex()}"


async def post_to_nineteen_ai(
    data_to_post: Union[Dict[str, Any], List[Dict[str, Any]]],
    keypair: Keypair,
    data_type_to_post: DataTypeToPost,
    timeout: int = 10,
) -> None:
    logger.debug(f"Sending {data_type_to_post} to {ccst.BASE_NINETEEN_API_URL}. Data: {data_to_post}")
    if not POST_TO_NINETEEN_AI:
        return
    timestamp = time.time()
    public_address = keypair.ss58_address
    signed_timestamp = _sign_timestamp(keypair, timestamp)

    headers = {
        "Content-Type": "application/json",
        "x-timestamp": str(timestamp),
        "x-signature": signed_timestamp,
        "x-public-key": public_address,
    }

    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            resp = await client.post(
                url=data_type_to_url[data_type_to_post],
                data=json.dumps(data_to_post),
                headers=headers,
            )
            logger.info(f"Resp status code from {ccst.BASE_NINETEEN_API_URL}: {resp.status_code} for post type {data_type_to_post}")
            resp.raise_for_status()
            return resp
        except Exception as e:
            logger.error(f"Error when posting to {ccst.BASE_NINETEEN_API_URL} to store data for {data_type_to_post}: {repr(e)}")


class MinerTypesPostBody(BaseModel):
    validator_hotkey: str
    miner_hotkey: str
    miner_type: str

class RewardDataPostBody(RewardData):
    testnet: bool


class ValidatorInfoPostBody(BaseModel):
    versions: str
    validator_hotkey: str
    task_configs: list[dict[str, Any]]


class MinerCapacitiesPostObject(BaseModel):
    miner_hotkey: str
    task: str
    volume: float
    validator_hotkey: str

class MinerWeightsPostObject(BaseModel):
    version_key: int
    netuid: int
    validator_hotkey: str
    created_at: datetime
    miner_hotkey: str
    node_weight: float

class ContenderWeightsInfoPostObject(BaseModel):
    version_key: int
    netuid: int
    validator_hotkey: str
    created_at: datetime
    miner_hotkey: str
    task: str

    average_quality_score: float
    metric_bonus: float
    combined_quality_score: float

    period_score_multiplier: float
    normalised_period_score: float

    contender_capacity: float
    
    normalised_net_score: float


class ContenderPayload(BaseModel):
    node_id: int
    node_hotkey: str
    validator_hotkey: str
    task: str
    declared_volume: float
    consumed_volume: Optional[float]
    total_requests_made: Optional[int]
    requests_429: Optional[int]
    requests_500: Optional[int]


class UidRecordPostObject(BaseModel):
    axon_uid: int
    miner_hotkey: str
    validator_hotkey: str
    task: str
    declared_volume: float
    consumed_volume: float
    total_requests_made: int
    requests_429: int
    requests_500: int
    period_score: Optional[float]

    def dict(self):
        return {
            "axon_uid": self.axon_uid,
            "miner_hotkey": self.miner_hotkey,
            "validator_hotkey": self.validator_hotkey,
            "task": self.task,
            "declared_volume": self.declared_volume,
            "consumed_volume": self.consumed_volume,
            "total_requests_made": self.total_requests_made,
            "requests_429": self.requests_429,
            "requests_500": self.requests_500,
            "period_score": self.period_score,
        }


class UidRecordsPostBody(BaseModel):
    data: List[UidRecordPostObject]

    def dump(self):
        return [ob.dict() for ob in self.data]
