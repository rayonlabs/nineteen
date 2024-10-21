from collections import defaultdict


from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

task_data = defaultdict(lambda: defaultdict(list))


class PeriodScore(BaseModel):
    hotkey: str
    task: str
    period_score: Optional[float]
    consumed_capacity: float
    created_at: datetime


class Contender(BaseModel):
    node_hotkey: str
    node_id: int
    netuid: int
    task: str
    raw_capacity: float = Field(..., description="Raw capacity straight from the miner")
    capacity: float = Field(..., description="Declared volume for the UID")
    capacity_to_score: float = Field(..., description="Volume to score")
    consumed_capacity: float = Field(0, description="Queried volume for the UID")
    total_requests_made: int = Field(0, description="Total requests made")
    requests_429: int = Field(0, description="HTTP 429 requests")
    requests_500: int = Field(0, description="HTTP 500 requests")
    period_score: Optional[float] = Field(None, description="Period score")

    @property
    def id(self) -> str:
        contender_id = self.node_hotkey + "-" + self.task
        return contender_id


def calculate_period_score(
    total_requests_made: float, capacity: float, consumed_capacity: float, requests_429: float, requests_500: float
) -> float | None:
    """
    Calculate a period score (not including quality which is scored separately)

    The closer we are to max volume used, the more forgiving we can be.
    For example, if you rate limited me loads (429), but I got most of your volume,
    then fair enough, perhaps I was querying too much

    But if I barely queried your volume, and you still rate limited me loads (429),
    then you're very naughty, you.
    """
    if total_requests_made == 0 or capacity == 0:
        return None

    capacity = max(capacity, 1)
    volume_unqueried = max(capacity - consumed_capacity, 0)

    percentage_of_volume_unqueried = volume_unqueried / capacity
    percentage_of_429s = requests_429 / total_requests_made
    percentage_of_500s = requests_500 / total_requests_made
    percentage_of_good_requests = (total_requests_made - requests_429 - requests_500) / total_requests_made

    rate_limit_punishment_factor = percentage_of_429s * percentage_of_volume_unqueried
    server_error_punishment_factor = percentage_of_500s * percentage_of_volume_unqueried

    return max(percentage_of_good_requests * (1 - rate_limit_punishment_factor) * (1 - server_error_punishment_factor), 0)


class RewardData(BaseModel):
    id: str
    task: str
    node_id: int
    quality_score: float
    validator_hotkey: str
    node_hotkey: str
    synthetic_query: bool
    metric: float | None = None
    response_time: float | None = None
    volume: float | None = None
    created_at: datetime = Field(default_factory=datetime.now)

    def dict(self):  # type: ignore
        return {
            "id": self.id,
            "task": self.task,
            "node_id": self.node_id,
            "quality_score": self.quality_score,
            "validator_hotkey": self.validator_hotkey,
            "node_hotkey": self.node_hotkey,
            "synthetic_query": self.synthetic_query,
            "metric": self.metric,
            "response_time": self.response_time,
            "volume": self.volume,
            "created_at": self.created_at.isoformat(),  # Convert datetime to ISO string
        }
