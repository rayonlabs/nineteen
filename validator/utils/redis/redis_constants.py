from typing import Optional
import uuid
from redis.asyncio import Redis

# REDIS KEYS
SYNTHETIC_DATA_KEY = "SYNTHETIC_DATA"
SYNTHETIC_DATA_VERSIONS_KEY = "SYNTHETIC_DATA_VERSIONS"
SYNTHETIC_SCHEDULING_QUEUE_KEY = "SYNTHETIC_SCHEDULING_QUEUE"
PUBLIC_KEYPAIR_INFO_KEY = "PUBLIC_KEYPAIR_INFO"
QUERY_QUEUE_KEY = "QUERY_QUEUE"
QUERY_RESULTS_KEY = "QUERY_RESULTS"
HOTKEY_INFO_KEY = "HOTKEY_INFO"
CAPACITIES_KEY = "CAPACITIES"
CONTENDER_IDS_KEY = "CONTENDER_IDS"
PARITICIPANT_IDS_TO_STOP_KEY = "PARITICIPANT_IDS_TO_STOP"
WEIGHTS_TO_SET_QUEUE_KEY = "WEIGHTS_TO_SET_QUEUE"

# SIGNING STUFF
SIGNING_DENDRITE_QUEUE_KEY = "SIGNING_DENDRITE_QUEUE"
SIGNING_WEIGHTS_QUEUE_KEY = "SIGNING_WEIGHTS_QUEUE"
SIGNED_MESSAGES_KEY = "SIGNED_MESSAGES"
DENDRITE_TYPE = "dendrite"
WEIGHTS_TYPE = "weights"

TYPE = "type"
MESSAGE = "message"
JOB_ID = "job_id"
IS_B64ENCODED = "is_b64encoded"
SIGNATURE = "signature"

SYNTHETIC_QUERY = "synthetic_query"
ORGANIC_QUERY = "organic_query"
JOB_RESULTS = "JOB_RESULTS"

# RESPONSE QUEUE HANDLING
RESPONSE_QUEUE_PREFIX = "response_queue:"
RESPONSE_QUEUE_TTL = 20  # seconds

def get_response_queue_key(job_id: str) -> str:
    return f"{RESPONSE_QUEUE_PREFIX}{job_id}"

def generate_job_id() -> str:
    return uuid.uuid4().hex

async def ensure_queue_clean(redis_db: Redis, job_id: str) -> None:
    response_queue = get_response_queue_key(job_id)
    await redis_db.delete(response_queue)