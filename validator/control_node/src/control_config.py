import os
from dataclasses import dataclass
from redis.asyncio import Redis

from fiber.logging_utils import get_logger

from fiber.chain import interface
from fiber.chain import chain_utils


from validator.db.src.database import PSQLDB

import httpx

from substrateinterface import SubstrateInterface, Keypair


logger = get_logger(__name__)


@dataclass
class Config:
    substrate: SubstrateInterface
    keypair: Keypair
    psql_db: PSQLDB
    redis_db: Redis
    subtensor_network: str
    subtensor_address: str | None
    gpu_server_address: str | None
    netuid: int
    replace_with_localhost: bool
    replace_with_docker_localhost: bool
    refresh_nodes: bool
    capacity_to_score_multiplier: float
    httpx_client: httpx.AsyncClient
    scoring_period_time_multiplier: float
    set_metagraph_weights_with_high_updated_to_not_dereg: bool
    testnet: bool = os.getenv("SUBTENSOR_NETWORK", "").lower() == "test"
    debug: bool = os.getenv("ENV", "prod").lower() != "prod"


def load_config() -> Config:
    subtensor_network = os.getenv("SUBTENSOR_NETWORK")
    subtensor_address = os.getenv("SUBTENSOR_ADDRESS") or None
    gpu_server_address = os.getenv("GPU_SERVER_ADDRESS", None)
    dev_env = os.getenv("ENV", "prod").lower() != "prod"
    if not gpu_server_address:
        if not dev_env:
            logger.error("GPU_SERVER_ADDRESS IT NOT SET - Please make sure env is Dev if you want to run without a GPU server")
            raise ValueError("GPU_SERVER_ADDRESS must be set if env is prod")
        gpu_server_address = None

    wallet_name = os.getenv("WALLET_NAME", "default")
    hotkey_name = os.getenv("HOTKEY_NAME", "default")
    netuid = os.getenv("NETUID")
    if netuid is None:
        raise ValueError("NETUID must be set")
    else:
        netuid = int(netuid)

    localhost = bool(os.getenv("LOCALHOST", "false").lower() == "true")
    if localhost:
        redis_host = "localhost"
        os.environ["POSTGRES_HOST"] = "localhost"
    else:
        redis_host = os.getenv("REDIS_HOST", "redis")

    replace_with_docker_localhost = bool(os.getenv("REPLACE_WITH_DOCKER_LOCALHOST", "false").lower() == "true")

    refresh_nodes: bool = os.getenv("REFRESH_NODES", "true").lower() == "true"
    if refresh_nodes:
        substrate = interface.get_substrate(subtensor_network=subtensor_network, subtensor_address=subtensor_address)
    else:
        substrate = None
    keypair = chain_utils.load_hotkey_keypair(wallet_name=wallet_name, hotkey_name=hotkey_name)

    default_capacity_to_score_multiplier = 0.1 if subtensor_network == "test" else 1.0
    capacity_to_score_multiplier = float(os.getenv("CAPACITY_TO_SCORE_MULTIPLIER", default_capacity_to_score_multiplier))
    logger.info(f"Capacity to score multiplier: {capacity_to_score_multiplier}")

    httpx_limits = httpx.Limits(max_connections=500, max_keepalive_connections=100)
    httpx_client = httpx.AsyncClient(limits=httpx_limits)

    scoring_period_time_multiplier = float(os.getenv("SCORING_PERIOD_TIME_MULTIPLIER", 1.0))

    set_metagraph_weights_with_high_updated_to_not_dereg = bool(
        os.getenv("SET_METAGRAPH_WEIGHTS_WITH_HIGH_UPDATED_TO_NOT_DEREG", "false").lower() == "true"
    )

    return Config(
        substrate=substrate,  # type: ignore
        keypair=keypair,
        psql_db=PSQLDB(),
        redis_db=Redis(host=redis_host),
        subtensor_network=subtensor_network,
        subtensor_address=subtensor_address,
        netuid=netuid,
        replace_with_docker_localhost=replace_with_docker_localhost,
        replace_with_localhost=localhost,
        refresh_nodes=refresh_nodes,
        capacity_to_score_multiplier=capacity_to_score_multiplier,
        httpx_client=httpx_client,
        gpu_server_address=gpu_server_address,
        debug=dev_env,
        scoring_period_time_multiplier=scoring_period_time_multiplier,
        set_metagraph_weights_with_high_updated_to_not_dereg=set_metagraph_weights_with_high_updated_to_not_dereg,
    )
