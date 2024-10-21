from functools import partial
import os
from fastapi.routing import APIRouter
from fiber.miner.dependencies import blacklist_low_stake, get_config, verify_request
from fiber.miner.security.encryption import decrypt_general_payload
from core.models.payload_models import CapacityPayload
from fiber.logging_utils import get_logger
from fastapi import Depends, Header
from fiber.miner.core.configuration import Config
from fiber import constants as fcst
from core import constants as cst

logger = get_logger(__name__)


async def capacity(
    configs: CapacityPayload = Depends(partial(decrypt_general_payload, CapacityPayload)),
    validator_hotkey: str = Header(..., alias=fcst.VALIDATOR_HOTKEY),
    config: Config = Depends(get_config),
) -> dict[str, float | str]:
    logger.info(f"Received task configs: {configs} from validator {validator_hotkey}. I should do something with this info...")

    my_miner_type = os.getenv("MINER_TYPE")
    
    metagraph = config.metagraph
    validator_node = metagraph.nodes.get(validator_hotkey)
    total_stake = sum(node.stake for node in metagraph.nodes.values())

    # NOTE: Below needs to be optimised on a per-validator basis - This is up to you!
    capacities = {cst.MINER_TYPE: my_miner_type}
    for task_config in configs.task_configs:
        task = task_config[cst.TASK]
        max_capacity = task_config[cst.MAX_CAPACITY]
        task_type = task_config[cst.TASK_TYPE]  # noqa
        model_config = task_config[cst.MODEL_CONFIG]  # noqa
        endpoint = task_config[cst.ENDPOINT]  # noqa
        weight = task_config[cst.WEIGHT]

        if my_miner_type != task_type:
            continue
        
        # TO help dev by just returning 10% to all  validators
        if os.getenv("ENV", "prod").lower() == "dev":
            capacities[task] = max_capacity * 0.1
        elif weight > 0:
            capacities[task] = max_capacity * validator_node.stake / total_stake

    logger.debug(f"Returning capacities: {capacities}")
    return capacities


def factory_router() -> APIRouter:
    router = APIRouter()
    router.add_api_route(
        "/capacity",
        capacity,
        tags=["Subnet"],
        methods=["POST"],
        dependencies=[Depends(blacklist_low_stake), Depends(verify_request)],
    )
    return router
