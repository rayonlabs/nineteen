"""
Calculates and schedules weights every SCORING_PERIOD
"""

from dotenv import load_dotenv
import os

from validator.utils.substrate.query_substrate import query_substrate


load_dotenv(os.getenv("ENV_FILE", ".vali.env"))

import asyncio

from validator.control_node.src.control_config import Config, load_config
from validator.db.src.sql.contenders import fetch_all_contenders
from validator.control_node.src.cycle import calculations
from fiber.chain import weights
from fiber.logging_utils import get_logger
from core import constants as ccst
from validator.db.src.sql.nodes import get_vali_node_id
from fiber.chain import fetch_nodes
from fiber.networking.models import NodeWithFernet as Node
from fiber.chain.interface import get_substrate

logger = get_logger(__name__)


async def _get_weights_to_set(config: Config) -> tuple[list[int], list[float]] | None:
    async with await config.psql_db.connection() as connection:
        contenders = await fetch_all_contenders(connection, None)

    if len(contenders) == 0:
        logger.warning("No contenders to calculate weights for!")
        return None
    else:
        logger.info(f"Found {len(contenders)} contenders to get weights for")
    node_ids, node_weights = await calculations.calculate_scores_for_settings_weights(config, contenders)

    return node_ids, node_weights


async def _get_and_set_weights(config: Config) -> None:
    validator_node_id = await get_vali_node_id(config.substrate, config.netuid, config.keypair.ss58_address)
    if validator_node_id is None:
        raise ValueError("Validator node id not found")
    result = await _get_weights_to_set(config)
    if result is None:
        logger.info("No weights to set. Skipping weight setting.")
        return
    node_ids, node_weights = result
    if len(node_ids) == 0:
        logger.info("No nodes to set weights for. Skipping weight setting.")
        return

    logger.info("Weights calculated, about to set...")

    all_nodes: list[Node] = fetch_nodes.get_nodes_for_netuid(config.substrate, config.netuid)
    all_node_ids = [node.node_id for node in all_nodes]
    all_node_weights = [0.0 for _ in all_nodes]
    for node_id, node_weight in zip(node_ids, node_weights):
        all_node_weights[node_id] = node_weight

    logger.info(f"Node ids: {all_node_ids}")
    logger.info(f"Node weights: {all_node_weights}")
    logger.info(f"Number of non zero node weights: {sum(1 for weight in all_node_weights if weight != 0)}")

    try:
        success = await asyncio.to_thread(
            weights.set_node_weights,
            substrate=config.substrate,
            keypair=config.keypair,
            node_ids=all_node_ids,
            node_weights=all_node_weights,
            netuid=config.netuid,
            version_key=ccst.VERSION_KEY,
            validator_node_id=int(validator_node_id),
            wait_for_inclusion=False,
            wait_for_finalization=False,
            max_attempts=3,
        )
    except Exception as e:
        logger.error(f"Failed to set weights: {e}")
        return False

    if success:
        logger.info("Weights set successfully.")
        return True
    else:
        logger.error("Failed to set weights :(")
        return False


async def _set_metagraph_weights(config: Config) -> None:
    nodes: list[Node] = fetch_nodes.get_nodes_for_netuid(config.substrate, config.netuid)
    node_ids = [node.node_id for node in nodes]
    node_weights = [node.incentive for node in nodes]
    validator_node_id = await get_vali_node_id(config.substrate, config.netuid, config.keypair.ss58_address)
    if validator_node_id is None:
        raise ValueError("Validator node id not found")

    await asyncio.to_thread(
        weights.set_node_weights,
        substrate=config.substrate,
        keypair=config.keypair,
        node_ids=node_ids,
        node_weights=node_weights,
        netuid=config.netuid,
        version_key=ccst.VERSION_KEY,
        validator_node_id=int(validator_node_id),
        wait_for_inclusion=False,
        wait_for_finalization=False,
        max_attempts=3,
    )


#


# To improve: use activity cutoff & The epoch length to set weights at the perfect times
async def set_weights_periodically(config: Config, just_once: bool = False) -> None:
    substrate = get_substrate(subtensor_address=config.substrate.url)
    substrate, uid = query_substrate(
        substrate, "SubtensorModule", "Uids", [config.netuid, config.keypair.ss58_address], return_value=True
    )

    consecutive_failures = 0
    while True:
        substrate, current_block = query_substrate(substrate, "System", "Number", [], return_value=True)
        substrate, last_updated_value = query_substrate(
            substrate, "SubtensorModule", "LastUpdate", [config.netuid], return_value=False
        )
        updated: float = current_block - last_updated_value[uid].value
        logger.info(f"Last updated: {updated} for my uid: {uid}")
        if updated < 150:
            logger.info(f"Last updated: {updated} - sleeping for a bit as we set recently...")
            await asyncio.sleep(12 * 25)  # sleep for 25 blocks
            continue

        if os.getenv("ENV", "prod").lower() == "dev":
            success = await _get_and_set_weights(config)
        else:
            try:
                success = await _get_and_set_weights(config)
            except Exception as e:
                logger.error(f"Failed to set weights with error: {e}")
                success = False

        if success:
            consecutive_failures = 0
            logger.info("Successfully set weights! Sleeping for 25 blocks before next check...")
            if just_once:
                return
            await asyncio.sleep(12 * 25)
            continue

        consecutive_failures += 1
        if just_once:
            logger.info("Failed to set weights, will try again...")
            await asyncio.sleep(12 * 1)
        else:
            logger.info(f"Failed to set weights {consecutive_failures} times in a row - sleeping for a bit...")
            await asyncio.sleep(12 * 25)  # Try again in 25 blocks

        if consecutive_failures == 1 or updated < 3000:
            continue

        if just_once or config.set_metagraph_weights_with_high_updated_to_not_dereg:
            logger.warning("Setting metagraph weights as our updated value is getting too high!")
            if just_once:
                logger.warning("Please exit if you do not want to do this!!!")
                await asyncio.sleep(4)
            try:
                success = await _set_metagraph_weights(config)
            except Exception as e:
                logger.error(f"Failed to set metagraph weights: {e}")
                success = False

            if just_once:
                return

            if success:
                consecutive_failures = 0
                continue


#


async def main():
    logger.info("Starting weight calculation...")
    config = load_config()
    logger.debug(f"Config: {config}")
    await config.psql_db.connect()

    just_once = os.getenv("JUST_ONCE", "false").lower() == "true"
    await set_weights_periodically(config, just_once=just_once)


if __name__ == "__main__":
    asyncio.run(main())
