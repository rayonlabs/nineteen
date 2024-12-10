"""
A cycle consists of
- Refreshing metagraph to get nodes (if not refreshed for X time in the case of restarts)
- Handshaking with the nodes
- Gathering the contenders from the nodes by querying for capacities
- Deciding what % of each contender should be queried
- Scheduling synthetics according the the amount of volume I need to query
- Getting the contender_scores from the 429's, 500's and successful queries
- Calculating weights when the scoring period is expired
- Setting the weights on the nodes async while starting the next cycle
"""

import asyncio
from datetime import datetime
from validator.control_node.src.control_config import Config
from validator.control_node.src.cycle import (
    refresh_nodes,
    refresh_contenders,
)
from validator.control_node.src.cycle.schedule_synthetic_queries import schedule_synthetics_until_done
from validator.db.src.sql.nodes import (
    get_nodes,
)
from validator.db.src.sql.nodes import insert_symmetric_keys_for_nodes
from fiber.logging_utils import get_logger

from validator.models import Contender
from validator.utils.post.nineteen import DataTypeToPost, ValidatorInfoPostBody, post_to_nineteen_ai
from core.task_config import get_public_task_configs
from core import constants as ccst
import httpx
from validator.db.src.sql.rewards_and_scores import delete_task_data_older_than_date

logger = get_logger(__name__)

async def get_worker_version(gpu_server_address: str, endpoint: str = '/worker-version'):
    url = f"{gpu_server_address.rstrip('/')}{endpoint}"

    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.text


async def _post_vali_stats(config: Config):
    public_configs = get_public_task_configs()

    if config.gpu_server_address:
        vali_worker_version = await get_worker_version(config.gpu_server_address)
        versions=str(ccst.VERSION_KEY) + ':' + vali_worker_version
    else:
        versions=str(ccst.VERSION_KEY)

    await post_to_nineteen_ai(
        data_to_post=ValidatorInfoPostBody(
            validator_hotkey=config.keypair.ss58_address,
            task_configs=public_configs,
            versions=versions,
        ).model_dump(mode="json"),
        keypair=config.keypair,
        data_type_to_post=DataTypeToPost.VALIDATOR_INFO,
    )


async def get_nodes_and_contenders(config: Config) -> list[Contender] | None:
    logger.info("Starting cycle...")
    if config.refresh_nodes:
        logger.info("First refreshing metagraph and getting nodes")
        initial_nodes = await refresh_nodes.get_refresh_nodes(config)
    else:
        initial_nodes = await get_nodes(config.psql_db, config.netuid)

    await _post_vali_stats(config)

    logger.info("Got nodes! Performing handshakes now...")

    all_handshake_nodes, nodes_where_handshake_worked = await refresh_nodes.perform_handshakes(initial_nodes, config)

    logger.info("Got handshakes! Getting the contenders from the nodes...")

    if config.refresh_nodes:
        logger.info(f"Storing {len(initial_nodes)} nodes...")
        await refresh_nodes.store_nodes(config, initial_nodes)
        await refresh_nodes.update_our_validator_node(config)

    async with await config.psql_db.connection() as connection:
        logger.info(f"Updating symmetric keys for {len(nodes_where_handshake_worked)} nodes...")
        await insert_symmetric_keys_for_nodes(connection, nodes_where_handshake_worked)

    contenders = await refresh_contenders.get_and_store_contenders(config, nodes_where_handshake_worked)

    logger.info(f"Got all contenders! {len(contenders)} contenders will be queried...")

    return contenders


async def _remove_task_data(config: Config):
    # NOTE: remove on next update
    # For a short time after this update, delete task data since the NSFW flag has changed.
    if datetime.now() < datetime(2024, 10, 14, 15):
        async with await config.psql_db.connection() as connection:
            await delete_task_data_older_than_date(connection, datetime.now())


async def main(config: Config) -> None:
    time_to_sleep_if_no_contenders = 20
    contenders = await get_nodes_and_contenders(config)

    if contenders is None or len(contenders) == 0:
        logger.info(
            f"No contenders to query, skipping synthetic scheduling and sleeping for {time_to_sleep_if_no_contenders} seconds to wait."
        )
        await asyncio.sleep(time_to_sleep_if_no_contenders)  # Sleep for 5 minutes to wait for contenders to become available
        tasks = []
    else:
        tasks = [schedule_synthetics_until_done(config)]

    while True:
        await _remove_task_data(config)
        await asyncio.gather(*tasks)
        contenders = await get_nodes_and_contenders(config)
        if contenders is None or len(contenders) == 0:
            logger.info(
                f"No contenders to query, skipping synthetic scheduling and sleeping for {time_to_sleep_if_no_contenders} seconds to wait."
            )
            await asyncio.sleep(time_to_sleep_if_no_contenders)  # Sleep for 5 minutes to wait for contenders to become available
            tasks = []
        else:
            tasks = [schedule_synthetics_until_done(config)]
