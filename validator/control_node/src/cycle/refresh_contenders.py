"""
Calculates period scores for contenders
Converts NODEs to contenders by querying them for their tasks. (NODE + Task = Contender)
Migrates old contenders and adds the new contenders to the db
"""

import asyncio
import random
from typing import List


from validator.db.src.sql.contenders import (
    fetch_all_contenders,
    migrate_contenders_to_contender_history,
    insert_contenders,
    update_contenders_period_scores,
)
from validator.models import Contender
from fiber.networking.models import NodeWithFernet as Node
from core import task_config as tcfg
from validator.control_node.src.control_config import Config
from fiber.logging_utils import get_logger
from core import constants as cst
from fiber.validator import client

from validator.utils.post.nineteen import (
    ContenderPayload,
    DataTypeToPost,
    MinerCapacitiesPostObject,
    MinerTypesPostBody,
    post_to_nineteen_ai,
)

logger = get_logger(__name__)


def _get_capacity_to_score(capacity: float, capacity_to_score_multiplier: float) -> float:
    if random.random() < 0.5:
        multiplier = 0.04
    elif random.random() < 0.8:
        multiplier = 0.08
    elif random.random() < 0.95:
        multiplier = 0.12
    else:
        multiplier = 0.8

    return capacity * multiplier * capacity_to_score_multiplier


async def _store_and_migrate_old_contenders(config: Config, new_contenders: List[Contender]):
    logger.info("Calculating period scores & refreshing contenders")
    async with await config.psql_db.connection() as connection:
        await update_contenders_period_scores(connection, config.netuid)
        await migrate_contenders_to_contender_history(connection)
        await insert_contenders(connection, new_contenders, config.keypair.ss58_address)


async def _fetch_node_capacity(config: Config, node: Node) -> dict[str, float] | None:
    server_address = client.construct_server_address(
        node=node,
        replace_with_docker_localhost=config.replace_with_docker_localhost,
        replace_with_localhost=config.replace_with_localhost,
    )
    public_configs = tcfg.get_public_task_configs()
    payload = {"task_configs": public_configs}
    assert node.symmetric_key_uuid is not None
    try:
        response = await client.make_non_streamed_post(
            httpx_client=config.httpx_client,
            server_address=server_address,
            validator_ss58_address=config.keypair.ss58_address,
            miner_ss58_address=node.hotkey,
            keypair=config.keypair,
            fernet=node.fernet,
            symmetric_key_uuid=node.symmetric_key_uuid,
            endpoint="/capacity",
            payload=payload,
            timeout=10,
        )
    except Exception as e:
        logger.error(f"Failed to fetch capacity from node {node.node_id}: {e}")
        return None

    if response.status_code != 200:
        logger.warning(f"Failed to fetch capacity from node {node.node_id}")
        return None

    return response.json()


async def _fetch_node_capacities(config: Config, nodes: list[Node]) -> list[dict[str, float] | None]:
    async def _fetch_or_none(node: Node):
        if node.fernet is None or node.symmetric_key_uuid is None:
            return None
        return await _fetch_node_capacity(config, node)

    capacity_tasks = []
    capacities = []
    for node in nodes:
        capacity_tasks.append(_fetch_or_none(node))
        if len(capacity_tasks) > 50:
            capacities.extend(await asyncio.gather(*capacity_tasks))
            capacity_tasks = []

    if capacity_tasks:
        capacities.extend(await asyncio.gather(*capacity_tasks))

    return capacities


async def _get_contenders_from_nodes(config: Config, nodes: list[Node]) -> List[Contender]:
    raw_capacities = await _fetch_node_capacities(config, nodes)
    task_configs = tcfg.get_task_configs()
    logger.info(f"Got capacities for {len([i for i in raw_capacities if i is not None])} nodes")

    miner_types = {}

    contenders = []
    for node, raw_node_capacities in zip(nodes, raw_capacities):
        if raw_node_capacities is None:
            continue
        node_hotkey = node.hotkey
        node_id = node.node_id
        netuid = node.netuid

        if cst.MINER_TYPE not in raw_node_capacities:
            logger.warning(f"Node {node_id} did not return a miner type")
            continue

        miner_type = raw_node_capacities[cst.MINER_TYPE]
        del raw_node_capacities[cst.MINER_TYPE]

        miner_types[node_hotkey] = miner_type

        for task, declared_capacity in raw_node_capacities.items():
            if task not in task_configs:
                logger.debug(f"Task {task} is not a valid task")
                continue

            task_config = tcfg.get_enabled_task_config(task)
            if task_config is None or task_config.task_type.value != miner_type:
                continue
            # NOTE: Change here. No longer use validator stake proportion. Let miners decide their own capacity.
            capacity = min(max(declared_capacity, 0), task_config.max_capacity)
            capacity_to_score = _get_capacity_to_score(capacity, config.capacity_to_score_multiplier)

            contenders.append(
                Contender(
                    node_hotkey=node_hotkey,
                    node_id=node_id,
                    netuid=netuid,
                    task=task,
                    raw_capacity=declared_capacity,
                    capacity=capacity,
                    capacity_to_score=capacity_to_score,
                    consumed_capacity=0,
                    total_requests_made=0,
                    requests_429=0,
                    requests_500=0,
                    period_score=None,
                )
            )

    # Post miner types to nineteen
    miner_types_payload = [
        MinerTypesPostBody(
            miner_hotkey=node_hotkey,
            validator_hotkey=config.keypair.ss58_address,
            miner_type=miner_type,
        ).model_dump(mode="json")
        for node_hotkey, miner_type in miner_types.items()
    ]
    await post_to_nineteen_ai(
        data_to_post=miner_types_payload, keypair=config.keypair, data_type_to_post=DataTypeToPost.MINER_TYPES, timeout=10
    )

    logger.info(f"Got {len(contenders)} contenders to score")
    return contenders


async def _post_contender_stats_to_nineteen(config: Config):
    async with await config.psql_db.connection() as connection:
        all_contenders = await fetch_all_contenders(connection, config.netuid)
    contender_payloads = []
    capacity_payloads = []
    for contender in all_contenders:
        contender_payloads.append(
            ContenderPayload(
                node_id=contender.node_id,
                node_hotkey=contender.node_hotkey,
                validator_hotkey=config.keypair.ss58_address,
                task=contender.task,
                declared_volume=contender.raw_capacity,
                consumed_volume=contender.consumed_capacity,
                total_requests_made=contender.total_requests_made,
                requests_429=contender.requests_429,
                requests_500=contender.requests_500,
            ).model_dump(mode="json")
        )
        capacity_payloads.append(
            MinerCapacitiesPostObject(
                miner_hotkey=contender.node_hotkey,
                validator_hotkey=config.keypair.ss58_address,
                task=contender.task,
                volume=contender.capacity,
            ).model_dump(mode="json")
        )
    await post_to_nineteen_ai(
        data_to_post=contender_payloads, keypair=config.keypair, data_type_to_post=DataTypeToPost.UID_RECORD, timeout=10
    )
    await post_to_nineteen_ai(
        data_to_post=capacity_payloads, keypair=config.keypair, data_type_to_post=DataTypeToPost.MINER_CAPACITIES, timeout=10
    )


async def get_and_store_contenders(config: Config, nodes: list[Node]) -> list[Contender]:
    logger.info(f"Got {len(nodes)} nodes to get contenders from...")

    contenders = await _get_contenders_from_nodes(config, nodes)
    await _store_and_migrate_old_contenders(config, contenders)

    await _post_contender_stats_to_nineteen(config)
    # NOTE: Could also add a feature here which deletes everything from
    # contender history for a node which doesn't align with their miner_type.
    # This would prevent changing miner_types - though there is no benefit to that
    # anyway
    return contenders
