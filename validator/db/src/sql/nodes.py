import datetime
from fiber import SubstrateInterface
from validator.db.src.database import PSQLDB
from fiber.networking.models import NodeWithFernet as Node
from fiber.logging_utils import get_logger

from asyncpg import Connection
from validator.utils.database import database_constants as dcst
from fiber import utils as futils
from cryptography.fernet import Fernet

from validator.utils.substrate.query_substrate import query_substrate

logger = get_logger(__name__)


async def insert_nodes(connection: Connection, nodes: list[Node], network: str) -> None:
    logger.debug(f"Inserting {len(nodes)} nodes into {dcst.NODES_TABLE}...")
    await connection.executemany(
        f"""
        INSERT INTO {dcst.NODES_TABLE} (
            {dcst.HOTKEY},
            {dcst.COLDKEY},
            {dcst.NODE_ID},
            {dcst.INCENTIVE},
            {dcst.NETUID},
            {dcst.STAKE},
            {dcst.TRUST},
            {dcst.VTRUST},
            {dcst.LAST_UPDATED},
            {dcst.IP},
            {dcst.IP_TYPE},
            {dcst.PORT},
            {dcst.PROTOCOL},
            {dcst.NETWORK},
            {dcst.SYMMETRIC_KEY}
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        """,
        [
            (
                node.hotkey,
                node.coldkey,
                node.node_id,
                node.incentive,
                node.netuid,
                node.stake,
                node.trust,
                node.vtrust,
                node.last_updated,
                node.ip,
                node.ip_type,
                node.port,
                node.protocol,
                network,
                None,
            )
            for node in nodes
        ],
    )


async def migrate_nodes_to_history(connection: Connection) -> None:  # noqa: F821
    logger.debug("Migrating NODEs to NODE history")
    await connection.execute(
        f"""
        INSERT INTO {dcst.NODES_HISTORY_TABLE} (
            {dcst.HOTKEY},
            {dcst.COLDKEY},
            {dcst.NODE_ID},
            {dcst.INCENTIVE},
            {dcst.NETUID},
            {dcst.STAKE},
            {dcst.TRUST},
            {dcst.VTRUST},
            {dcst.LAST_UPDATED},
            {dcst.IP},
            {dcst.IP_TYPE},
            {dcst.PORT},
            {dcst.PROTOCOL},
            {dcst.NETWORK},
            {dcst.CREATED_AT}
        )
        SELECT
            {dcst.HOTKEY},
            {dcst.COLDKEY},
            {dcst.NODE_ID},
            {dcst.INCENTIVE},
            {dcst.NETUID},
            {dcst.STAKE},
            {dcst.TRUST},
            {dcst.VTRUST},
            {dcst.LAST_UPDATED},
            {dcst.IP},
            {dcst.IP_TYPE},
            {dcst.PORT},
            {dcst.PROTOCOL},
            {dcst.NETWORK},
            {dcst.CREATED_AT}
        FROM {dcst.NODES_TABLE}
    """
    )

    logger.debug("Truncating NODE info table")
    await connection.execute(f"DELETE FROM {dcst.NODES_TABLE}")


async def get_last_updated_time_for_nodes(connection: Connection, netuid: int) -> datetime.datetime | None:
    query = f"""
        SELECT MAX({dcst.CREATED_AT})
        FROM {dcst.NODES_TABLE}
        WHERE {dcst.NETUID} = $1
    """
    return await connection.fetchval(query, netuid)


async def insert_symmetric_keys_for_nodes(connection: Connection, nodes: list[Node]) -> None:
    logger.info(f"Inserting {len([node for node in nodes if node.fernet is not None])} nodes into {dcst.NODES_TABLE}...")
    await connection.executemany(
        f"""
        UPDATE {dcst.NODES_TABLE}
        SET {dcst.SYMMETRIC_KEY} = $1, {dcst.SYMMETRIC_KEY_UUID} = $2
        WHERE {dcst.HOTKEY} = $3 and {dcst.NETUID} = $4
        """,
        [
            (futils.fernet_to_symmetric_key(node.fernet), node.symmetric_key_uuid, node.hotkey, node.netuid)
            for node in nodes
            if node.fernet is not None
        ],
    )


async def get_nodes(psql_db: PSQLDB, netuid: int) -> list[Node]:
    query = f"""
        SELECT 
            {dcst.HOTKEY},
            {dcst.COLDKEY},
            {dcst.NODE_ID},
            {dcst.INCENTIVE},
            {dcst.NETUID},
            {dcst.STAKE},
            {dcst.TRUST},
            {dcst.VTRUST},
            {dcst.LAST_UPDATED},
            {dcst.IP},
            {dcst.IP_TYPE},
            {dcst.PORT},
            {dcst.PROTOCOL}
        FROM {dcst.NODES_TABLE}
        WHERE {dcst.NETUID} = $1
    """

    nodes = await psql_db.fetchall(query, netuid)

    return [Node(**node) for node in nodes]


async def get_node_stakes(psql_db: PSQLDB, netuid: int) -> dict[str, float]:
    NODEs = await psql_db.fetchall(
        f"""
        SELECT {dcst.HOTKEY}, {dcst.STAKE}
        FROM {dcst.NODES_TABLE}
        WHERE {dcst.NETUID} = $1
        """,
        netuid,
    )
    hotkey_to_stake = {NODE[dcst.HOTKEY]: NODE[dcst.STAKE] for NODE in NODEs}

    return hotkey_to_stake


async def get_node(psql_db: PSQLDB, node_id: int, netuid: int) -> Node | None:
    query = f"""
        SELECT 
            {dcst.HOTKEY},
            {dcst.COLDKEY},
            {dcst.NODE_ID},
            {dcst.INCENTIVE},
            {dcst.NETUID},
            {dcst.STAKE},
            {dcst.TRUST},
            {dcst.VTRUST},
            {dcst.LAST_UPDATED},
            {dcst.IP},
            {dcst.IP_TYPE},
            {dcst.PORT},
            {dcst.PROTOCOL},
            {dcst.SYMMETRIC_KEY},
            {dcst.SYMMETRIC_KEY_UUID}
        FROM {dcst.NODES_TABLE}
        WHERE {dcst.NODE_ID} = $1 AND {dcst.NETUID} = $2
    """

    node = await psql_db.fetchone(query, node_id, netuid)

    if node is None:
        logger.error(f"No node found for node id {node_id} and netuid {netuid}")
        logger.error(f"all nodes: {await psql_db.fetchall(f'SELECT * FROM {dcst.NODES_TABLE} WHERE {dcst.NETUID} = $1', netuid)}")
        raise ValueError(f"No node found for node id {node_id} and netuid {netuid}")
    try:
        node["fernet"] = Fernet(node[dcst.SYMMETRIC_KEY])
    except Exception as e:
        logger.error(f"Error creating fernet: {e}")
        logger.error(f"node: {node}")
        return None
    return Node(**node)


async def update_our_vali_node_in_db(connection: Connection, ss58_address: str, netuid: int) -> None:
    query = f"""
        UPDATE {dcst.NODES_TABLE}
        SET {dcst.OUR_VALIDATOR} = true
        WHERE {dcst.HOTKEY} = $1 AND {dcst.NETUID} = $2
    """
    await connection.execute(query, ss58_address, netuid)


async def get_vali_ss58_address(psql_db: PSQLDB, netuid: int) -> str | None:
    query = f"""
        SELECT 
            {dcst.HOTKEY}
        FROM {dcst.NODES_TABLE}
        WHERE {dcst.OUR_VALIDATOR} = true AND {dcst.NETUID} = $1
    """

    node = await psql_db.fetchone(query, netuid)

    if node is None:
        logger.error(f"I cannot find the validator node for netuid {netuid} in the DB. Maybe control node is still syncing?")
        return None

    return node[dcst.HOTKEY]


async def get_vali_node_id(substrate: SubstrateInterface, netuid: int, ss58_address: str) -> str | None:
    _, uid = query_substrate(
        substrate, "SubtensorModule", "Uids", [netuid, ss58_address], return_value=True
    )
    return uid