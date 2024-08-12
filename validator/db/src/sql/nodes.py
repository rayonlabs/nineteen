from validator.db.src.database import PSQLDB
from fiber.chain_interactions.models import Node
from core.logging import get_logger

from asyncpg import Connection
from validator.utils import database_constants as dcst


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
            {dcst.IP},
            {dcst.IP_TYPE},
            {dcst.PORT},
            {dcst.PROTOCOL},
            {dcst.NETWORK},
            {dcst.SYMMETRIC_KEY}
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
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
    logger.debug("Migrating axons to axon history")
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
            {dcst.IP},
            {dcst.IP_TYPE},
            {dcst.PORT},
            {dcst.PROTOCOL},
            {dcst.NETWORK},
            {dcst.CREATED_AT}
        FROM {dcst.NODES_TABLE}
    """
    )

    logger.debug("Truncating axon info table")
    await connection.execute(f"DELETE FROM {dcst.NODES_TABLE}")


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
            {dcst.IP},
            {dcst.IP_TYPE},
            {dcst.PORT},
            {dcst.PROTOCOL}
        FROM {dcst.NODES_TABLE}
        WHERE {dcst.NETUID} = $1
    """

    nodes = await psql_db.fetchall(query, netuid)

    return [Node(**node) for node in nodes]


# TODO: Do I need to add network there too?
async def get_node_stakes(psql_db: PSQLDB, netuid: int) -> dict[str, float]:
    axons = await psql_db.fetchall(
        f"""
        SELECT {dcst.HOTKEY}, {dcst.STAKE}
        FROM {dcst.NODES_TABLE}
        WHERE {dcst.NETUID} = $1
        """,
        netuid,
    )
    hotkey_to_stake = {axon[dcst.HOTKEY]: axon[dcst.STAKE] for axon in axons}

    return hotkey_to_stake


async def get_node(psql_db: PSQLDB, hotkey: str, netuid: int) -> Node:
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
            {dcst.IP},
            {dcst.IP_TYPE},
            {dcst.PORT},
            {dcst.PROTOCOL}
        FROM {dcst.NODES_TABLE}
        WHERE {dcst.HOTKEY} = $1 AND {dcst.NETUID} = $2
    """

    node = await psql_db.fetchone(query, hotkey, netuid)

    if node is None:
        raise ValueError(f"No node found for hotkey {hotkey} and netuid {netuid}")

    return Node(**node)
