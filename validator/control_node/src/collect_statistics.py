import asyncio
from validator.control_node.src.control_config import Config, load_config
from asyncpg import Connection

from validator.utils.database import database_constants as dcst
from validator.utils.substrate.query_substrate import query_substrate
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


async def _get_number_of_nodes(connection: Connection, netuid: int):
    query = f"SELECT COUNT(*) FROM {dcst.NODES_TABLE} WHERE {dcst.NETUID} = $1"
    return await connection.fetchval(query, netuid)


async def _get_number_of_contenders(connection: Connection, netuid: int):
    query = f"SELECT COUNT(*) FROM {dcst.CONTENDERS_TABLE} WHERE {dcst.NETUID} = $1"
    return await connection.fetchval(query, netuid)


async def _get_number_of_rows_in_contender_history(connection: Connection, netuid: int):
    query = f"SELECT COUNT(*) FROM {dcst.CONTENDERS_HISTORY_TABLE} WHERE {dcst.NETUID} = $1"
    return await connection.fetchval(query, netuid)


async def _get_number_of_rows_in_reward_table(connection: Connection):
    query = f"SELECT COUNT(*) FROM {dcst.TABLE_REWARD_DATA}"
    return await connection.fetchval(query)


async def _get_number_of_unique_hotkeys_in_the_contestants_table(connection: Connection, netuid: int):
    query = f"SELECT COUNT(DISTINCT {dcst.NODE_HOTKEY}) FROM {dcst.CONTENDERS_TABLE} WHERE {dcst.NETUID} = $1"
    return await connection.fetchval(query, netuid)


async def _get_number_of_unique_hotkeys_in_the_reward_table(connection: Connection):
    query = f"SELECT COUNT(DISTINCT {dcst.NODE_HOTKEY}) FROM {dcst.TABLE_REWARD_DATA}"
    return await connection.fetchval(query)


async def _get_number_of_unique_hotkeys_in_the_contestants_history_table(connection: Connection, netuid: int):
    query = f"SELECT COUNT(DISTINCT {dcst.NODE_HOTKEY}) FROM {dcst.CONTENDERS_HISTORY_TABLE} WHERE {dcst.NETUID} = $1"
    return await connection.fetchval(query, netuid)


async def _get_number_of_requests_sent_out_in_the_current_period(connection: Connection, netuid: int):
    query = f"SELECT sum({dcst.TOTAL_REQUESTS_MADE}) FROM {dcst.CONTENDERS_TABLE} WHERE {dcst.NETUID} = $1"
    return await connection.fetchval(query, netuid)


async def _get_last_updated_value(config: Config):
    substrate, uid = query_substrate(
        config.substrate, "SubtensorModule", "Uids", [config.netuid, config.keypair.ss58_address], return_value=True
    )

    substrate, current_block = query_substrate(config.substrate, "System", "Number", [], return_value=True)
    substrate, last_updated_value = query_substrate(
        substrate, "SubtensorModule", "LastUpdate", [config.netuid], return_value=False
    )
    updated: float = current_block - last_updated_value[uid].value
    return updated


async def main():
    config = load_config()
    await config.psql_db.connect()

    async with await config.psql_db.connection() as connection:
        number_of_nodes = await _get_number_of_nodes(connection, config.netuid)
        number_of_contenders = await _get_number_of_contenders(connection, config.netuid)
        number_of_rows_in_contender_history = await _get_number_of_rows_in_contender_history(connection, config.netuid)
        number_of_rows_in_reward_table = await _get_number_of_rows_in_reward_table(connection)
        number_of_unique_hotkeys_in_the_contestants_table = await _get_number_of_unique_hotkeys_in_the_contestants_table(
            connection, config.netuid
        )
        number_of_unique_hotkeys_in_the_reward_table = await _get_number_of_unique_hotkeys_in_the_reward_table(connection)
        number_of_unique_hotkeys_in_the_contestants_history_table = (
            await _get_number_of_unique_hotkeys_in_the_contestants_history_table(connection, config.netuid)
        )
        number_of_requests_sent_out_in_the_current_period = await _get_number_of_requests_sent_out_in_the_current_period(
            connection, config.netuid
        )
        last_updated_value = await _get_last_updated_value(config)

    logger.info(
        f"Some statistics about the validator running:"
        f"\nNumber of nodes: {number_of_nodes}"
        f"\nNumber of contenders: {number_of_contenders}"
        f"\nNumber of rows in contender history: {number_of_rows_in_contender_history}"
        f"\nNumber of rows in reward table: {number_of_rows_in_reward_table}"
        f"\nNumber of unique hotkeys in the contestants table: {number_of_unique_hotkeys_in_the_contestants_table}"
        f"\nNumber of unique hotkeys in the reward table: {number_of_unique_hotkeys_in_the_reward_table}"
        f"\nNumber of unique hotkeys in the contestants history table: {number_of_unique_hotkeys_in_the_contestants_history_table}"
        f"\nNumber of requests sent out in the current period: {number_of_requests_sent_out_in_the_current_period}"
        f"\nLast updated value: {last_updated_value}"
    )


if __name__ == "__main__":
    asyncio.run(main())
