import datetime
from validator.db.database import PSQLDB
from core.bittensor_overrides import chain_data
from core.logging import get_logger


from validator.utils import database_constants as dcst


logger = get_logger(__name__)


# TODO: Include history on here and
# e.g. created_at, expired_at
# And have some cron to clean it if we're worried about size
# Basiaclly's insanely useful for debugging to  be
# able to get a snapshot at any point in the history
async def insert_axon_info(psql_db: PSQLDB, axon_info: chain_data.AxonInfo) -> None:
    logger.debug("Inserting axon info")
    await psql_db.execute(
        f"""
        INSERT INTO {dcst.AXON_INFO_TABLE} (
            {dcst.HOTKEY},
            {dcst.COLDKEY}
            {dcst.VERSION},
            {dcst.IP},
            {dcst.PORT},
            {dcst.IP_TYPE},
            {dcst.AXON_UID},
            {dcst.CREATED_AT}
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
        """,
        axon_info.hotkey,
        axon_info.coldkey,
        axon_info.version,
        axon_info.ip,
        axon_info.port,
        axon_info.ip_type,
        axon_info.axon_uid,
    )


async def get_axons(psql_db: PSQLDB) -> list[chain_data.AxonInfo]:
    axons = await psql_db.fetch_all(
        f"SELECT {dcst.HOTKEY}, {dcst.COLDKEY}, {dcst.VERSION},"
        f"{dcst.IP}, {dcst.PORT}, {dcst.IP_TYPE}, {dcst.AXON_UID}"
        f" FROM {dcst.AXON_INFO_TABLE}"
    )
    return [chain_data.AxonInfo(**axon) for axon in axons]


# TODO: Do we even need this? If we only need UIDS for setting weights
# We can just query the metagraph for these surely.

# from models.utility_models import HotkeyInfo
# async def insert_hotkey_info(psql_db: PSQLDB, hotkey_info: HotkeyInfo) -> None:
#     await psql_db.execute(
#         f"""
#         INSERT INTO {dcst.HOTKEY_INFO_TABLE} (
#             {dcst.HOTKEY},
#             {dcst.UID}
#         )
#         VALUES ($1, $2)
#         """,
#         hotkey_info.hotkey,
#         hotkey_info.uid,
#     )
