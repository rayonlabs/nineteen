from fiber.logging_utils import get_logger

from asyncpg import Connection
from datetime import datetime
from validator.db.src.database import PSQLDB
from validator.models import Contender, PeriodScore, calculate_period_score
from validator.utils.database import database_constants as dcst
from validator.utils.post.nineteen import ContenderWeightsInfoPostObject, MinerWeightsPostObject

logger = get_logger(__name__)

async def insert_scoring_stats(connection: Connection, scoring_stats: list[ContenderWeightsInfoPostObject]) -> None:
    logger.debug(f"Inserting {len(scoring_stats)} scoring stats")

    await connection.executemany(
        f"""
        INSERT INTO {dcst.CONTENDERS_WEIGHTS_STATS_TABLE} (
            {dcst.VERSION_KEY},
            {dcst.NETUID},
            {dcst.VALIDATOR_HOTKEY},
            {dcst.CREATED_AT},
            {dcst.COLUMN_MINER_HOTKEY},
            {dcst.COLUMN_TASK},
            {dcst.COLUMN_AVERAGE_QUALITY_SCORE},
            {dcst.COLUMN_METRIC_BONUS},
            {dcst.COLUMN_COMBINED_QUALITY_SCORE},
            {dcst.COLUMN_PERIOD_SCORE_MULTIPLIER},
            {dcst.COLUMN_NORMALISED_PERIOD_SCORE},
            {dcst.COLUMN_CONTENDER_CAPACITY},
            {dcst.COLUMN_NORMALISED_NET_SCORE}
        )
        VALUES ($1, $2, $3, NOW(), $4, $5, $6, $7, $8, $9, $10, $11, $12)
        """,
        [
            (
                stat.version_key,
                stat.netuid,
                stat.validator_hotkey,
                stat.miner_hotkey,
                stat.task,
                stat.average_quality_score,
                stat.metric_bonus,
                stat.combined_quality_score,
                stat.period_score_multiplier,
                stat.normalised_period_score,
                stat.contender_capacity,
                stat.normalised_net_score
            )
            for stat in scoring_stats
        ],
    )


async def insert_weights(connection: Connection, miner_weights: list[MinerWeightsPostObject]) -> None:
    logger.debug(f"Inserting {len(miner_weights)} miner weights records")

    await connection.executemany(
        f"""
        INSERT INTO {dcst.NODES_WEIGHTS_TABLE} (
            {dcst.VERSION_KEY},
            {dcst.NETUID},
            {dcst.VALIDATOR_HOTKEY},
            {dcst.CREATED_AT},
            {dcst.COLUMN_MINER_HOTKEY},
            {dcst.COLUMN_WEIGHT}
        )
        VALUES ($1, $2, $3, NOW(), $4, $5)
        """,
        [
            (
                weight_info.version_key,
                weight_info.netuid,
                weight_info.validator_hotkey,
                weight_info.miner_hotkey,
                weight_info.node_weight
            )
            for weight_info in miner_weights
        ],
    )

async def delete_weights_info_older_than(connection: Connection, timestamp: datetime) -> None:
    await connection.execute(
        f"""
        DELETE FROM {dcst.CONTENDERS_WEIGHTS_STATS_TABLE}
        WHERE {dcst.CREATED_AT} < $1
        """,
        timestamp
    )


async def delete_miner_weights_older_than(connection: Connection, timestamp: datetime) -> None:
    await connection.execute(
        f"""
        DELETE FROM {dcst.NODES_WEIGHTS_TABLE}
        WHERE {dcst.CREATED_AT} < $1
        """,
        timestamp
    )