from fiber.logging_utils import get_logger

from asyncpg import Connection
import random
import numpy as np

from validator.db.src.database import PSQLDB
from validator.models import Contender, PeriodScore, calculate_period_score
from validator.utils.database import database_constants as dcst
from validator.utils.generic import generic_constants as gcst
from validator.utils.database import database_constants as dcst

logger = get_logger(__name__)


async def insert_contenders(connection: Connection, contenders: list[Contender], validator_hotkey: str) -> None:
    logger.debug(f"Inserting {len(contenders)} contender records")

    await connection.executemany(
        f"""
        INSERT INTO {dcst.CONTENDERS_TABLE} (
            {dcst.CONTENDER_ID},
            {dcst.NODE_HOTKEY},
            {dcst.NODE_ID},
            {dcst.NETUID},
            {dcst.TASK},
            {dcst.VALIDATOR_HOTKEY},
            {dcst.CAPACITY},
            {dcst.RAW_CAPACITY},
            {dcst.CAPACITY_TO_SCORE},
            {dcst.CONSUMED_CAPACITY},
            {dcst.TOTAL_REQUESTS_MADE},
            {dcst.REQUESTS_429},
            {dcst.REQUESTS_500},
            {dcst.CREATED_AT},
            {dcst.UPDATED_AT}
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW(), NOW())
        """,
        [
            (
                contender.id,
                contender.node_hotkey,
                contender.node_id,
                contender.netuid,
                contender.task,
                validator_hotkey,
                contender.capacity,
                contender.raw_capacity,
                contender.capacity_to_score,
                contender.consumed_capacity,
                contender.total_requests_made,
                contender.requests_429,
                contender.requests_500,
            )
            for contender in contenders
        ],
    )


async def migrate_contenders_to_contender_history(connection: Connection) -> None:
    await connection.execute(
        f"""
        INSERT INTO {dcst.CONTENDERS_HISTORY_TABLE} (
            {dcst.CONTENDER_ID},
            {dcst.NODE_HOTKEY},
            {dcst.NODE_ID},
            {dcst.NETUID},
            {dcst.TASK},
            {dcst.VALIDATOR_HOTKEY},
            {dcst.CAPACITY},
            {dcst.RAW_CAPACITY},
            {dcst.CAPACITY_TO_SCORE},
            {dcst.CONSUMED_CAPACITY},
            {dcst.TOTAL_REQUESTS_MADE},
            {dcst.REQUESTS_429},
            {dcst.REQUESTS_500},
            {dcst.PERIOD_SCORE},
            {dcst.CREATED_AT},
            {dcst.UPDATED_AT}
        )
        SELECT
            {dcst.CONTENDER_ID},
            {dcst.NODE_HOTKEY},
            {dcst.NODE_ID},
            {dcst.NETUID},
            {dcst.TASK},
            {dcst.VALIDATOR_HOTKEY},
            {dcst.CAPACITY},
            {dcst.RAW_CAPACITY},
            {dcst.CAPACITY_TO_SCORE},
            {dcst.CONSUMED_CAPACITY},
            {dcst.TOTAL_REQUESTS_MADE},
            {dcst.REQUESTS_429},
            {dcst.REQUESTS_500},
            {dcst.PERIOD_SCORE},
            {dcst.CREATED_AT},
            {dcst.UPDATED_AT}
        FROM {dcst.CONTENDERS_TABLE}
        """
    )

    await connection.execute(f"DELETE FROM {dcst.CONTENDERS_TABLE}")


async def get_contenders_for_synthetic_task(psql_db: PSQLDB, task: str, top_x: int = 5 )-> list[Contender]:
    async with await psql_db.connection() as connection:
        rows = await connection.fetch(
            f"""
            WITH ranked_contenders AS (
                SELECT 
                    c.{dcst.CONTENDER_ID}, c.{dcst.NODE_HOTKEY}, c.{dcst.NODE_ID}, c.{dcst.TASK},
                    c.{dcst.RAW_CAPACITY}, c.{dcst.CAPACITY_TO_SCORE}, c.{dcst.CONSUMED_CAPACITY},
                    c.{dcst.TOTAL_REQUESTS_MADE}, c.{dcst.REQUESTS_429}, c.{dcst.REQUESTS_500}, 
                    c.{dcst.CAPACITY}, c.{dcst.PERIOD_SCORE}, c.{dcst.NETUID},
                    ROW_NUMBER() OVER (
                        ORDER BY c.{dcst.TOTAL_REQUESTS_MADE} ASC
                    ) AS rank
                FROM {dcst.CONTENDERS_TABLE} c
                JOIN {dcst.NODES_TABLE} n ON c.{dcst.NODE_ID} = n.{dcst.NODE_ID} AND c.{dcst.NETUID} = n.{dcst.NETUID}
                WHERE c.{dcst.TASK} = $1 
                AND  c.{dcst.CONSUMED_CAPACITY} < c.{dcst.CAPACITY}
                AND n.{dcst.SYMMETRIC_KEY_UUID} IS NOT NULL
            )
            SELECT *
            FROM ranked_contenders
            WHERE rank <= $2
            ORDER BY rank
            """,
            task,
            top_x,
        )

        # If not enough rows are returned, run another query to get more contenders
        if not rows or len(rows) < top_x:
            additional_rows = await connection.fetch(
                f"""
                SELECT 
                    c.{dcst.CONTENDER_ID}, c.{dcst.NODE_HOTKEY}, c.{dcst.NODE_ID}, c.{dcst.TASK},
                    c.{dcst.RAW_CAPACITY}, c.{dcst.CAPACITY_TO_SCORE}, c.{dcst.CONSUMED_CAPACITY},
                    c.{dcst.TOTAL_REQUESTS_MADE}, c.{dcst.REQUESTS_429}, c.{dcst.REQUESTS_500}, 
                    c.{dcst.CAPACITY}, c.{dcst.PERIOD_SCORE}, c.{dcst.NETUID}
                FROM {dcst.CONTENDERS_TABLE} c
                JOIN {dcst.NODES_TABLE} n ON c.{dcst.NODE_ID} = n.{dcst.NODE_ID} AND c.{dcst.NETUID} = n.{dcst.NETUID}
                WHERE c.{dcst.TASK} = $1 
                AND c.{dcst.CAPACITY} > 0 
                AND n.{dcst.SYMMETRIC_KEY_UUID} IS NOT NULL
                ORDER BY c.{dcst.TOTAL_REQUESTS_MADE} ASC
                LIMIT $2
                OFFSET $3
                """,
                task,
                top_x - len(rows) if rows else top_x,
                len(rows) if rows else 0,
            )
            rows = rows + additional_rows if rows else additional_rows

    return [Contender(**row) for row in rows]
    

async def get_contenders_for_organic_task(psql_db: PSQLDB, task: str, top_x: int = 5) -> list[Contender]:

    def create_weight_distribution(total_length, top_percentage=0.25):
        top_count = max(1, int(top_percentage * total_length))
        remaining_count = total_length - top_count
        
        weights = []
        for i in range(top_count):
            weight = gcst.ORGANIC_TOP_POURC_FACTOR #top_percentage% of entries get same weight to favor top contenders uniformly keeping heavy traffic in mind 
            weights.append(weight)
        
        for i in range(remaining_count):
            weight = gcst.ORGANIC_TOP_POURC_FACTOR  ** (1 - (i + 1)/remaining_count)
            weights.append(weight)
        
        return weights
    
    def weighted_order_preserving_selection(contenders, top_x):
        if len(contenders) <= top_x:
            return contenders
        
        weights = create_weight_distribution(len(contenders))
        
        available_entries = [(idx, weights[idx]) for idx in range(len(contenders))]
        selected_indices = []
        
        total_weight = sum(weight for _, weight in available_entries)
        probabilities = [weight / total_weight for _, weight in available_entries]
        
        selected_entry_indices = random.choices(
            range(len(available_entries)), 
            weights=probabilities, 
            k=min(top_x, len(available_entries))
        )
        
        # Sort indices to preserve original order, as we query `contenders_to_query` in the same order, in order to query the best shortlisted contender first 
        selected_indices = sorted(available_entries[idx][0] for idx in selected_entry_indices)
        
        return [contenders[idx] for idx in selected_indices]

    async with await psql_db.connection() as connection:
        rows = await connection.fetch(
            f"""
            WITH ranked_contenders AS (
                SELECT DISTINCT ON (c.{dcst.NODE_HOTKEY}, c.{dcst.TASK})
                    c.{dcst.CONTENDER_ID}, c.{dcst.NODE_HOTKEY}, c.{dcst.NODE_ID}, c.{dcst.TASK},
                    c.{dcst.RAW_CAPACITY}, c.{dcst.CAPACITY_TO_SCORE}, c.{dcst.CONSUMED_CAPACITY},
                    c.{dcst.TOTAL_REQUESTS_MADE}, c.{dcst.REQUESTS_429}, c.{dcst.REQUESTS_500}, 
                    c.{dcst.CAPACITY}, c.{dcst.PERIOD_SCORE}, c.{dcst.NETUID}
                FROM {dcst.CONTENDERS_TABLE} c
                JOIN {dcst.CONTENDERS_WEIGHTS_STATS_TABLE} s ON c.{dcst.NODE_HOTKEY} = s.{dcst.NODE_HOTKEY} 
                    AND c.{dcst.TASK} = s.{dcst.TASK}
                WHERE c.{dcst.TASK} = $1 
                AND c.{dcst.CONSUMED_CAPACITY} < c.{dcst.CAPACITY}  
                AND n.{dcst.SYMMETRIC_KEY_UUID} IS NOT NULL
                ORDER BY s.{dcst.COLUMN_NORMALISED_NET_SCORE} DESC, s.{dcst.COLUMN_METRIC_BONUS} DESC, s.{dcst.CREATED_AT} DESC
            )
            SELECT *
            FROM ranked_contenders
            """,
            task,
        )
    logger.debug(f"Number of valid contenders for task {task} for organic query : {len(rows)}")

    contenders = [Contender(**row) for row in rows]
    
    if contenders:
        if len(contenders) > top_x:
            selected_contenders = weighted_order_preserving_selection(contenders, top_x)
            logger.debug(f"Selected contenders for task {task}: {selected_contenders}")
            return selected_contenders
        else:
            logger.debug(f"Number of contenders ({len(contenders)}) < top_x ({top_x}). Returning all contenders")
            return contenders
    else:
        logger.debug(f"Contenders selection for organic queries with task {task} yielded nothing (probably statistiques table is empty), falling back to synthetic queries logic.")
        return await get_contenders_for_synthetic_task(psql_db, task, top_x)



async def get_contenders_for_task(psql_db: PSQLDB, task: str, top_x: int = 5, 
                                  query_type: str = gcst.SYNTHETIC) -> list[Contender]:
    if query_type == gcst.SYNTHETIC:
        return await get_contenders_for_synthetic_task(psql_db, task, top_x)
    elif query_type == gcst.ORGANIC:
        return await get_contenders_for_organic_task(psql_db, task, top_x)
    else:
        raise ValueError(f"No contender selection strategy have been implemented for query type : {query_type}")
    

async def update_contender_capacities(psql_db: PSQLDB, contender: Contender, capacitity_consumed: float) -> None:
    async with await psql_db.connection() as connection:
        await connection.execute(
            f"""
            UPDATE {dcst.CONTENDERS_TABLE}
            SET {dcst.CONSUMED_CAPACITY} = {dcst.CONSUMED_CAPACITY} + $1
            WHERE {dcst.CONTENDER_ID} = $2
            """,
            capacitity_consumed,
            contender.id,
        )


async def update_total_requests_made(psql_db: PSQLDB, contender: Contender) -> None:
    async with await psql_db.connection() as connection:
        await connection.execute(
            f"""
            UPDATE {dcst.CONTENDERS_TABLE}
            SET {dcst.TOTAL_REQUESTS_MADE} = {dcst.TOTAL_REQUESTS_MADE} + 1
            WHERE {dcst.CONTENDER_ID} = $1
            """,
            contender.id,
        )

async def update_contender_429_count(psql_db: PSQLDB, contender: Contender) -> None:
    async with await psql_db.connection() as connection:
        await connection.execute(
            f"""
            UPDATE {dcst.CONTENDERS_TABLE}
            SET {dcst.REQUESTS_429} = {dcst.REQUESTS_429} + 1
            WHERE {dcst.CONTENDER_ID} = $1
            """,
            contender.id,
        )


async def update_contender_500_count(psql_db: PSQLDB, contender: Contender) -> None:
    async with await psql_db.connection() as connection:
        await connection.execute(
            f"""
            UPDATE {dcst.CONTENDERS_TABLE}
            SET {dcst.REQUESTS_500} = {dcst.REQUESTS_500} + 1
            WHERE {dcst.CONTENDER_ID} = $1
            """,
            contender.id,
        )


async def fetch_contender(connection: Connection, contender_id: str) -> Contender | None:
    row = await connection.fetchrow(
        f"""
        SELECT 
            {dcst.CONTENDER_ID}, {dcst.NODE_HOTKEY}, {dcst.NODE_ID},{dcst.TASK},
            {dcst.CAPACITY}, {dcst.RAW_CAPACITY}, {dcst.CAPACITY_TO_SCORE},
             {dcst.CONSUMED_CAPACITY}, {dcst.TOTAL_REQUESTS_MADE}, {dcst.REQUESTS_429}, {dcst.REQUESTS_500}, 
            {dcst.PERIOD_SCORE}
        FROM {dcst.CONTENDERS_TABLE} 
        WHERE {dcst.CONTENDER_ID} = $1
        """,
        contender_id,
    )
    if not row:
        return None
    return Contender(**row)


async def fetch_all_contenders(connection: Connection, netuid: int | None = None) -> list[Contender]:
    base_query = f"""
        SELECT 
            {dcst.CONTENDER_ID}, {dcst.NODE_HOTKEY}, {dcst.NODE_ID}, {dcst.NETUID}, {dcst.TASK}, 
            {dcst.RAW_CAPACITY}, {dcst.CAPACITY_TO_SCORE}, {dcst.CONSUMED_CAPACITY}, 
            {dcst.TOTAL_REQUESTS_MADE}, {dcst.REQUESTS_429}, {dcst.REQUESTS_500}, 
            {dcst.CAPACITY}, {dcst.PERIOD_SCORE}
        FROM {dcst.CONTENDERS_TABLE}
        """
    if netuid is None:
        rows = await connection.fetch(base_query)
    else:
        rows = await connection.fetch(base_query + f" WHERE {dcst.NETUID} = $1", netuid)
    return [Contender(**row) for row in rows]


async def fetch_hotkey_scores_for_task(connection: Connection, task: str, node_hotkey: str) -> list[PeriodScore]:
    rows = await connection.fetch(
        f"""
        SELECT
            {dcst.NODE_HOTKEY} as hotkey,
            {dcst.TASK},
            {dcst.PERIOD_SCORE},
            {dcst.CONSUMED_CAPACITY},
            {dcst.CREATED_AT}
        FROM {dcst.CONTENDERS_HISTORY_TABLE}
        WHERE {dcst.TASK} = $1
        AND {dcst.NODE_HOTKEY} = $2
        ORDER BY {dcst.CREATED_AT} DESC
        """,
        task,
        node_hotkey,
    )
    return [PeriodScore(**row) for row in rows]


async def update_contenders_period_scores(connection: Connection, netuid: int) -> None:
    rows = await connection.fetch(
        f"""
        SELECT 
            {dcst.CONTENDER_ID},
            {dcst.TOTAL_REQUESTS_MADE},
            {dcst.CAPACITY},
            {dcst.CONSUMED_CAPACITY},
            {dcst.REQUESTS_429},
            {dcst.REQUESTS_500}
        FROM {dcst.CONTENDERS_TABLE}
        WHERE {dcst.NETUID} = $1
    """,
        netuid,
    )

    updates = []
    for row in rows:
        score = calculate_period_score(
            float(row[dcst.TOTAL_REQUESTS_MADE]),
            float(row[dcst.CAPACITY]),
            float(row[dcst.CONSUMED_CAPACITY]),
            float(row[dcst.REQUESTS_429]),
            float(row[dcst.REQUESTS_500]),
        )
        if score is not None:
            updates.append((score, row[dcst.CONTENDER_ID]))

    logger.info(f"Updating {len(updates)} contenders with new period scores")

    await connection.executemany(
        f"""
        UPDATE {dcst.CONTENDERS_TABLE}
        SET {dcst.PERIOD_SCORE} = $1,
            {dcst.UPDATED_AT} = NOW() AT TIME ZONE 'UTC'
        WHERE {dcst.CONTENDER_ID} = $2
    """,
        updates,
    )
    logger.info(f"Updated {len(updates)} contenders with new period scores")


async def get_and_decrement_synthetic_request_count(connection: Connection, contender_id: str) -> int | None:
    """
    Asynchronously retrieves and decrements the synthetic request count for a given contender, setting it to 0 if
    it the consumed capacity is greater than the announced capacity.
    """

    result = await connection.fetchrow(
        f"""
        UPDATE {dcst.CONTENDERS_TABLE}
        SET {dcst.SYNTHETIC_REQUESTS_STILL_TO_MAKE} = 
            CASE 
                WHEN {dcst.CONSUMED_CAPACITY} > {dcst.CAPACITY} THEN 0
                ELSE GREATEST({dcst.SYNTHETIC_REQUESTS_STILL_TO_MAKE} - 1, 0)
            END
        WHERE {dcst.CONTENDER_ID} = $1
        RETURNING {dcst.SYNTHETIC_REQUESTS_STILL_TO_MAKE}
        """,
        contender_id,
    )

    if result:
        return result[dcst.SYNTHETIC_REQUESTS_STILL_TO_MAKE]
    else:
        return None
