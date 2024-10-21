"""
Scores results for tasks by querying an external scoring server.
Selects tasks to score based on the number of results available.
Stores the scored results in the database and potentially posts stats to TauVision.
"""

import asyncio
from datetime import datetime


from fiber.logging_utils import get_logger
from validator.db.src.sql.rewards_and_scores import (
    delete_contender_history_older_than,
)
from validator.control_node.src.control_config import load_config


logger = get_logger(__name__)


async def nuke_contender_history():
    config = load_config()
    await config.psql_db.connect()
    date_to_delete = datetime.now()
    async with await config.psql_db.connection() as connection:
        await delete_contender_history_older_than(connection, date_to_delete)


if __name__ == "__main__":
    asyncio.run(nuke_contender_history())
