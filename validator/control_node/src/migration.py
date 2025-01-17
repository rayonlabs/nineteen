from dotenv import load_dotenv
import os
import asyncio

from fiber.logging_utils import get_logger

from validator.control_node.src.control_config import load_config


load_dotenv(os.getenv("ENV_FILE", ".vali.env"))

logger = get_logger(__name__)


async def main() -> None:
    config = load_config()
    await config.psql_db.connect()
    async with await config.psql_db.connection() as connection:
        await connection.execute("""
            DELETE FROM tasks
            where created_at < '2025-01-02'
        """)


if __name__ == "__main__":
    asyncio.run(main())
