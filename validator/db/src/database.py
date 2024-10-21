import socket
from typing import Any

import asyncpg
from asyncpg import Pool

from validator.utils.database import database_utils as dutils
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


class PSQLDB:
    def __init__(self, from_env: bool = True, connection_string: str | None = None):
        if from_env:
            connection_string = dutils.get_connection_string_from_env()
        elif connection_string is None:
            raise ValueError("Either from_env must be True or connection_string must be set")
        self.connection_string: str = connection_string
        self.pool: Pool | None = None

    async def connect(self) -> None:
        logger.debug(f"Connecting to {self.connection_string}....")
        if self.pool is None:
            try:
                self.pool = await asyncpg.create_pool(self.connection_string)
                if self.pool is None:
                    raise ConnectionError("Failed to create connection pool")
                else:
                    logger.debug("Connection pool created successfully")
            except asyncpg.exceptions.PostgresError as e:
                raise ConnectionError(f"PostgreSQL error: {str(e)}") from e
            except socket.gaierror as e:
                raise ConnectionError(
                    f"DNS resolution failed: {str(e)}. Check your host name. connection_string: {self.connection_string}"
                ) from e
            except Exception as e:
                raise ConnectionError(f"Unexpected error while connecting: {str(e)}") from e

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()

    async def fetchall(self, query: str, *args: Any) -> list[dict[str, Any]]:
        if not self.pool:
            raise RuntimeError("Database connection not established. Call connect() first.")
        async with self.pool.acquire() as connection:
            try:
                rows = await connection.fetch(query, *args)
                return [dict(row) for row in rows]

            except asyncpg.exceptions.PostgresError as e:
                logger.error(f"PostgreSQL error in fetch_all: {str(e)}")
                logger.error(f"Query: {query}")
                raise

    async def connection(self) -> asyncpg.pool.PoolAcquireContext:
        if not self.pool:
            raise RuntimeError("Database connection not established. Call connect() first.")
        return self.pool.acquire()

    async def truncate_all_tables(self) -> None:
        if not self.pool:
            raise RuntimeError("Database connection not established. Call connect() first.")

        async with self.pool.acquire() as connection:
            try:
                # Get all table names in the current schema
                tables = await connection.fetch(
                    """
                    SELECT tablename FROM pg_tables
                    WHERE schemaname = 'public'
                    """
                )

                # Disable triggers and truncate each table
                await connection.execute("SET session_replication_role = 'replica';")
                for table in tables:
                    await connection.execute(f'TRUNCATE TABLE "{table["tablename"]}" CASCADE;')
                await connection.execute("SET session_replication_role = 'origin';")

                logger.info("All tables have been truncated successfully.")
            except asyncpg.exceptions.PostgresError as e:
                logger.error(f"PostgreSQL error in truncate_all_tables: {str(e)}")
                raise

    async def fetchone(self, query: str, *args: Any) -> dict[str, Any] | None:
        if not self.pool:
            raise RuntimeError("Database connection not established. Call connect() first.")
        async with self.pool.acquire() as connection:
            try:
                row = await connection.fetchrow(query, *args)
                return dict(row) if row else None
            except asyncpg.exceptions.PostgresError as e:
                logger.error(f"PostgreSQL error in fetchone: {str(e)}")
                logger.error(f"Query: {query}")
                raise
