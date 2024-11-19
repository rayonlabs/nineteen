import socket
from typing import Any

import asyncpg
from asyncpg import Pool

from validator.utils.database import database_utils as dutils
import validator.utils.database.database_constants as dcst
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
        # Add default pool settings
        self.pool_min_size = dcst.PSQL_MIN_POOL_SIZE
        self.pool_max_size = dcst.PSQL_MAX_POOL_SIZE
        self.pool_max_inactive_connection_lifetime = dcst.PSQL_MIN_INACTIVE_CNX_LIFETIME
        self.pool_timeout = dcst.POOL_TIMEOUT

    async def connect(self) -> None:
        logger.debug(f"Connecting to {self.connection_string}....")
        if self.pool is None:
            try:
                self.pool = await asyncpg.create_pool(
                    self.connection_string,
                    min_size=self.pool_min_size,
                    max_size=self.pool_max_size,
                    max_inactive_connection_lifetime=self.pool_max_inactive_connection_lifetime,
                    timeout=self.pool_timeout,
                    command_timeout=10.0,
                    server_settings={
                        'statement_timeout': '30000',  # 30 seconds
                        'idle_in_transaction_session_timeout': '30000'  # 30 seconds
                    }
                )
                if self.pool is None:
                    raise ConnectionError("Failed to create connection pool")
                else:
                    logger.debug(f"Connection pool created successfully with {self.pool_min_size}-{self.pool_max_size} connections")
            except asyncpg.exceptions.PostgresError as e:
                raise ConnectionError(f"PostgreSQL error: {str(e)}") from e
            except socket.gaierror as e:
                raise ConnectionError(
                    f"DNS resolution failed: {str(e)}. Check your host name. connection_string: {self.connection_string}"
                ) from e
            except Exception as e:
                raise ConnectionError(f"Unexpected error while connecting: {str(e)}") from e

    async def acquire(self) -> asyncpg.Connection:
        """Explicitly acquire a connection from the pool."""
        if not self.pool:
            raise RuntimeError("Database connection not established. Call connect() first.")
        return await self.pool.acquire()

    async def release(self, connection: asyncpg.Connection) -> None:
        """Explicitly release a connection back to the pool."""
        if not self.pool:
            raise RuntimeError("Database connection not established. Call connect() first.")
        await self.pool.release(connection)

    async def connection(self) -> asyncpg.pool.PoolAcquireContext:
        """Get a connection context manager."""
        if not self.pool:
            raise RuntimeError("Database connection not established. Call connect() first.")
        return self.pool.acquire()

    async def close(self) -> None:
        """Close all pool connections."""
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