import os
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


def get_connection_string_from_env() -> str:
    username = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    database = os.getenv("POSTGRES_DB")

    assert username is not None
    assert password is not None
    assert host is not None
    assert port is not None
    assert database is not None

    port = int(port)

    if not all([username, password, host, port, database]):
        raise ValueError(
            "All of POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_PORT, and POSTGRES_HOST must be set",
            f"But i got: username; {username}, password; *****, host; {host}, port; {port}, database; {database}",
        )
    return get_connection_string(username, password, host, port, database)


def get_connection_string(username: str, password: str, host: str, port: int, database: str) -> str:
    return f"postgresql://{username}:{password}@{host}:{port}/{database}"
