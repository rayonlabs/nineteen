import os
from dotenv import load_dotenv
from generic.logging import get_logger

logger = get_logger(__name__)
load_dotenv(dotenv_path=".default.env", verbose=True)


def get_connection_string_from_env() -> str:
    username = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_DB")
    port = os.getenv("POSTGRES_PORT")
    database = os.getenv("POSTGRES_NAME")

    if not all([username, password, host, port, database]):
        raise ValueError(
            "All of POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_PORT, and POSTGRES_NAME must be set",
            f"But i got: username; {username}, password; *****, host; {host}, port; {port}, database; {database}",
        )
    return get_connection_string(username, password, host, port, database)


def get_connection_string(username: str, password: str, host: str, port: int, database: str) -> str:
    return f"postgresql://{username}:{password}@{host}:{port}/{database}"
