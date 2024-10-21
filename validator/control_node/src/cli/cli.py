from dotenv import load_dotenv
load_dotenv(".vali.env")
from validator.db.src.sql.api import (
    add_api_key,
    delete_api_key,
    get_logs_for_key,
    list_api_keys,
    update_api_key_balance,
    update_api_key_name,
    update_api_key_rate_limit_per_minute,
)

load_dotenv(".vali.env")
import asyncclick as click
from rich.console import Console
from rich.table import Table
from validator.control_node.src.control_config import load_config
import uuid
from validator.utils.database import database_constants as dcst

config = load_config()


@click.group()
def cli():
    """A simple CLI tool with multiple commands."""
    pass


@cli.command()
@click.option("--count", default=1, help="Number of greetings.")
@click.option(
    "--rate-limit-per-minute", prompt="Rate limit per minute", help="The rate limit per minute for the API key.", default=100
)
@click.option("--name", prompt="Name of the key for identification", help="The name for the API key.", default="No-Name")
async def create_key(count, rate_limit_per_minute, name):
    """Simple program that greets NAME for a total of COUNT times."""
    await config.psql_db.connect()
    balance = 100000000
    print("For now, the balance is set to 100,000,000 [will be improved soon]")

    api_key = str(uuid.uuid4())
    print(api_key, balance, rate_limit_per_minute, name)
    async with await config.psql_db.connection() as connection:
        await add_api_key(connection, api_key, balance, rate_limit_per_minute, name)

    console = Console()
    table = Table(show_header=True, header_style="bold magenta")

    table.add_column(dcst.KEY)
    table.add_column(dcst.BALANCE)
    table.add_column(dcst.RATE_LIMIT_PER_MINUTE)
    table.add_column(dcst.NAME)

    row = (api_key, balance, rate_limit_per_minute, name)
    table.add_row(*map(str, row))

    console.print(table)


@cli.command()
@click.option("--api-key", prompt="API Key", help="The API key to update.", required=True)
@click.option("--balance", prompt="Balance", help="The balance to update.", default=-1, type=int)
@click.option(
    "--rate-limit-per-minute", prompt="Rate limit per minute", help="The rate limit per minute to update.", default=-1, type=int
)
@click.option("--name", prompt="Name", help="The name to update.", default="")
async def update_key(api_key, balance, rate_limit_per_minute, name):
    await config.psql_db.connect()
    async with await config.psql_db.connection() as connection:
        if balance >= 0:
            await update_api_key_balance(connection, api_key, balance)
        if rate_limit_per_minute >= 0:
            await update_api_key_rate_limit_per_minute(connection, api_key, rate_limit_per_minute)
        if name:
            await update_api_key_name(connection, api_key, name)


@cli.command()
@click.option("--api-key", prompt="API Key", help="The API key to delete.", required=True)
async def delete_key(api_key):
    await config.psql_db.connect()
    async with await config.psql_db.connection() as connection:
        await delete_api_key(connection, api_key)


@cli.command()
async def list_keys():
    await config.psql_db.connect()
    async with await config.psql_db.connection() as connection:
        keys = await list_api_keys(connection)
    console = Console()

    table = Table(show_header=True, header_style="bold magenta")
    try:
        _ = keys[0].keys()
    except IndexError:
        console.print("No keys table found - try adding a key")
        return

    columns_need_adding = True

    for key in keys:
        if key:
            if columns_need_adding:
                for column_name in key.keys():
                    table.add_column(column_name)

                columns_need_adding = False

            table.add_row(*[str(value) for value in key.values()])

    console.print(table)


@cli.command()
@click.option("--api-key", prompt="API Key", help="The API key to get logs for.", required=True)
async def logs_for_key(api_key):
    await config.psql_db.connect()
    async with await config.psql_db.connection() as connection:
        logs = await get_logs_for_key(connection, api_key)
    console = Console()
    table = Table(show_header=True, header_style="bold magenta")

    if logs:
        for column_name in logs[0].keys():
            table.add_column(column_name)

        for log in logs:
            log = dict(log)
            table.add_row(*[str(value) for value in log.values()])

        console.print(table)
    else:
        print(f"No logs found for key: {api_key}")


@cli.command()
async def logs_summary():
    await config.psql_db.connect()
    async with await config.psql_db.connection() as connection:
        keys = await list_api_keys(connection)

    console = Console()

    summary_table = Table(show_header=True, header_style="bold magenta")
    summary_table.add_column("key")
    summary_table.add_column("Total Requests")
    summary_table.add_column("Total Credits Used")

    global_endpoint_dict = {}

    for key in keys:
        key = dict(key)[dcst.KEY]
        async with await config.psql_db.connection() as connection:
            logs = await get_logs_for_key(connection, key)

        total_requests = len(logs)
        total_credits_used = sum([dict(log).get("cost", 0) for log in logs])

        for log in logs:
            log = dict(log)
            endpoint = log.get(dcst.ENDPOINT, "unknown_endpoint")
            global_endpoint_dict[endpoint] = global_endpoint_dict.get(endpoint, 0) + 1

        summary_table.add_row(key, str(total_requests), str(total_credits_used))

    console.print(summary_table)

    breakdown_table = Table(show_header=True, header_style="bold green")
    breakdown_table.add_column("Endpoint")
    breakdown_table.add_column("Count")

    for endpoint, count in global_endpoint_dict.items():
        breakdown_table.add_row(endpoint, str(count))

    console.print("Endpoint Breakdown:")
    console.print(breakdown_table)


if __name__ == "__main__":
    cli(_anyio_backend="asyncio")
