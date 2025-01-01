from dotenv import load_dotenv
import os
import asyncio
import nltk

from fiber.logging_utils import get_logger

from validator.control_node.src.score_results import score_results
from validator.control_node.src.control_config import load_config
from validator.control_node.src.cycle import execute_cycle  # noqa
from validator.utils.synthetic import synthetic_utils as sutils

load_dotenv(os.getenv("ENV_FILE", ".vali.env"))

logger = get_logger(__name__)

async def main() -> None:
    nltk.download('punkt_tab')

    config = load_config()
    await config.psql_db.connect()

    # NOTE: We could make separate threads if you wanted to be fancy
    await asyncio.gather(
        score_results.main(config),
        sutils.get_save_random_text(),
        execute_cycle.main(config)
    )


if __name__ == "__main__":
    asyncio.run(main())
