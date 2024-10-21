import contextlib
import json
import logging
import time
from typing import AsyncGenerator


from fiber.logging_utils import get_logger
from validator.utils.generic import generic_constants as gcst

logger = get_logger(__name__)


@contextlib.contextmanager
def log_time(description: str, logger: logging.Logger):
    start_time = time.time()
    try:
        yield
    finally:
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.debug(f"{description} took {elapsed_time:.4f} seconds")


async def async_chain(first_chunk: str, async_gen: AsyncGenerator[str, str]) -> AsyncGenerator[str, str]:
    yield first_chunk  # manually yield the first chunk
    async for item in async_gen:
        yield item  # then yield from the original generator


def get_error_event(job_id: str, error_message: str | None, status_code: int) -> str:
    return json.dumps({gcst.JOB_ID: job_id, gcst.ERROR_MESSAGE: error_message, gcst.STATUS_CODE: status_code})


def get_success_event(content: str, job_id: str, status_code: int) -> str:
    return json.dumps({gcst.JOB_ID: job_id, gcst.STATUS_CODE: status_code, gcst.CONTENT: content})
