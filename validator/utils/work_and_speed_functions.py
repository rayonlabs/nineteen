import json

from core.models import config_models as cmodels
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

CHARACTER_TO_TOKEN_CONVERSION = 4.0


def _calculate_work_image(steps: int) -> float:
    """Returns the expected work for that image boi. Overhead is not needed in this calculation

    Volume for images is in terms of steps."""
    work = steps
    return work


def _calculate_work_text(character_count: int) -> float:
    """
    Returns the expected work for dem chars .

    Volume for text is tokems"""
    work = character_count / CHARACTER_TO_TOKEN_CONVERSION
    return work


def calculate_work(
    task_config: cmodels.FullTaskConfig,
    result: dict,
    steps: int | None = None,
) -> float:
    """Gets volume for the task that was executed"""
    config = task_config

    raw_formatted_response = result.get("formatted_response", {})

    if config.task_type == cmodels.TaskType.IMAGE:
        assert steps is not None
        return _calculate_work_image(steps)
    elif config.task_type == cmodels.TaskType.TEXT:
        formatted_response = (
            json.loads(raw_formatted_response) if isinstance(raw_formatted_response, str) else raw_formatted_response
        )
        character_count = 0
        for text_json in formatted_response:
            try:
                character_count += len(text_json["choices"][0]["delta"]["content"])
            except KeyError:
                logger.error(f"KeyError: {text_json}")

        logger.info(f"Number of characters: {character_count}")

        if character_count == 0:
            return 1

        return _calculate_work_text(character_count)
    else:
        raise ValueError(f"Task {task_config.task} not found for work bonus calculation")
