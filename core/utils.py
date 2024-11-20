from httpx import Client
from core import constants as ccst
from core.models import config_models as cmodels
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


def fetch_voted_weights() -> dict[str, float]:
    url = ccst.BASE_NINETEEN_API_URL + "v1/weights"
    with Client() as client:
        response = client.get(url)
        response.raise_for_status()
        return response.json()


def get_updated_task_config_with_voted_weights(
    task_configs: dict[str, cmodels.FullTaskConfig],
) -> dict[str, cmodels.FullTaskConfig]:
    """
    Replace all default task configs weights with the voted weights to follow consensus
    [you should normalise after this]
    """
    try:
        # Get the voted weights (those decided by other validators)

        voted_weights = fetch_voted_weights()
        logger.debug(f"Voted weights: {voted_weights}")

        weights_for_tasks_we_support = {
            task_name: weight for task_name, weight in voted_weights.items() if weight > 0 and task_name in task_configs
        }

        if not voted_weights:
            return task_configs

        # Normalise all weights
        for task_name, task_config in task_configs.items():
            logger.debug(f"Task name: {task_name}, weight: {weights_for_tasks_we_support.get(task_name, 0)}")
            task_config.weight = weights_for_tasks_we_support.get(task_name, task_config.weight)
            if task_config.weight == 0:
                task_config.enabled = False

        return task_configs
    except Exception as e:
        logger.error(f"Error when updating task config with voted weights: {e}")


def normalise_task_config_weights(task_configs: dict[str, cmodels.FullTaskConfig]) -> dict[str, cmodels.FullTaskConfig]:
    total_weight = sum(task_config.weight for task_config in task_configs.values())
    if total_weight <= 0:
        raise ValueError(f"Total weight is {total_weight} for all tasks - how? It should be >0")
    for task_config in task_configs.values():
        task_config.weight /= total_weight
    return task_configs
