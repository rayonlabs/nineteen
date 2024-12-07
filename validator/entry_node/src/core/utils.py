from core.models import config_models as cmodels
from fiber.logging_utils import get_logger
from core import task_config as tcfg
from validator.entry_node.src.models.request_models import ImageModelResponse, TextModelResponse

logger = get_logger(__name__)


def _create_text_model_response(config: cmodels.FullTaskConfig) -> TextModelResponse:
    """Create a TextModelResponse from a text task configuration."""
    id = config.orchestrator_server_config.load_model_config["model"]
    name = config.display_name
    created = config.created
    description = config.description
    context_length = config.orchestrator_server_config.load_model_config["max_model_len"]
    architecture = config.architecture
    endpoint = config.orchestrator_server_config.load_model_config["endpoint"]

    return TextModelResponse(
        id=id,
        name=name,
        created=created,
        description=description,
        context_length=context_length,
        architecture=architecture,
        pricing={"prompt": "0.00", "completion": "0.00", "request": "0.00"},
        is_moderated=False,
        endpoints=[endpoint],
    )


def _create_image_model_response(config: cmodels.FullTaskConfig) -> ImageModelResponse:
    """Create an ImageModelResponse from an image task configuration."""
    return ImageModelResponse(
        id=config.model_info["model"],
        name=config.display_name,
        created=config.created,
        description=config.description,
        pricing={"steps": 0},
    )


def get_text_model_responses() -> list[TextModelResponse]:
    """Get all text model responses from task configs."""
    task_configs = tcfg.get_task_configs()
    text_responses: dict[str, TextModelResponse] = {}

    for config in task_configs.values():
        if config.task_type == cmodels.TaskType.TEXT:
            id = config.orchestrator_server_config.load_model_config["model"]

            if id in text_responses:
                # Add additional endpoint to existing text model response
                endpoint = config.orchestrator_server_config.load_model_config["endpoint"]
                text_responses[id].endpoints.append(endpoint)
            else:
                text_responses[id] = _create_text_model_response(config)

    return list(text_responses.values())


def get_image_model_responses() -> list[ImageModelResponse]:
    """Get all image model responses from task configs."""
    task_configs = tcfg.get_task_configs()
    image_responses: dict[str, ImageModelResponse] = {}

    for config in task_configs.values():
        if config.task_type != cmodels.TaskType.TEXT:
            id = config.model_info["model"]
            image_responses[id] = _create_image_model_response(config)

    return list(image_responses.values())
