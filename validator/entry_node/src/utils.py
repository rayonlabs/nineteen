from functools import lru_cache

from fastapi.exceptions import HTTPException
from core.models import config_models as cmodels
from fiber.logging_utils import get_logger
from core import task_config as tcfg
from validator.entry_node.src.models.request_models import ChatRequest, CompletionRequest, ImageModelResponse, TextModelResponse
import random
from core.models import payload_models
from core import constants as cst

logger = get_logger(__name__)


def _create_text_model_response(config: cmodels.FullTaskConfig) -> TextModelResponse:
    """Create a TextModelResponse from a text task configuration."""
    id = config.orchestrator_server_config.load_model_config["model"]
    name = config.display_name
    created = config.created
    description = config.description
    context_length = config.orchestrator_server_config.load_model_config["max_model_len"]
    architecture = config.architecture
    endpoint = config.orchestrator_server_config.endpoint

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


@lru_cache
def get_text_model_responses() -> list[TextModelResponse]:
    """Get all text model responses from task configs."""
    task_configs = tcfg.get_task_configs()
    text_responses: dict[str, TextModelResponse] = {}

    for config in task_configs.values():
        if config.task_type == cmodels.TaskType.TEXT:
            id = config.orchestrator_server_config.load_model_config["model"]

            if id in text_responses:
                # Add additional endpoint to existing text model response
                endpoint = config.orchestrator_server_config.endpoint
                text_responses[id].endpoints.append(endpoint)
            else:
                text_responses[id] = _create_text_model_response(config)

    return list(text_responses.values())


@lru_cache
def get_image_model_responses() -> list[ImageModelResponse]:
    """Get all image model responses from task configs."""
    task_configs = tcfg.get_task_configs()
    image_responses: dict[str, ImageModelResponse] = {}

    for config in task_configs.values():
        task = config.task
        if config.task_type != cmodels.TaskType.TEXT:
            image_responses[task] = _create_image_model_response(config)

    return list(image_responses.values())


@lru_cache
def get_model_id_to_task_text(completions: bool) -> dict[str, str]:
    """Get a mapping of model IDs to task names for text models."""
    if completions:
        return {
            config.orchestrator_server_config.load_model_config["model"]: config.task
            for config in tcfg.get_task_configs().values()
            if config.task_type == cmodels.TaskType.TEXT and config.endpoint == cmodels.Endpoints.completions.value
        }
    else:
        return {
            config.orchestrator_server_config.load_model_config["model"]: config.task
            for config in tcfg.get_task_configs().values()
            if config.task_type == cmodels.TaskType.TEXT and config.endpoint == cmodels.Endpoints.chat_completions.value
        }


def chat_to_payload(chat_request: ChatRequest) -> payload_models.ChatPayload:
    task_configs = tcfg.get_task_configs()
    model_hypened = chat_request.model.replace("_", "-")
    model_with_chat_prepended = "chat-" + model_hypened.strip("chat-")

    if model_hypened not in task_configs:
        if model_with_chat_prepended in task_configs:
            model = model_with_chat_prepended
        else:
            model_id_to_task = get_model_id_to_task_text(completions=False)
            if model_hypened not in model_id_to_task:
                raise HTTPException(
                    status_code=404,
                    detail=(
                        f"Model {model_hypened} not found for /v1/chat/completions. Available models: {list(model_id_to_task.keys())}"
                    ),
                )
            model = model_id_to_task[model_hypened]
    else:
        model = model_hypened

    return payload_models.ChatPayload(
        messages=chat_request.messages,
        temperature=chat_request.temperature,
        max_tokens=chat_request.max_tokens,
        model=model,
        top_p=chat_request.top_p,
        stream=True,
        logprobs=chat_request.logprobs,
        seed=random.randint(1, 100000),
    )


def chat_comp_to_payload(chat_request: CompletionRequest) -> payload_models.CompletionPayload:
    task_configs = tcfg.get_task_configs()
    model_hypened = chat_request.model.replace("_", "-")
    model_with_chat_prepended = "chat-" + model_hypened.strip("chat-")

    if model_hypened not in task_configs:
        if model_with_chat_prepended in task_configs:
            model = model_with_chat_prepended
        else:
            model_id_to_task = get_model_id_to_task_text(completions=True)
            if model_hypened not in model_id_to_task:
                raise HTTPException(
                    status_code=404,
                    detail=(
                        f"Model {model_hypened} not found for /v1/completions. Available models: {list(model_id_to_task.keys())}"
                    ),
                )
            model = model_id_to_task[model_hypened]

    else:
        model = model_hypened

    return payload_models.CompletionPayload(
        prompt=chat_request.prompt,
        temperature=chat_request.temperature,
        max_tokens=chat_request.max_tokens,
        model=model,
        top_p=chat_request.top_p,
        stream=True,
        logprobs=chat_request.logprobs,
        seed=random.randint(1, 100000),
    )


def handle_min_steps(task_config: cmodels.FullTaskConfig, steps: int) -> None:
    min_steps = task_config.model_info.get(cst.MIN_STEPS, 0)
    max_steps = task_config.model_info.get(cst.MAX_STEPS, float("inf"))

    if steps < min_steps:
        raise HTTPException(status_code=422, detail=f"Minimum steps for {task_config.model_info['model']} is {min_steps}")
    if steps > max_steps:
        raise HTTPException(status_code=422, detail=f"Maximum steps for {task_config.model_info['model']} is {max_steps}")
