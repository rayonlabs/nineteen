from typing import Any
from fiber.logging_utils import get_logger
from fastapi.routing import APIRouter
from validator.entry_node.src.core.utils import get_text_model_responses, get_image_model_responses
from core import task_config as tcfg
from validator.entry_node.src.models.request_models import ImageModelResponse, TextModelResponse

logger = get_logger(__name__)


async def models_deprecated() -> list[dict[str, Any]]:
    models = tcfg.get_public_task_configs()
    new_models = []
    for model in models:
        new_model = {"model_name": model["task"]}
        new_model.update({k: v for k, v in model.items() if k != "task"})
        new_models.append(new_model)
    return new_models


async def text_models() -> list[TextModelResponse]:
    return get_text_model_responses()
    

async def image_models() -> list[ImageModelResponse]:
    return get_image_model_responses()


router = APIRouter()
router.add_api_route("/v1/models", models_deprecated, methods=["GET"], tags=["Models"], response_model=None)
router.add_api_route("/v1/info/models", models_deprecated, methods=["GET"], tags=["Models"], response_model=None)


router.add_api_route("/v2/models", text_models, methods=["GET"], tags=["Models"])
router.add_api_route("/v2/models/image", image_models, methods=["GET"], tags=["Models"])
