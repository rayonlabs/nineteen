from typing import Any
from fiber.logging_utils import get_logger
from fastapi.routing import APIRouter
from validator.entry_node.src.models.text_models import llm_models
from core import task_config as tcfg

logger = get_logger(__name__)


async def models() -> list[dict[str, Any]]:
    models = tcfg.get_public_task_configs()
    new_models = []
    for model in models:
        new_model = {"model_name": model["task"]} 
        new_model.update({k: v for k, v in model.items() if k != "task"})
        new_models.append(new_model)
    return new_models



router = APIRouter()
router.add_api_route("/v1/models", models, methods=["GET"], tags=["Text"], response_model=None)
