
from validator.query_node.src.api.dependencies.deps import get_config_dependency
from validator.query_node.src.api.dependencies.deps import verify_api_key_dependency
from core import task_config as tcfg

from typing import Any, Annotated
from fastapi import Depends
from fastapi.routing import APIRouter


async def models(
    config: Annotated[Any, Depends(get_config_dependency)],
    _: Annotated[None, Depends(verify_api_key_dependency)]
) -> list[dict[str, Any]]:
    models = tcfg.get_public_task_configs()
    new_models = []
    for model in models:
        new_model = {"model_name": model["task"]} 
        new_model.update({k: v for k, v in model.items() if k != "task"})
        new_models.append(new_model)
    return new_models

generic_router = APIRouter(tags=["Models"])
generic_router.add_api_route(
    "/v1/models",
    models,
    methods=["GET"],
    response_model=None,
)