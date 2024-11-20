from validator.hybrid_node.src.api import request_models
from validator.hybrid_node.src.handlers.process_queries import process_organic_image_request
from validator.hybrid_node.src.api.dependencies.deps import get_config_dependency
from validator.hybrid_node.src.api.dependencies.deps import verify_api_key_dependency

from fastapi.routing import APIRouter
from fastapi.responses import JSONResponse
from fastapi import Depends
from typing import Any, Annotated


async def image_to_image(
    request: request_models.ImageToImageRequest,
    config: Annotated[Any, Depends(get_config_dependency)],
    _: Annotated[None, Depends(verify_api_key_dependency)],
) -> JSONResponse:
    payload = await request_models.image_to_image_to_payload(
        request,
        httpx_client=config.httpx_client,
        prod=config.prod,
    )
    return await process_organic_image_request(config, payload, payload.model)


async def inpaint(
    request: request_models.InpaintRequest,
    config: Annotated[Any, Depends(get_config_dependency)],
    _: Annotated[None, Depends(verify_api_key_dependency)],
) -> JSONResponse:
    payload = await request_models.inpaint_to_payload(request, httpx_client=config.httpx_client, prod=config.prod)
    return await process_organic_image_request(config, payload, "inpaint")


async def avatar(
    request: request_models.AvatarRequest,
    config: Annotated[Any, Depends(get_config_dependency)],
    _: Annotated[None, Depends(verify_api_key_dependency)],
) -> JSONResponse:
    payload = await request_models.avatar_to_payload(request, httpx_client=config.httpx_client, prod=config.prod)
    return await process_organic_image_request(config, payload, "avatar")


async def text_to_image(
    request: request_models.TextToImageRequest,
    config: Annotated[Any, Depends(get_config_dependency)],
    _: Annotated[None, Depends(verify_api_key_dependency)],
) -> JSONResponse:
    payload = request_models.text_to_image_to_payload(request)
    return await process_organic_image_request(config, payload, payload.model)


image_router = APIRouter(tags=["Image"])

image_router.add_api_route("/v1/text-to-image", text_to_image, methods=["POST"], response_model=None)
image_router.add_api_route("/v1/image-to-image", image_to_image, methods=["POST"], response_model=None)
image_router.add_api_route("/v1/inpaint", inpaint, methods=["POST"], response_model=None)
image_router.add_api_route("/v1/avatar", avatar, methods=["POST"], response_model=None)
