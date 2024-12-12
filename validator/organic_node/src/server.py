from contextlib import asynccontextmanager
import os
from fastapi import FastAPI
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
from fiber.logging_utils import get_logger
from fiber.miner.middleware import configure_extra_logging_middleware  # noqa
from scalar_fastapi import get_scalar_api_reference
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from validator.organic_node.src.endpoints.text import router as chat_router
from validator.organic_node.src.endpoints.image import router as image_router
from validator.organic_node.src.endpoints.generic import router as generic_router
from validator.organic_node.src.core import configuration


logger = get_logger(__name__)


def factory_app(debug: bool = False) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await configuration.factory_config()  # Cachin'
        yield

        logger.info("Shutting down...")

    app = FastAPI(lifespan=lifespan, debug=debug)

    app.add_api_route(
        "/scalar",
        lambda: get_scalar_api_reference(openapi_url=app.openapi_url, title=app.title),
        methods=["GET"],
    )

    FastAPIInstrumentor().instrument_app(app)
    return app


app = factory_app()
app.include_router(chat_router)
app.include_router(image_router)
app.include_router(generic_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if os.getenv("ENV") != "prod":
    configure_extra_logging_middleware(app)


if __name__ == "__main__":

    organic_port_nginx = os.getenv("ORGANIC_SERVER_PORT")
    if organic_port_nginx is None:
        logger.error("ORGANIC_SERVER_PORT is not set")
        exit(1)
    else:
        uvicorn.run(app, host="0.0.0.0", port=6919)

    # uvicorn validator.organic_node.src.server:app --reload --host 0.0.0.0 --port 8091 --env-file .vali.env