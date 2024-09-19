import functools

from contextlib import asynccontextmanager
from fastapi import FastAPI
import uvicorn
from validator.entry_node.src.endpoints.text import router as chat_router
from validator.entry_node.src.endpoints.image import router as image_router

from validator.entry_node.src.core import configuration
from core.logging import get_logger
from scalar_fastapi import get_scalar_api_reference
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor


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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8091)

    # uvicorn validator.entry_node.src.server:app --reload --host 0.0.0.0 --port 8091 --env-file .dev.env
