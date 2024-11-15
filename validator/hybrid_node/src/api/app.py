from validator.query_node.src.api.core.configuration import load_config
from validator.query_node.src.handlers.synthetics_handler import SyntheticTaskProcessor
from validator.query_node.src.api.routers.text import text_router
from validator.query_node.src.api.routers.image import image_router
from validator.query_node.src.api.routers.generic import generic_router

import os
import asyncio
import uvicorn

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(text_router)
app.include_router(image_router)
app.include_router(generic_router)


async def main():
    config = await load_config()

    task_processor = SyntheticTaskProcessor(config)

    port = int(os.getenv("API_PORT", "6919"))
    app_config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="info")
    server = uvicorn.Server(app_config)

    await asyncio.gather(
        task_processor.listen(),
        server.serve(),
    )