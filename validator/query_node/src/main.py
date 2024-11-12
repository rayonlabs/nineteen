import os
import asyncio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from fiber.logging_utils import get_logger
from validator.query_node.core.configuration import load_config
from validator.query_node.src.synthetics_handler import SyntheticTaskProcessor
from validator.query_node.src.endpoints import image_router, text_router, generic_router

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

if __name__ == "__main__":
    asyncio.run(main())
