import os
from fiber.miner import server
from core.models.config_models import TaskType
from miner.endpoints.text import factory_router as text_factory_router
from miner.endpoints.image import factory_router as image_factory_router
from miner.endpoints.generic import factory_router as generic_factory_router
from fiber.logging_utils import get_logger
from fiber.miner.middleware import configure_extra_logging_middleware

logger = get_logger(__name__)

app = server.factory_app(debug=True)


text_router = text_factory_router()
image_router = image_factory_router()
generic_router = generic_factory_router()
app.include_router(text_router)
app.include_router(image_router)
app.include_router(generic_router)

if os.getenv("ENV", "prod").lower() == "dev":
    configure_extra_logging_middleware(app)

my_miner_type = os.getenv("MINER_TYPE")
if not my_miner_type:
    raise ValueError("MINER_TYPE is not set. Please set the MINER_TYPE environment variable miner!!!")
if my_miner_type not in TaskType._value2member_map_:
    allowed_values = ", ".join(TaskType._value2member_map_.keys())
    raise ValueError(
        f"MINER_TYPE {my_miner_type} is not valid. Please set the MINER_TYPE to one of the following: {allowed_values}"
    )

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=7999)

    # uvicorn miner.server:app --reload --host 127.0.0.1 --port 7999 --env-file .1.env
