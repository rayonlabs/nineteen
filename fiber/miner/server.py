"""
TODO:
- Add metagraph for miner so it can check validators are in the metagraph and their stake
- add the blacklist functionality. Middleware?
- ADD A 'POST DETAILS TO CHAIN' METHOD FOR MINERS TO POST THEIR AXON DETAILS TO THE CHAIN
- INTEGRATE THIS NEW WAY OF DOING THINGS INTO THE SUBNET CODE, BYE BYE SYNAPSES AND DENDRITES
- BIN SIGNING SERVICE AND BAKE ALL THIS INTO THE SUBNET
- THEN TEST motha
"""

from fastapi import FastAPI
from fiber.miner.endpoints.subnet import factory_router as subnet_factory_router
from fiber.miner.endpoints.handshake import factory_router as handshake_factory_router
from fiber.miner.core.config import factory_config
from scalar_fastapi import get_scalar_api_reference


def factory_app() -> FastAPI:
    app = FastAPI()

    subnet_router = subnet_factory_router()
    handshake_router = handshake_factory_router()
    app.include_router(subnet_router)
    app.include_router(handshake_router)

    return app


app = factory_app()


@app.get("/scalar", include_in_schema=False)
async def scalar_html():
    return get_scalar_api_reference(openapi_url=app.openapi_url, title=app.title)


if __name__ == "__main__":
    import uvicorn

    # Caching some configuration
    factory_config()

    uvicorn.run(app, host="127.0.0.1", port=7999)
