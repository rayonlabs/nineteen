from typing import Any, Annotated
from validator.hybrid_node.src.api.core.middleware import verify_api_key_rate_limit
from validator.hybrid_node.src.api.core.configuration import load_config

from fastapi.security import HTTPAuthorizationCredentials
from fastapi import Depends
from fastapi.security import HTTPBearer
from fastapi import Security

from fiber.logging_utils import get_logger


auth_scheme = HTTPBearer()
logger = get_logger(__name__)

async def get_config_dependency():
    config = await load_config()
    return config

async def verify_api_key_dependency(
    config: Annotated[Any, Depends(get_config_dependency)],
    credentials: Annotated[HTTPAuthorizationCredentials, Security(auth_scheme)]):
    
    await verify_api_key_rate_limit(config, credentials.credentials)
