from fastapi import Security
from fastapi.security import HTTPAuthorizationCredentials
from validator.entry_node.src.core.configuration import Config
from validator.entry_node.src.core import configuration
from fastapi import HTTPException
from fastapi.security import HTTPBearer

auth_scheme = HTTPBearer()


async def get_config() -> Config:
    return await configuration.factory_config()


async def get_api_key(credentials: HTTPAuthorizationCredentials = Security(auth_scheme)):
    if not credentials.credentials:
        raise HTTPException(status_code=403, detail="Invalid API key")
    return credentials.credentials
