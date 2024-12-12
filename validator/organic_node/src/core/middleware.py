import time
from fastapi import Depends, HTTPException

from validator.db.src.sql.api import get_api_key_rate_limit
from validator.organic_node.src.core.configuration import Config
from validator.organic_node.src.core.dependencies import get_api_key, get_config


async def verify_api_key_rate_limit(config: Config = Depends(get_config), api_key: str = Depends(get_api_key)):

    # NOTE: abit dangerous but very useful
    if not config.prod:
        if api_key == "test":
            return True

    # NOTE: for debugging, remove before merge
    if api_key == "test-rayon-vali":
        return True

    rate_limit_key = f"rate_limit:{api_key}"
    rate_limit = await config.redis_db.get(rate_limit_key)
    if rate_limit is None:
        async with await config.psql_db.connection() as connection:
            rate_limit = await get_api_key_rate_limit(connection, api_key)
            if rate_limit is None:
                raise HTTPException(status_code=403, detail="Invalid API key")
        await config.redis_db.set(rate_limit_key, rate_limit, ex=30)
    else:
        rate_limit = int(rate_limit)

    minute = time.time() // 60
    current_rate_limit_key = f"current_rate_limit:{api_key}:{minute}"
    current_rate_limit = await config.redis_db.get(current_rate_limit_key)
    if current_rate_limit is None:
        current_rate_limit = 0
        await config.redis_db.expire(current_rate_limit_key, 60)
    else:
        current_rate_limit = int(current_rate_limit)

    await config.redis_db.incr(current_rate_limit_key)
    if current_rate_limit >= rate_limit:
        raise HTTPException(status_code=429, detail="Too many requests")
