from typing import Any
from substrateinterface import SubstrateInterface
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

def query_substrate(substrate: SubstrateInterface, module: str, method: str, params: list[Any], return_value: bool = True) -> Any:
    try:
        query_result = substrate.query(module, method, params)

        return_val = query_result.value if return_value else query_result

        return substrate, return_val
    except Exception as e:
        logger.error(f"Query failed with error: {e}. Module: {module}, Method: {method}, Params: {params}. Reconnecting and retrying.")

        substrate = SubstrateInterface(url=substrate.url)

        query_result = substrate.query(module, method, params)

        return_val = query_result.value if return_value else query_result

        return substrate, return_val
