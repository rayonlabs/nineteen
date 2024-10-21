import json
import httpx
from pydantic import BaseModel
from fiber.logging_utils import get_logger
from miner.config import WorkerConfig

logger = get_logger(__name__)


async def get_image_from_server(
    httpx_client: httpx.AsyncClient,
    body: BaseModel,
    post_endpoint: str,
    worker_config: WorkerConfig,
    timeout: float = 20.0,
) -> dict | None:
    assert worker_config.IMAGE_WORKER_URL is not None, "IMAGE_WORKER_URL is not set in env vars!"
    endpoint = worker_config.IMAGE_WORKER_URL.rstrip("/") + "/" + post_endpoint

    try:
        logger.debug(f"Sending request to {endpoint}")
        response = await httpx_client.post(endpoint, json=body.model_dump(), timeout=timeout)
        response.raise_for_status()

        data = response.json()
        return data

    except httpx.HTTPStatusError as error:
        error_details = {
            "status_code": error.response.status_code,
            "request_url": str(error.request.url),
            "request_method": error.request.method,
            "request_headers": dict(error.request.headers),
            "request_body": json.loads(error.request.content.decode()) if error.request.content else None,
            "response_headers": dict(error.response.headers),
            "response_body": error.response.text,
        }
        logger.error(f"Detailed error information:\n{json.dumps(error_details, indent=2)}")
        logger.error(f"HTTP Status error when getting an image from {endpoint}. Status code: {error.response.status_code}")
        logger.error(f"Response body: {error.response.text[:1000]}...")  # Log first 1000 characters of response body
        logger.error(f"Request headers: {dict(error.request.headers)}")
        logger.error(f"Response headers: {dict(error.response.headers)}")

        if error.response.status_code == 400:
            logger.error("Bad request. Check if the request payload is correct.")
        elif error.response.status_code == 401:
            logger.error("Unauthorized. Check if authentication credentials are correct.")
        elif error.response.status_code == 403:
            logger.error("Forbidden. Check if the client has necessary permissions.")
        elif error.response.status_code == 404:
            logger.error(f"Not found. Verify if the endpoint {endpoint} is correct.")
        elif error.response.status_code == 500:
            logger.error("Internal server error. The image server might be experiencing issues.")

        return None
    except httpx.RequestError as error:
        logger.error(f"Request error when getting an image from {endpoint}")
        logger.error(f"Error details: {str(error)}")
        logger.error(f"Error type: {type(error).__name__}")

        if isinstance(error, httpx.ConnectError):
            logger.error(f"Failed to establish a connection to {endpoint}. Check if the server is running and accessible.")
        elif isinstance(error, httpx.ReadTimeout):
            logger.error(f"Request timed out after {timeout} seconds. Consider increasing the timeout or check server load.")
        elif isinstance(error, httpx.WriteTimeout):
            logger.error(
                "Failed to send the request within the timeout period. Check network conditions or server responsiveness."
            )
        elif isinstance(error, httpx.PoolTimeout):
            logger.error("Timed out while waiting for a connection from the pool. The server might be overloaded.")

        logger.error(f"Request URL: {error.request.url}")
        logger.error(f"Request method: {error.request.method}")
        logger.error(f"Request headers: {dict(error.request.headers)}")

        return None
    except json.JSONDecodeError as error:
        logger.error(f"Failed to decode JSON response from {endpoint}")
        logger.error(f"JSON decode error: {str(error)}")
        logger.error(f"Response content: {error.doc[:1000]}...")  # Log first 1000 characters of the invalid JSON
        return None
    except Exception as error:
        logger.error(f"Unexpected error occurred while getting an image from {endpoint}")
        logger.error(f"Error type: {type(error).__name__}")
        logger.error(f"Error details: {str(error)}")
        logger.exception("Stack trace:")
        return None
