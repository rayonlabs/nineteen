import base64
import binascii
import httpx


async def fetch_image_b64(url: str, httpx_client: httpx.AsyncClient) -> str:
    response = await httpx_client.get(url)
    image_data = response.content
    return base64.b64encode(image_data).decode("utf-8")


def image_b64_is_valid(image_b64: str) -> bool:
    try:
        decoded_data = base64.b64decode(image_b64)
        # Check if the decoded data starts with known image file signatures
        image_signatures = [
            b"\xff\xd8\xff",  # JPEG
            b"\x89\x50\x4e\x47\x0d\x0a\x1a\x0a",  # PNG
            b"GIF87a",
            b"GIF89a",  # GIF
            b"\x42\x4d",  # BMP
            b"\x49\x49\x2a\x00",
            b"\x4d\x4d\x00\x2a",  # TIFF
        ]
        return any(decoded_data.startswith(sig) for sig in image_signatures)
    except binascii.Error:
        return False
