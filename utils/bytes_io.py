import base64
from typing import Dict, List, Union, overload

from utils.logging import logger


@overload
def convert_bytes_to_str(obj: bytes) -> str: ...


@overload
def convert_bytes_to_str(obj: List[bytes]) -> List[str]: ...


@overload
def convert_bytes_to_str(obj: Dict[bytes, bytes]) -> Dict[str, str]: ...


def convert_bytes_to_str(obj: Union[Dict, List, bytes]) -> Union[Dict, List, str]:
    """
    Recursively converts bytes to strings in a JSON-like object.
    Parameters:
        obj: The object to convert.
    Returns:
        The object with bytes converted to strings.
    """
    if isinstance(obj, bytes):
        try:
            return base64.b64encode(obj).decode("utf-8")
        except UnicodeDecodeError:
            logger.bind(obj=obj).warning(
                "failed to decode bytes, returning hex representation"
            )
            return obj.hex()
    if isinstance(obj, list):
        return [convert_bytes_to_str(item) for item in obj]
    if isinstance(obj, dict):
        return {
            convert_bytes_to_str(key): convert_bytes_to_str(value)
            for key, value in obj.items()
        }
    return obj
