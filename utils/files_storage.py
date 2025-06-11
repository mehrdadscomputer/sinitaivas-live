from pathlib import Path
import os


def current_dir() -> str:
    """
    Get the current working directory.
    :return: The current working directory as a string.
    """
    return os.getcwd()


def create_dir_if_not_exists(prefix: str) -> None:
    """
    Create a directory if it does not exist.
    :param prefix: The prefix to create, each directory separated by a slash.
    :return: None
    """
    Path(prefix).mkdir(parents=True, exist_ok=True)
