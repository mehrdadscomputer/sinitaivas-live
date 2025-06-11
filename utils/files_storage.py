from pathlib import Path


def create_dir_if_not_exists(prefix: str) -> None:
    """
    Create a directory if it does not exist.
    :param prefix: The prefix to create, each directory separated by a slash.
    :return: None
    """
    Path(prefix).mkdir(parents=True, exist_ok=True)
