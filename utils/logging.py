import sys
from loguru import logger
from tenacity import RetryCallState
import os

logger = logger

# log file goes to the same directory as the entry point script, with the same name as the script
entry_point = os.path.splitext(os.path.basename(sys.argv[0]))[0]
entry_point_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
log_file_name = os.path.join(entry_point_dir, f"{entry_point}.log")

logger.configure(
    handlers=[
        {
            "sink": sys.stderr,
            "format": "<d>{extra}</> | "
            + "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | "
            + "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            + "<level>{message}</level>",
            "serialize": False,
            "level": "DEBUG",  # Log everything from DEBUG level and above
        },
        {
            "sink": log_file_name,
            "format": "<d>{extra}</> | "
            + "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | "
            + "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            + "<level>{message}</level>",
            "serialize": False,
            "level": "WARNING",  # Log everything from WARNING level and above
        },
    ]
)

handle_catch_error = logger.catch(onerror=lambda _: sys.exit(-1))


def log_before_retry(retry_state: RetryCallState) -> None:
    """Log a retry attempt before the retry occurs.

    Parameters:
        retry_state (RetryCallState): The state of the retry.

    Returns:
        None
    """
    message = (
        f"Attempt {retry_state.attempt_number} "
        f"for {retry_state.fn.__name__ if retry_state.fn else 'unknown function'}"
    )
    if retry_state.attempt_number == 1:
        # Log the first attempt at INFO level
        logger.info(message)
    else:
        # Log subsequent attempts at ERROR level, with the exception that caused the retry
        # and the time to wait before the next attempt
        message += (
            f" will wait {retry_state.next_action.sleep if retry_state.next_action else '---'} seconds "
            f"due to {retry_state.outcome.exception() if retry_state.outcome else '---'}"
        )
        logger.error(message)


def log_after_retry(retry_state: RetryCallState) -> None:
    """Log a retry attempt after the retry occurs.

    Parameters:
        retry_state (RetryCallState): The state of the retry.

    Returns:
        None
    """
    if retry_state.outcome:
        if retry_state.outcome.failed:
            logger.error(
                f"Failed to execute {retry_state.fn.__name__ if retry_state.fn else 'unknown function'} "
                f"after {retry_state.attempt_number} attempts. "
                f"Exception: {retry_state.outcome.exception()}"
            )
        else:
            logger.info(
                f"Successfully executed {retry_state.fn.__name__ if retry_state.fn else 'unknown function'} "
                f"after {retry_state.attempt_number} attempts."
            )
    else:
        logger.error(
            f"Retry state outcome is None for {retry_state.fn.__name__ if retry_state.fn else 'unknown function'} "
            f"after {retry_state.attempt_number} attempts."
        )
