from datetime import datetime, timezone

import utils.datetime_fmt as dt_fmt


def current_datetime_utc() -> datetime:
    """
    Get the current datetime in UTC timezone.

    Returns:
        datetime: The current datetime in UTC timezone.
    """
    return datetime.now(timezone.utc)


def datetime_as_zulu_str(dt: datetime) -> str:
    """
    Get the datetime as a Zulu string (%Y-%m-%dT%H:%M:%S.%fZ).

    Parameters:
        dt (datetime): The datetime to convert.

    Returns:
        str: The datetime as a Zulu string.
    """
    return dt.strftime(dt_fmt.DATETIME_ZULU_FORMAT)


def datetime_as_date_and_hour_str(dt: datetime) -> str:
    """
    Get the datetime as a date and hour string (%Y-%m-%dT%H).

    Parameters:
        dt (datetime): The datetime to convert.

    Returns:
        str: The datetime as a date and hour string.
    """
    return dt.strftime(dt_fmt.DATE_AND_HOUR_FORMAT)


def datetime_as_date_str(dt: datetime) -> str:
    """
    Get the datetime as a date string (%Y-%m-%d).

    Parameters:
        dt (datetime): The datetime to convert.

    Returns:
        str: The datetime as a date string.
    """
    return dt.strftime(dt_fmt.DATE_FORMAT)


def str_to_date(date_str: str) -> datetime:
    """
    Convert a date string to a datetime object (%Y-%m-%d).

    Parameters:
        date_str (str): The date string to convert.

    Returns:
        datetime: The datetime object.
    """
    return datetime.strptime(date_str, dt_fmt.DATE_FORMAT)


def str_to_zulu_datetime(zulu_str: str) -> datetime:
    """
    Convert a Zulu datetime string to a datetime object (%Y-%m-%dT%H:%M:%S.%fZ).

    Parameters:
        zulu_str (str): The Zulu datetime string to convert.

    Returns:
        datetime: The datetime object.
    """
    return datetime.strptime(zulu_str, dt_fmt.DATETIME_ZULU_FORMAT)
