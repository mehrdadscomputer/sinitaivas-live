from atproto_client.models.utils import (
    get_or_create,
    get_model_as_json,
    get_model_as_dict,
)
from atproto import (
    CAR,
    AtUri,
    FirehoseSubscribeReposClient,
    firehose_models,
    models,
    parse_subscribe_repos_message,
)
import json
import os
from datetime import datetime
from typing import Any, Dict

import firehose.constants as const
import utils.datetime_utils as dt_utils
import utils.files_storage as fs
import utils.bytes_io as bytes_io
from utils.logging import logger, log_before_retry, log_after_retry
from tenacity import retry, stop_after_attempt, wait_exponential


def process_commit(commit: models.ComAtprotoSyncSubscribeRepos.Commit) -> None:
    """
    Process commit operations and save them to a JSON file.

    Parameters:
        commit (models.ComAtprotoSyncSubscribeRepos.Commit): The commit message to process.
    Returns:
        None
    """
    car = CAR.from_bytes(commit.blocks)

    current_utc_time = dt_utils.current_datetime_utc()

    # Process each element
    for op in commit.ops:
        with logger.contextualize(op=op):
            _process_op(car, commit, op, current_utc_time)


def _process_op(
    car: CAR,
    commit: models.ComAtprotoSyncSubscribeRepos.Commit,
    op: models.ComAtprotoSyncSubscribeRepos.RepoOp,
    current_utc_time: datetime,
) -> None:
    """
    Process an operation and save it to a JSON file.

    Parameters:
        car (CAR): The CAR object.
        commit (models.ComAtprotoSyncSubscribeRepos.Commit): The commit message.
        op (models.ComAtprotoSyncSubscribeRepos.RepoOp): The operation to process.
        current_utc_time (datetime): The current datetime in UTC timezone.
    Returns:
        None
    """
    current_utc_time_str = dt_utils.datetime_as_date_and_hour_str(current_utc_time)
    current_utc_date_str = dt_utils.datetime_as_date_str(current_utc_time)
    prefix = f"firehose_stream/{current_utc_date_str}"
    fs.create_dir_if_not_exists(prefix)
    output_filename = f"{prefix}/{current_utc_time_str}.ndjson"

    commit_info = _init_commit_info(commit, op, current_utc_time)
    if not commit_info:
        return
    updated_commit_info = _extract_record_from_blocks(car, op, commit_info)
    _save_commit_info_to_file(updated_commit_info, output_filename)


def _save_commit_info_to_file(
    commit_info: Dict[str, Any], output_filename: str
) -> None:
    """
    Save the commit info to a JSON file.

    Parameters:
        commit_info (Dict[str, Any]): The commit info to save.
        output_filename (str): The output file name.
    Returns:
        None
    """
    try:
        with open(output_filename, "a", encoding="utf-8") as json_file:
            json_file.write(json.dumps(commit_info) + "\n")
    except Exception as e:
        logger.bind(file=output_filename, commit_info=commit_info).error(
            f"Failed to write to file: {e}"
        )


def _extract_record_from_blocks(
    car: CAR,
    op: models.ComAtprotoSyncSubscribeRepos.RepoOp,
    commit_info: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Extract the record from the blocks.

    Parameters:
        car (CAR): The CAR object.
        op (models.ComAtprotoSyncSubscribeRepos.RepoOp): The operation to process.
        commit_info (Dict[str, Any]): The commit info to update.
    Returns:
        Dict[str, Any]: The updated commit info.
    """
    raw_record = car.blocks.get(op.cid)
    record = get_or_create(raw_record, strict=False)
    if not record:
        logger.bind(raw_record=raw_record).warning("No record from blocks")
        return commit_info
    try:
        record_json = get_model_as_json(record)
        commit_info.update(json.loads(record_json))
    except Exception as e:
        # this fails for invalid utf-8 bytes
        logger.bind(record=record).warning(f"Failed to get record as JSON: {e}")
        try:
            record_json = get_model_as_dict(record)
            record_json = bytes_io.convert_bytes_to_str(record_json)
            commit_info.update(record_json)
        except Exception as e:
            logger.bind(record=record).error(
                f"Failed to update commit_info with record from blocks: {e}"
            )
    return commit_info


def _init_commit_info(
    commit: models.ComAtprotoSyncSubscribeRepos.Commit,
    op: models.ComAtprotoSyncSubscribeRepos.RepoOp,
    current_utc_time: datetime,
) -> Dict[str, Any]:
    """
    Initialize the commit info.
    Parameters:
        commit (models.ComAtprotoSyncSubscribeRepos.Commit): The commit message.
            Represents an update of repository state. Note that empty commits
            are allowed, which include no repo data changes, but an update to
            rev and signature.
        op (models.ComAtprotoSyncSubscribeRepos.RepoOp):
            Represents a repo operation, ie a mutation of a single record.
        current_utc_time (datetime): The current datetime in UTC timezone.
    Returns:
        Dict[str, Any]: The initialized commit info.
    """
    try:
        uri = AtUri.from_str(f"at://{commit.repo}/{op.path}")
        return {
            "seq": commit.seq,
            "collected_at": dt_utils.datetime_as_zulu_str(current_utc_time),
            "rev": commit.rev,
            "since": commit.since,
            "commit_time": commit.time,
            "action": op.action,
            "type": uri.collection,
            "uri": str(uri),
            # "uri_host": uri.hostname,
            # "uri_http": uri.http,
            # "uri_origin": uri.origin,
            # "uri_protocol": uri.protocol,
            # "uri_rkey": uri.rkey,
            "author": commit.repo,
            "cid": str(op.cid),
            # "op": op.model_dump(exclude_unset=True),
        }
    except Exception as e:
        logger.bind(uri=uri, cid=op.cid).error(f"Failed to init commit info: {e}")
        return {}


def reset_cursor(client: FirehoseSubscribeReposClient) -> None:
    """
    Reset the cursor in the client and in cursors file.
    Parameters:
        client (FirehoseSubscribeReposClient): The client to reset.
    Returns:
        None
    """
    client.update_params(models.ComAtprotoSyncSubscribeRepos.Params(cursor=None))
    cursor = read_cursor()
    # pop the streamer cursor
    cursor.pop("streamer", None)
    try:
        with open(const.CURSORS_FILE, "w") as f:
            json.dump(cursor, f)
    except Exception as e:
        logger.bind(file=const.CURSORS_FILE).error(f"Failed to reset cursor: {e}")


def update_cursor(client: FirehoseSubscribeReposClient, cursor_position: int) -> None:
    """
    Update the cursor in the client and in cursors file.
    Parameters:
        client (FirehoseSubscribeReposClient): The client to update.
        cursor_position (int): The cursor value to update.
    Returns:
        None
    """
    client.update_params(
        models.ComAtprotoSyncSubscribeRepos.Params(cursor=cursor_position)
    )
    cursor = read_cursor()
    cursor["streamer"] = {
        "cursor": cursor_position,
        "updated_at": dt_utils.datetime_as_zulu_str(dt_utils.current_datetime_utc()),
    }
    try:
        with open(const.CURSORS_FILE, "w") as f:
            json.dump(cursor, f)
    except Exception as e:
        logger.bind(file=const.CURSORS_FILE).error(f"Failed to update cursor: {e}")


def read_cursor() -> Dict[str, Any]:
    """
    Read the cursor from the cursors file.

    Returns:
        Dict[str, Any]: The content of cursors file.
    """
    file = const.CURSORS_FILE
    try:
        with open(file, "r") as f:
            return json.load(f)  # type: ignore
    except Exception as e:
        logger.bind(file=file).error(f"Failed to read cursors file: {e}")
        return {}


def read_last_seq_from_file() -> int:
    """
    Read the last sequence from the latest ndjson file.

    Returns:
        int: The last sequence value.
    """
    json_files = [file for file in os.listdir() if file.endswith(".ndjson")]
    if len(json_files) == 0:
        logger.warning("No JSON files found")
        return 0

    # Sort the files by modification time and get the most recently modified file
    latest_json_file = max(json_files, key=lambda x: os.path.getmtime(x))

    try:
        # Get the last line of the latest modified file
        with open(latest_json_file, "r") as file:
            # Seek to the end of the file
            file.seek(0, os.SEEK_END)
            file.seek(file.tell() - 2, os.SEEK_SET)
            while file.read(1) != "\n":
                file.seek(file.tell() - 2, os.SEEK_SET)
            last_line = file.readline()
            last_line_json = json.loads(last_line)
        return int(last_line_json["seq"])
    except Exception as e:
        logger.bind(latest_json_file=latest_json_file).error(
            f"Failed to read last seq from file: {e}"
        )
        return 0


def get_fresh_client() -> FirehoseSubscribeReposClient:
    """
    Start a Firehose Subscriber client.
    Returns:
        FirehoseSubscribeReposClient: The client instance.
    """
    return FirehoseSubscribeReposClient(base_uri="wss://bsky.network/xrpc")


def resume_streamer() -> FirehoseSubscribeReposClient:
    """
    Resume the streamer from the last cursor position.
    Returns:
        FirehoseSubscribeReposClient: The client instance.
    """
    client = get_fresh_client()
    cursor_position = read_cursor().get("streamer", {}).get("cursor")
    if not cursor_position:
        last_seq = read_last_seq_from_file()
        cursor_position = last_seq
    client.update_params(
        models.ComAtprotoSyncSubscribeRepos.Params(cursor=cursor_position)
    )
    logger.info(f"Resuming streamer from cursor: {cursor_position}")
    return client


def start(client: FirehoseSubscribeReposClient) -> FirehoseSubscribeReposClient:
    """
    Start the streamer.
    Returns:
        FirehoseSubscribeReposClient: The client instance
    """

    def on_message_callback(message: firehose_models.MessageFrame) -> None:
        """
        Handle incoming messages from the firehose.

        Args:
            message (firehose_models.MessageFrame): The incoming message frame.
        """
        commit = parse_subscribe_repos_message(message)
        if (
            not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit)
            or not commit.blocks
        ):
            logger.bind(commit=commit).warning("Invalid commit")
            return

        process_commit(commit)
        update_cursor(client, commit.seq)

    def on_callback_error_callback(error: BaseException) -> None:
        """
        Handle errors from the callback.

        Args:
            error (BaseException): The error encountered.
        """
        logger.error(error)

    client.start(on_message_callback, on_callback_error_callback)
    return client


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    before=log_before_retry,
    after=log_after_retry,
)
def start_with_retry(
    client: FirehoseSubscribeReposClient,
) -> FirehoseSubscribeReposClient:
    return start(client)


def streamer_main(mode: str) -> None:
    """Main function to run the streamer.

    Parameters:
        mode (str): Mode to run the streamer [fresh/resume].
    Returns:
        None
    """
    if mode == "fresh":
        client = get_fresh_client()
        reset_cursor(client)
    else:
        client = resume_streamer()

    try:
        client = start_with_retry(client)
    except Exception as e:
        logger.error(f"Cannot recover from failures: {e}")
