import click
from sinitaivas_live.streamer import streamer_main
from utils.logging import logger, handle_catch_error


@handle_catch_error
@click.command()
@click.option(
    "--mode", default="fresh", help="Mode to run the streamer [fresh/resume]."
)
def main(mode: str):
    """
    Main function to run the streamer.

    Parameters:
        mode (str): Mode to run the streamer [fresh/resume].
    Returns:
        None

    ----------------
    Example usage:
    ----------------

    python -m sinitavas_live.main --mode fresh

    python -m sinitaivas_live.main --mode resume
    """
    logger.info(f"Starting streamer process as {mode}")
    if mode not in ["fresh", "resume"]:
        raise ValueError("Only fresh and resume modes are supported")
    streamer_main(mode)


if __name__ == "__main__":
    main()
