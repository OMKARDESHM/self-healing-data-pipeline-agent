import logging
from rich.logging import RichHandler


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # Already configured

    logger.setLevel(logging.INFO)

    handler = RichHandler(rich_tracebacks=True, markup=True)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger
