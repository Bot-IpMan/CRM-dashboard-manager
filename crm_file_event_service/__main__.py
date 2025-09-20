"""Command line interface for the CRM file event monitoring service."""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .config import load_config
from .service import FileEventService, configure_logging


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to the JSON configuration file.",
    )
    parser.add_argument(
        "--log-level",
        default=None,
        help="Override the log level defined in the configuration.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single polling cycle instead of a long running service.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    log_level = args.log_level or config.log_level
    configure_logging(log_level)
    logging.getLogger(__name__).info("Loaded configuration from %s", args.config)

    service = FileEventService(config)
    if args.once:
        service.run_once()
    else:
        service.start()


if __name__ == "__main__":
    main()
