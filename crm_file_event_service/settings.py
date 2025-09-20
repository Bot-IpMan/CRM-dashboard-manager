"""Common configuration helpers shared across entry points."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

CONFIG_ENV_VAR = "CRM_SERVICE_CONFIG"
DEFAULT_CONFIG_PATH = Path("config.json")


def resolve_config_path(explicit_path: Path | str | None = None) -> Path:
    """Return the configuration path using CLI arguments or fallbacks.

    Parameters
    ----------
    explicit_path:
        Path provided explicitly by the caller. When ``None``, the path is
        looked up from the ``CRM_SERVICE_CONFIG`` environment variable and
        finally defaults to ``config.json`` inside the working directory.
    """

    if explicit_path is not None:
        return Path(explicit_path).expanduser()

    env_path = os.getenv(CONFIG_ENV_VAR)
    if env_path:
        return Path(env_path).expanduser()

    return DEFAULT_CONFIG_PATH.expanduser()
