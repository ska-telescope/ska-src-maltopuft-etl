"""Intialise application config."""

import logging
import sys
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


def get_config() -> dict[Any, Any]:
    """Parse project config yaml."""
    config_ = {}

    src_path = Path(sys.path[-1])
    project_path = src_path.parent.absolute()
    cfg_path = project_path / "cfg"
    default_config_path = cfg_path / "config.default.yml"
    user_config_path = cfg_path / "config.yml"

    with Path.open(default_config_path, "r", encoding="utf-8") as file:
        logger.info(
            f"Loading default configuration file from {default_config_path}",
        )
        config_ = yaml.safe_load(file)

    try:
        logging.debug(
            f"Looking for user configuration file at {user_config_path}",
        )
        with Path.open(user_config_path, "r", encoding="utf-8") as f:
            user_config = yaml.safe_load(f)
            config_.update(user_config)
    except FileNotFoundError:
        logger.warning(
            f"No user configuration file found at {user_config_path}",
        )

    for k, v in config_.items():
        if "path" in k:
            config_[k] = Path(v)

    return config_


config = get_config()
