r"""Constants"""

import os

from pathlib import Path

DAWGZ_DIR: str | Path | None = None


def get_dawgz_dir() -> Path:
    if DAWGZ_DIR is None:
        dawgz_dir = os.environ.get("DAWGZ_DIR", ".dawgz")
    else:
        dawgz_dir = DAWGZ_DIR

    return Path(dawgz_dir).expanduser().resolve()


def set_dawgz_dir(value: str | Path | None) -> Path:
    global DAWGZ_DIR
    DAWGZ_DIR = value

    return get_dawgz_dir()
