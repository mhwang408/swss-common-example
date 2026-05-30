"""swsscommon loading and conversion helpers.

This module provides a single importable ``swsscommon`` binding plus small
utility functions used by every example script.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any


def load_swsscommon() -> Any:
    """Load and return the swsscommon SWIG module.

    Tries the normal import path first (works when the C extension is
    installed system-wide).  Falls back to inserting the local build
    directories from the sonic-swss-common submodule.
    """
    try:
        from swsscommon import swsscommon as _sw
        return _sw
    except ImportError:
        pass

    repo_root = Path(__file__).resolve().parents[3]
    py_wrapper_dir = repo_root / "src" / "sonic-swss-common" / "pyext" / "py3"
    extension_dir = Path("/usr/local/lib/python3/dist-packages/swsscommon")

    for path in (str(extension_dir), str(py_wrapper_dir)):
        if path not in sys.path:
            sys.path.insert(0, path)

    sys.modules.pop("swsscommon", None)
    import swsscommon  # type: ignore[import]
    return swsscommon


swsscommon: Any = load_swsscommon()
"""The loaded ``swsscommon`` SWIG module instance."""


def field_value_pairs(fields: dict[str, str]) -> Any:
    """Convert a Python dict to swsscommon.FieldValuePairs.

    Args:
        fields: Mapping of field names to string values.

    Returns:
        A ``FieldValuePairs`` object suitable for Table.set() and similar APIs.
    """
    return swsscommon.FieldValuePairs([(str(k), str(v)) for k, v in fields.items()])


def load_db_config(path: str | None) -> None:
    """Load a SONiC database_config.json if a path is provided.

    Args:
        path: Filesystem path to database_config.json, or None to skip.
    """
    if path:
        swsscommon.SonicDBConfig.load_sonic_db_config(path)
