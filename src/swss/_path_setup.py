"""Bootstrap sys.path for swsscommon when running scripts directly.

Import this module at the top of any executable script to ensure the
swsscommon C extension is importable regardless of how the script is invoked.

Usage (at the top of each script, after the shebang)::

    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    import _path_setup  # noqa: F401  (side-effect import)
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PY_WRAPPER_DIR = _REPO_ROOT / "src" / "sonic-swss-common" / "pyext" / "py3"
_EXTENSION_DIR = Path("/usr/local/lib/python3/dist-packages/swsscommon")

for _p in (str(_EXTENSION_DIR), str(_PY_WRAPPER_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)
