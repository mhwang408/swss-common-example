"""Load swsscommon in both SONiC and this repository's local runner."""

import sys
from pathlib import Path


def load_swsscommon():
    try:
        from swsscommon import swsscommon

        return swsscommon
    except ImportError:
        pass

    repo_root = Path(__file__).resolve().parents[2]
    py_wrapper_dir = repo_root / "src" / "sonic-swss-common" / "pyext" / "py3"
    extension_dir = Path("/usr/local/lib/python3/dist-packages/swsscommon")

    for path in (str(extension_dir), str(py_wrapper_dir)):
        if path not in sys.path:
            sys.path.insert(0, path)

    sys.modules.pop("swsscommon", None)
    import swsscommon

    return swsscommon
