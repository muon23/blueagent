import sys
from pathlib import Path


def project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def ensure_paths() -> tuple[Path, Path]:
    root = project_root()
    src_main = root / "src" / "main"
    cjutil_main = root.parent / "cjutil" / "src" / "main"

    if str(src_main) not in sys.path:
        sys.path.insert(0, str(src_main))
    if str(cjutil_main) not in sys.path:
        sys.path.insert(0, str(cjutil_main))

    return root, cjutil_main


PROJECT_ROOT, CJUTIL_MAIN = ensure_paths()
