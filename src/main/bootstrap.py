import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CJUTIL_MAIN = PROJECT_ROOT.parent / "cjutil" / "src" / "main"
if str(CJUTIL_MAIN) not in sys.path:
    sys.path.insert(0, str(CJUTIL_MAIN))
