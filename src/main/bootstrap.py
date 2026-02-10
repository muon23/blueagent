import sys
from pathlib import Path

ANYCHAT_LLMS = Path("/Users/cjwang/IdeaProjects/anychat/src/main")
if str(ANYCHAT_LLMS) not in sys.path:
    sys.path.insert(0, str(ANYCHAT_LLMS))
