import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.dashboard.app import app


def main() -> None:
    app.run(host="0.0.0.0", port=8000, debug=False)


if __name__ == "__main__":
    main()
