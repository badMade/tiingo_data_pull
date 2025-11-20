"""Convenience launcher for the tiingo_data_pull CLI."""

from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parent
SRC_PATH = PROJECT_ROOT / "src"
if SRC_PATH.exists() and str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from tiingo_data_pull.cli import run  # noqa: E402


if __name__ == "__main__":
    run()
