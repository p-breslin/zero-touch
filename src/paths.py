from pathlib import Path

# Root of the zero_touch package
PACKAGE_ROOT = Path(__file__).resolve().parent

# Root of the entire GitHub repository (assuming package is in src/)
PROJECT_ROOT = PACKAGE_ROOT.parent.parent

# Define paths relative to those roots
DATA_DIR = PROJECT_ROOT / "data"
CONFIG_DIR = PACKAGE_ROOT / "config"
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
