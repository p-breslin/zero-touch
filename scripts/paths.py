from pathlib import Path

# Root of the zero_touch package
PACKAGE_ROOT = Path(__file__).resolve().parent

# Root of the entire GitHub repository
PROJECT_ROOT = PACKAGE_ROOT.parent

# Define paths relative to those roots
DATA_DIR = PROJECT_ROOT / "data"
CONFIG_DIR = PROJECT_ROOT / "configs"
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
