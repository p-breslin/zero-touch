from pathlib import Path

# Package root
PACKAGE_ROOT = Path(__file__).resolve().parent

# Project root
PROJECT_ROOT = PACKAGE_ROOT.parent

# Paths relative to those roots
DATA_DIR = PROJECT_ROOT / "data"
CONFIG_DIR = PROJECT_ROOT / "configs"
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
