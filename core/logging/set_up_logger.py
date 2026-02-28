from pathlib import Path
from loguru import logger


def setup_custom_logger():
    base_dir = Path(__file__).resolve().parent

    log_path = base_dir / "log_files" / "app.log"

    logger.add(log_path, rotation="10 MB", encoding="utf-8")