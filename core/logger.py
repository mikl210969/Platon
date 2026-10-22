"""
Настройка логирования для платформы.
"""

import logging
import sys
from pathlib import Path

def setup_logger(name: str = "plato", log_file: str = "plato.log", level: str = "INFO") -> logging.Logger:
    """Настроить логгер с выводом в консоль и файл (защита от дублирования)."""
    
    logger = logging.getLogger(name)
    
    # Если хендлеры уже есть, не добавляем их снова (решает проблему дублирования)
    if logger.handlers:
        return logger
        
    logger.setLevel(getattr(logging, level.upper()))
    
    # Формат сообщений
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 1. Консольный вывод
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # 2. Файловый вывод (создаем файл в текущей директории)
    file_path = Path(log_file)
    file_handler = logging.FileHandler(file_path, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger