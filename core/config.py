"""
Загрузка конфигурации из config.yaml.
"""

import yaml
from pathlib import Path
from typing import Dict, Any


class Config:
    """Загрузчик конфигурации."""
    
    def __init__(self, config_path: str = "config.yaml"):
        self._config = self._load_config(config_path)
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Загружает YAML файл."""
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Конфиг не найден: {config_path}")
        
        with open(path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def get(self, key: str, default: Any = None) -> Any:
        """Получить значение по ключу (поддержка вложенности через точку)."""
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return default
            
            if value is None:
                return default
        
        return value
    
    @property
    def raw(self) -> Dict[str, Any]:
        """Вернуть весь конфиг как словарь."""
        return self._config.copy()