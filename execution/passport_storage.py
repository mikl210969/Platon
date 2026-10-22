"""
Хранилище паспортов сделок. Сохраняет их в формате JSON.
V2: Один файл на паспорт (перезаписывается при каждом обновлении).
"""
import json
import logging
from pathlib import Path
from execution.passport import TradePassport

logger = logging.getLogger("plato.passport_storage")

class PassportStorage:
    def __init__(self, base_dir: str = "data/passports"):
        self.storage_dir = Path(base_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"💾 Хранилище паспортов инициализировано: {self.storage_dir.absolute()}")

    def save(self, passport: TradePassport):
        # ✅ ИСПРАВЛЕНО: Убрали статус из имени файла. Теперь файл один на сделку.
        filename = f"{passport.passport_id}_{passport.symbol}.json"
        file_path = self.storage_dir / filename
        
        try:
            data = passport.to_dict()
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            # Логируем только при смене статуса или первом сохранении, чтобы не спамить
            # logger.debug(f"💾 Паспорт обновлен: {file_path.name} (status={passport.status})")
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения паспорта {passport.passport_id}: {e}")