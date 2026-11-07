"""
Zone Delta Tracker.
Отслеживает кумулятивную дельту для конкретных активных зон.
"""
import logging
from typing import Dict

logger = logging.getLogger("plato.delta_tracker")

class ZoneDeltaTracker:
    def __init__(self):
        # Словарь: zone_id -> кумулятивная дельта
        self.zone_deltas: Dict[str, float] = {}

    def register_zone(self, zone_id: str):
        """Регистрирует новую зону для отслеживания."""
        if zone_id not in self.zone_deltas:
            self.zone_deltas[zone_id] = 0.0

    def unregister_zone(self, zone_id: str):
        """Удаляет зону из отслеживания (когда она неактивна)."""
        self.zone_deltas.pop(zone_id, None)

    def add_tick(self, zone_id: str, signed_qty: float):
        """Добавляет объем тика к кумулятивной дельте зоны."""
        if zone_id in self.zone_deltas:
            self.zone_deltas[zone_id] += signed_qty

    def get_delta(self, zone_id: str) -> float:
        """Возвращает текущую кумулятивную дельту для зоны."""
        return self.zone_deltas.get(zone_id, 0.0)