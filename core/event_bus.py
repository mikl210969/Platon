"""
EventBus — шина событий для связи между модулями.
Модули не знают друг о друге, они только публикуют и подписываются на события.
"""

import asyncio
from typing import Callable, Dict, List, Any
from datetime import datetime


class EventBus:
    """Асинхронная шина событий."""
    
    def __init__(self):
        self._subscribers: Dict[str, List[Callable]] = {}
        self._message_log: List[Dict[str, Any]] = []
    
    def subscribe(self, event_name: str, callback: Callable) -> None:
        """Подписаться на событие."""
        if event_name not in self._subscribers:
            self._subscribers[event_name] = []
        self._subscribers[event_name].append(callback)
    
    async def publish(self, event_name: str, data: Any = None) -> None:
        """Опубликовать событие и вызвать всех подписчиков."""
        # Логируем событие
        self._message_log.append({
            "timestamp": datetime.now().isoformat(),
            "event": event_name,
            "data": data
        })
        
        # Вызываем подписчиков
        if event_name in self._subscribers:
            for callback in self._subscribers[event_name]:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(data)
                    else:
                        callback(data)
                except Exception as e:
                    print(f"❌ Ошибка в обработчике события {event_name}: {e}")
    
    def get_log(self) -> List[Dict[str, Any]]:
        """Вернуть историю событий."""
        return self._message_log.copy()