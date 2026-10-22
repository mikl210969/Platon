"""
Сбор метрик нагрузки на REST и UDS.
"""

import time
from dataclasses import dataclass, field
from typing import Dict, List
from datetime import datetime

@dataclass
class RESTMetrics:
    """Метрики REST API."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_response_time: float = 0.0
    last_request_time: float = 0.0
    errors: List[str] = field(default_factory=list)
    
    def record_request(self, success: bool, response_time: float, error: str = ""):
        self.total_requests += 1
        self.last_request_time = time.time()
        self.total_response_time += response_time
        
        if success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1
            if error:
                self.errors.append(f"{datetime.now().isoformat()}: {error}")
                # Храним только последние 100 ошибок
                if len(self.errors) > 100:
                    self.errors = self.errors[-100:]
    
    @property
    def avg_response_time(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return self.total_response_time / self.total_requests
    
    @property
    def success_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return (self.successful_requests / self.total_requests) * 100
    
    def to_dict(self) -> dict:
        return {
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "avg_response_time_ms": round(self.avg_response_time * 1000, 2),
            "success_rate_percent": round(self.success_rate, 2),
            "last_request_ago_sec": round(time.time() - self.last_request_time, 2) if self.last_request_time > 0 else 0,
            "recent_errors": self.errors[-10:]
        }

@dataclass
class UDSMetrics:
    """Метрики WebSocket (UDS)."""
    total_messages: int = 0
    connection_attempts: int = 0
    successful_connections: int = 0
    failed_connections: int = 0
    reconnections: int = 0
    last_message_time: float = 0.0
    connection_start_time: float = 0.0
    messages_per_minute: float = 0.0
    
    def record_message(self):
        self.total_messages += 1
        self.last_message_time = time.time()
    
    def record_connection_attempt(self, success: bool):
        self.connection_attempts += 1
        if success:
            self.successful_connections += 1
            if self.connection_start_time == 0:
                self.connection_start_time = time.time()
            else:
                self.reconnections += 1
        else:
            self.failed_connections += 1
    
    @property
    def uptime_seconds(self) -> float:
        if self.connection_start_time == 0:
            return 0.0
        return time.time() - self.connection_start_time
    
    @property
    def messages_per_min(self) -> float:
        uptime = self.uptime_seconds
        if uptime == 0:
            return 0.0
        return (self.total_messages / uptime) * 60
    
    def to_dict(self) -> dict:
        return {
            "total_messages": self.total_messages,
            "connection_attempts": self.connection_attempts,
            "successful_connections": self.successful_connections,
            "failed_connections": self.failed_connections,
            "reconnections": self.reconnections,
            "uptime_seconds": round(self.uptime_seconds, 2),
            "messages_per_minute": round(self.messages_per_min, 2),
            "last_message_ago_sec": round(time.time() - self.last_message_time, 2) if self.last_message_time > 0 else 0
        }

class MetricsCollector:
    """Глобальный сборщик метрик."""
    def __init__(self):
        self.rest = RESTMetrics()
        self.uds = UDSMetrics()
    
    def get_summary(self) -> dict:
        return {
            "timestamp": datetime.now().isoformat(),
            "rest": self.rest.to_dict(),
            "uds": self.uds.to_dict()
        }

# Глобальный экземпляр
metrics = MetricsCollector()