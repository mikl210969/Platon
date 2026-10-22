"""
Паспорт сделки V2. Хранит полную историю торговой операции.
Чистая схема: план / исполнение / риск / PnL / выход + таймлайн.
Мусорные поля архива удалены. Статусы канонические (см. passport_manager.py).
"""
import uuid
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
from enum import Enum


class SignalSource(Enum):
    ADAPTIVE_ZONE = "AdaptiveZoneV1"
    MANUAL = "Manual"
    TEST = "SelfTest"


class MarketMode(Enum):
    CALM = "calm"
    NORMAL = "normal"
    VOLATILE = "volatile"


class CloseReason(Enum):
    TP1 = "TP1"
    TP2 = "TP2"
    SL = "SL"
    BE = "BE"
    TIMEOUT = "Timeout"
    MANUAL = "Manual"
    RECON_FLAT = "ReconFlat"
    ERROR = "Error"
    NONE = ""


# Структура sizing_info по умолчанию (оставлена для обратной совместимости отчётов)
DEFAULT_SIZING_INFO = {
    "base_risk_usdt": 30.0,
    "adjusted_risk_usdt": 30.0,
    "smart_multiplier": 1.0,
    "smart_reason": "Режим по умолчанию (аналитика не подключена)",
    "btc_trend": "UNKNOWN",
    "btc_regime": "UNKNOWN",
    "base_sl_distance": 0.5,
    "adjusted_sl_distance": 0.5,
    "sl_multiplier": 1.0,
    "sl_reason": "Базовый SL (×1.0)",
    "symbol_delta": 0.0,
    "sl_distance": 0.5,
    "final_quantity": 7.0,
    "max_size_cap": 10.0,
    "fallback_used": False,
}


@dataclass
class TradePassport:
    # === Идентификация ===
    passport_id: str = field(
        default_factory=lambda: f"PASS_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    )
    signal_id: Optional[str] = None
    strategy: str = "AdaptiveZoneV1"
    signal_source: SignalSource = SignalSource.TEST
    client_order_id: Optional[str] = None  # V2: ключ сопоставления UDS<->REST

    # === Инструмент и направление ===
    symbol: str = "SOLUSDT"
    side: str = "LONG"

    # === План (уровни сделки) ===
    entry_price: float = 0.0
    sl_price: float = 0.0
    tp1_price: float = 0.0
    tp2_price: float = 0.0
    be_price: Optional[float] = None

    # === Исполнение ===
    planned_size: float = 0.0
    position_size: float = 0.0
    filled_qty: float = 0.0
    fill_price: float = 0.0          # V2: каноническая цена входа (факт)
    exchange_verified: bool = False  # V2: исполнение подтверждено биржей

    # === Прозрачный риск (V2, баг 1 архива) ===
    target_risk_usdt: float = 30.0   # сколько хотели рискнуть
    actual_risk_usdt: float = 0.0    # сколько рискнули реально
    cap_applied: bool = False        # был ли объём обрезан потолком

    # === История ордеров ===
    orders: List[Dict[str, Any]] = field(default_factory=list)

    # === Временные метки ===
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    closed_at: Optional[datetime] = None

    # === Состояние (канонические статусы passport_manager) ===
    status: str = "IDLE"
    exit_reason: str = ""
    exit_price: float = 0.0

    # === P&L (только два канонических поля прибыли) ===
    gross_pnl: float = 0.0
    commission: float = 0.0
    realized_pnl: float = 0.0
    tp1_pnl: float = 0.0
    tp2_pnl: float = 0.0
    sl_pnl: float = 0.0

    # === Контекст sizing (обратная совместимость) ===
    sizing_info: Dict[str, Any] = field(default_factory=lambda: dict(DEFAULT_SIZING_INFO))

    # === Чёрный ящик ===
    timeline: List[Dict[str, Any]] = field(default_factory=list)

    # ------------------------------------------------------------
    # Расчёты
    # ------------------------------------------------------------
    def calculate_pnl(self, exit_price: float, qty: float) -> float:
        if self.side.upper() == "LONG":
            return (exit_price - self.entry_price) * qty
        else:
            return (self.entry_price - exit_price) * qty

    def calculate_leg_pnl(self, exit_price: float, closed_qty: float) -> float:
        """PnL одного плеча строго от закрытого объёма этого плеча.
        Исправляет баг архива (раньше считалось от полного position_size → ×2)."""
        if self.side.upper() == "LONG":
            return (exit_price - self.entry_price) * closed_qty
        else:
            return (self.entry_price - exit_price) * closed_qty

    def calculate_commission(self, price: float, qty: float, fee_rate: float = 0.00055) -> float:
        """Комиссия (0.055% для Bybit Demo)."""
        return price * qty * fee_rate

    # ------------------------------------------------------------
    # Таймлайн и ордера
    # ------------------------------------------------------------
    def add_event(self, event: str, details: str, source: str = "SYSTEM"):
        self.timeline.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event": event,
            "details": details,
            "source": source,
        })
        self.updated_at = datetime.now(timezone.utc)

    def add_order_record(self, order_id: str, order_type: str, status: str,
                         price: float, qty: float, client_order_id: Optional[str] = None):
        for order in self.orders:
            if order.get("order_id") == order_id:
                order["status"] = status
                order["updated_at"] = datetime.now(timezone.utc).isoformat()
                return
        rec = {
            "order_id": order_id,
            "type": order_type,
            "status": status,
            "price": price,
            "qty": qty,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        if client_order_id:
            rec["client_order_id"] = client_order_id
        self.orders.append(rec)

    # ------------------------------------------------------------
    # Сериализация
    # ------------------------------------------------------------
    def to_dict(self) -> dict:
        return {
            "passport_id": self.passport_id,
            "signal_id": self.signal_id,
            "strategy": self.strategy,
            "signal_source": self.signal_source.value if hasattr(self.signal_source, "value") else str(self.signal_source),
            "client_order_id": self.client_order_id,
            "symbol": self.symbol,
            "side": self.side,
            "status": self.status,
            # план
            "entry_price": self.entry_price,
            "sl_price": self.sl_price,
            "tp1_price": self.tp1_price,
            "tp2_price": self.tp2_price,
            "be_price": self.be_price,
            # исполнение
            "planned_size": self.planned_size,
            "position_size": self.position_size,
            "filled_qty": self.filled_qty,
            "fill_price": self.fill_price,
            "exchange_verified": self.exchange_verified,
            # риск
            "target_risk_usdt": self.target_risk_usdt,
            "actual_risk_usdt": self.actual_risk_usdt,
            "cap_applied": self.cap_applied,
            # ордера и время
            "orders": self.orders,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "closed_at": self.closed_at.isoformat() if self.closed_at else None,
            "timeline": self.timeline,
            # выход и PnL
            "exit_reason": self.exit_reason,
            "exit_price": self.exit_price,
            "gross_pnl": self.gross_pnl,
            "commission": self.commission,
            "realized_pnl": self.realized_pnl,
            "tp1_pnl": self.tp1_pnl,
            "tp2_pnl": self.tp2_pnl,
            "sl_pnl": self.sl_pnl,
            "sizing_info": self.sizing_info,
        }