"""
Passport Manager V2 — стейт-машина жизненного цикла сделки.
ЕДИНСТВЕННЫЙ легальный способ сменить статус паспорта: _set_state().
Любой переход вне таблицы разрешённых переходов → TransitionError.
"""
import uuid
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Set

logger = logging.getLogger("plato.passport_manager")


class TransitionError(Exception):
    """Попытка нелегального перехода состояния."""
    pass


# ============================================================
# ТАБЛИЦА РАЗРЕШЁННЫХ ПЕРЕХОДОВ (нервная система PLATO V2)
# ============================================================
ALLOWED_TRANSITIONS: Dict[str, Set[str]] = {
    "IDLE":               {"SIGNAL_GENERATED", "ORDER_PREPARING", "ORDER_SENT", 
                           "POSITION_OPEN", "PARTIAL_FILL", "TP1_PARTIAL_CLOSE",
                           "ORDER_CANCELLED", "ORDER_REJECTED", "ERROR_STATE"},
    "SIGNAL_GENERATED":   {"ORDER_PREPARING", "ORDER_SENT", "POSITION_OPEN", 
                           "PARTIAL_FILL", "TP1_PARTIAL_CLOSE",
                           "ORDER_CANCELLED", "ORDER_REJECTED", "ERROR_STATE"},
    "ORDER_PREPARING":    {"ORDER_SENT", "POSITION_OPEN", "PARTIAL_FILL",
                           "ORDER_CANCELLED", "ORDER_REJECTED", "ERROR_STATE"},
    "ORDER_SENT":         {"ORDER_PENDING", "PARTIAL_FILL", "POSITION_OPEN",
                           "ORDER_CANCELLED", "ORDER_REJECTED", "ERROR_STATE"},
    "ORDER_PENDING":      {"PARTIAL_FILL", "POSITION_OPEN", "ORDER_CANCELLED", "ERROR_STATE"},
    "PARTIAL_FILL":       {"POSITION_OPEN", "TP1_PARTIAL_CLOSE", "ORDER_CANCELLED", "ERROR_STATE"},
    "POSITION_OPEN":      {"TP1_PARTIAL_CLOSE", "TP_FULL_CLOSE", "SL_TRIGGERED",
                           "MANUAL_CLOSE", "CLOSED", "ERROR_STATE"},
    "TP1_PARTIAL_CLOSE":  {"TP_FULL_CLOSE", "SL_TRIGGERED", "MANUAL_CLOSE", "CLOSED", "ERROR_STATE"},
    "TP_FULL_CLOSE":      {"CLOSED"},
    "SL_TRIGGERED":       {"CLOSED"},
    "MANUAL_CLOSE":       {"CLOSED"},
    "CLOSED":             set(),
    "ORDER_CANCELLED":    set(),
    "ORDER_REJECTED":     set(),
    # ✅ ИСПРАВЛЕНО: Разрешаем аварийный выход из ошибки в CLOSED
    "ERROR_STATE":        {"CLOSED"}, 
}

TERMINAL_STATES = {"CLOSED", "ORDER_CANCELLED", "ORDER_REJECTED", "ERROR_STATE"}
ACTIVE_STATES = set(ALLOWED_TRANSITIONS.keys()) - TERMINAL_STATES


def is_terminal(status: str) -> bool:
    return status in TERMINAL_STATES


def generate_client_order_id(passport_id: str, signal_id: str) -> str:
    """Уникальный ключ сопоставления UDS<->REST (правило 3 матрицы)."""
    short_passport = passport_id.split("_")[-1] if "_" in passport_id else passport_id[:6]
    short_signal = signal_id.split("_")[-1] if "_" in signal_id else signal_id[:8]
    uid = uuid.uuid4().hex[:8]
    return f"PLT_{short_passport}_{short_signal}_{uid}"[:36]


class PassportManager:
    """Обёртка над TradePassport: владеет состоянием и таймлайном."""

    def __init__(self, passport):
        self.passport = passport
        if not getattr(self.passport, "status", None) or self.passport.status == "PENDING":
            self.passport.status = "IDLE"
        if not getattr(self.passport, "client_order_id", None):
            self.passport.client_order_id = generate_client_order_id(
                self.passport.passport_id,
                getattr(self.passport, "signal_id", "") or ""
            )

    def set_state(self, new_status: str, reason: str = "",
                  details: str = "", source: str = "SYSTEM") -> bool:
        current = self.passport.status

        if new_status == current:
            return False

        allowed = ALLOWED_TRANSITIONS.get(current, set())
        if new_status not in allowed:
            msg = (f"🚫 НЕЛЕГАЛЬНЫЙ ПЕРЕХОД: {current} -> {new_status} "
                   f"(passport={self.passport.passport_id}, reason={reason})")
            logger.error(msg)
            raise TransitionError(msg)

        self.passport.status = new_status
        self._append_event(f"STATUS: {new_status}", details or reason, source)

        if is_terminal(new_status) and not getattr(self.passport, "closed_at", None):
            self.passport.closed_at = datetime.now(timezone.utc)

        logger.info(f"🔄 [{self.passport.passport_id}] {current} -> {new_status} | {reason}")
        return True

    # ✅ НОВОЕ: Метод для аварийного закрытия (Fail-Safe)
    def force_close(self, reason: str):
        """Принудительное закрытие проблемного паспорта, игнорируя стандартные переходы."""
        old_state = self.passport.status
        if old_state == "CLOSED":
            return
        
        self.passport.status = "CLOSED"
        self.passport.close_reason = f"FORCE_CLOSED: {reason}"
        self.passport.closed_at = datetime.now(timezone.utc)
        
        self._append_event("STATUS: CLOSED (FORCE)", reason, source="RECON_FIX")
        logger.warning(f"🔧 Passport {self.passport.passport_id} FORCE_CLOSED from {old_state}. Reason: {reason}")

    # ✅ НОВОЕ: Помощник для RECON, чтобы не трогать молодые ордера
    def get_state_age_seconds(self) -> float:
        """Возвращает время в секундах с момента последнего изменения статуса."""
        timeline = getattr(self.passport, "timeline", [])
        current_state = self.passport.status
        
        for event in reversed(timeline):
            if event.get("event", "").startswith("STATUS:"):
                try:
                    ts_str = event["timestamp"].replace("Z", "+00:00")
                    ts = datetime.fromisoformat(ts_str)
                    return (datetime.now(timezone.utc) - ts).total_seconds()
                except Exception:
                    pass
        return 0.0

    def _append_event(self, event: str, details: str, source: str = "SYSTEM"):
        timeline = getattr(self.passport, "timeline", None)
        if timeline is None:
            self.passport.timeline = []
            timeline = self.passport.timeline
        timeline.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event": event,
            "details": details,
            "source": source,
        })
        self.passport.updated_at = datetime.now(timezone.utc)

    def log_guard_registered(self, sl: float, tp1: float, tp2: float, be: Optional[float] = None):
        be_txt = f", BE: {be}" if be is not None else ""
        self._append_event("GUARD_REGISTERED",
                           f"Защита выставлена. SL: {sl}, TP1: {tp1}, TP2: {tp2}{be_txt}",
                           source="GUARD")

    def log_recon(self, action: str, details: str):
        self._append_event(f"RECON: {action}", details, source="RECON")

    def log_exchange_verified(self, order_id: str, fill_price: float, qty: float):
        self._append_event("EXCHANGE_VERIFIED",
                           f"Исполнение подтверждено биржей. order={order_id}, price={fill_price}, qty={qty}",
                           source="UDS")

    def is_active(self) -> bool:
        return not is_terminal(self.passport.status)

    def current_state(self) -> str:
        return self.passport.status