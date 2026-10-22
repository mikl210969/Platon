"""
Reconciliation Engine V2 — источник истины «биржа <-> паспорт».
Независимый от UDS канал правды через REST. Убивает фантомные беззащитные сделки.

Правила:
  R1 — стартовая сверка при запуске (до включения стратегии)
  R2 — периодическая сверка каждые INTERVAL_SEC
  R3 — UDS-пульсометр: тишина прикладных событий > SILENCE_SEC → форс-сверка
  R4 — усыновление фантомов / чистка сирот / финализация по истории сделок
Все действия пишут событие RECON:* в таймлайн паспорта.
"""
import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Optional, List, Any

from core.event_bus import EventBus
from adapters.bybit_rest import BybitRestClient
from execution.passport_storage import PassportStorage
from execution.passport_manager import is_terminal

logger = logging.getLogger("plato.recon_engine")


class ReconciliationEngine:
    def __init__(self, event_bus: EventBus, rest: BybitRestClient,
                 storage: PassportStorage, get_active_passports, get_manager,
                 order_manager=None,
                 symbol: str = "SOLUSDT", category: str = "linear",
                 interval_sec: int = 30, silence_sec: int = 300):
        self.bus = event_bus
        self.rest = rest
        self.storage = storage
        self.get_active_passports = get_active_passports   # callable -> dict[id, passport]
        self.get_manager = get_manager                     # callable(passport) -> PassportManager
        self.om = order_manager
        self.symbol = symbol
        self.category = category
        self.interval_sec = interval_sec
        self.silence_sec = silence_sec

        self._last_uds_event_ts: float = datetime.now(timezone.utc).timestamp()
        self._task: Optional[asyncio.Task] = None
        self._running = False

        # Подписываемся на прикладные UDS-события для пульсометра (R3)
        self.bus.subscribe("UDS:ORDER_UPDATE", self._on_uds_activity)
        self.bus.subscribe("UDS:POSITION_UPDATE", self._on_uds_activity)
        self.bus.subscribe("UDS:WALLET_UPDATE", self._on_uds_activity)  # ✅ добавили
    # ------------------------------------------------------------
    # Управление жизненным циклом
    # ------------------------------------------------------------
    async def start(self):
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info(f"🛰️ [RECON] Запущен. interval={self.interval_sec}s, silence_threshold={self.silence_sec}s")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
        logger.info("🛰️ [RECON] Остановлен")

    def _on_uds_activity(self, _data):
        """Любое прикладное UDS-событие сбрасывает таймер тишины."""
        self._last_uds_event_ts = datetime.now(timezone.utc).timestamp()

    # ------------------------------------------------------------
    # Главный цикл
    # ------------------------------------------------------------
    async def _loop(self):
        # R1: стартовая сверка сразу после запуска
        try:
            await self.reconcile(reason="STARTUP")
        except Exception as e:
            logger.error(f"🛰️ [RECON] Стартовая сверка упала: {e}")

        while self._running:
            try:
                await asyncio.sleep(self.interval_sec)
                if not self._running:
                    break

                # R3: пульсометр — если UDS молчит дольше порога, форсируем сверку
                silence = datetime.now(timezone.utc).timestamp() - self._last_uds_event_ts
                if silence > self.silence_sec:
                    logger.warning(f"🛰️ [RECON] Тишина UDS {silence:.0f}s > {self.silence_sec}s → форс-сверка")
                    await self.reconcile(reason="UDS_SILENCE")
                    self._last_uds_event_ts = datetime.now(timezone.utc).timestamp()
                else:
                    await self.reconcile(reason="PERIODIC")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"🛰️ [RECON] Ошибка цикла сверки: {e}")
                await asyncio.sleep(5)

    # ------------------------------------------------------------
    # Ядро сверки
    # ------------------------------------------------------------
    async def reconcile(self, reason: str = "PERIODIC"):
        # 1. Забираем истину с биржи (один пакет REST)
        try:
            positions = await self.rest.get_positions(self.category, self.symbol)
            open_orders = await self.rest.get_open_orders(self.category, self.symbol)
        except Exception as e:
            logger.error(f"🛰️ [RECON] Не удалось получить данные биржи: {e}")
            return

        exchange_size = 0.0
        for pos in positions:
            if pos.get("symbol") == self.symbol:
                exchange_size += abs(float(pos.get("size", 0)))

        exchange_order_ids = {o.get("orderId") for o in open_orders if o.get("orderId")}
        active = self.get_active_passports()

        logger.info(f"🛰️ [RECON:{reason}] pos_size={exchange_size}, open_orders={len(exchange_order_ids)}, active_passports={len(active)}")

        # 2. Матрица R2: обходим активные паспорта
        for pid, passport in list(active.items()):
            mgr = self.get_manager(passport)
            known_order_ids = {o.get("order_id") for o in passport.orders if o.get("order_id")}

            # R2a: паспорт активен, позиции на бирже нет, защитных ордеров нет → финализация
            if mgr.is_active() and exchange_size == 0:
                protective_alive = known_order_ids & exchange_order_ids
                if not protective_alive:
                    await self._finalize_from_history(passport, mgr, reason)

            # R2b: Позиция на бирже есть, а защитных ордеров нет → восстанавливаем защиту
            if mgr.is_active() and exchange_size > 0:
                protective_alive = known_order_ids & exchange_order_ids
                
                if not protective_alive:
                    # ✅ КРИТИЧЕСКИЙ ФИКС: Если платформа "забыла" про исполнение (filled_qty=0),
                    # но биржа говорит, что позиция есть, мы принудительно синхронизируем состояние.
                    if passport.filled_qty == 0:
                        logger.warning(f"🛰️ [RECON] Рассинхрон: биржа имеет позицию {exchange_size}, а паспорт ждет входа. Принудительная синхронизация.")
                        passport.filled_qty = exchange_size
                        passport.position_size = exchange_size
                        
                        # Пытаемся взять среднюю цену входа с биржи, иначе оставляем плановую
                        pos_data = next((p for p in positions if p.get("symbol") == self.symbol), {})
                        avg_price = pos_data.get("avgPrice")
                        if avg_price is not None:
                            try:
                                passport.fill_price = float(avg_price)
                                passport.entry_price = passport.fill_price
                            except (TypeError, ValueError):
                                # Если не удалось сконвертировать, оставляем плановую цену
                                logger.warning(f"🛰️ [RECON] Не удалось получить avgPrice с биржи, используем плановую {passport.entry_price}")
                        
                        # Переводим паспорт в корректное состояние
                        mgr.set_state("POSITION_OPEN", reason="RECON: Synced with exchange position", source="RECON")
                        mgr.log_guard_registered(passport.sl_price, passport.tp1_price, passport.tp2_price, passport.be_price)

                    # Теперь, когда filled_qty > 0, ставим защиту
                    await self._reprotect(passport, mgr, reason)

        # 3. Сироты: ордера на бирже, которых нет ни в одном паспорте → отменить
        all_known_ids = set()
        for p in active.values():
            all_known_ids |= {o.get("order_id") for o in p.orders if o.get("order_id")}
        for oid in exchange_order_ids:
            if oid not in all_known_ids:
                await self._cancel_orphan(oid, reason)

        # 4. Фантом: позиция на бирже есть, а активного паспорта нет вообще
        if exchange_size > 0 and not any(self.get_manager(p).is_active() for p in active.values()):
            await self._adopt_phantom(exchange_size, positions, reason)

    # ------------------------------------------------------------
    # Действия RECON
    # ------------------------------------------------------------
    async def _finalize_from_history(self, passport, mgr, reason: str):
        """Позиции нет, ордеров нет, а паспорт активен → закрываем по истории сделок."""
        try:
            history = await self.rest.get_closed_pnl_history(self.category, self.symbol, limit=10)
        except Exception as e:
            logger.error(f"🛰️ [RECON] История PnL недоступна: {e}")
            return

        # Ищем последнее закрытие, совпадающее по символу
        last = None
        for rec in history:
            if rec.get("symbol") == self.symbol:
                last = rec
                break

        if last:
            cum_fee = float(last.get("cumFee", 0))
            closed_pnl = float(last.get("closedPnl", 0))
            avg_price = float(last.get("avgEntryPrice", passport.entry_price)) or passport.entry_price
            passport.commission = max(passport.commission, abs(cum_fee))
            # Если плечи не были учтены — кладём весь PnL в realized
            if passport.tp1_pnl == 0 and passport.tp2_pnl == 0 and passport.sl_pnl == 0:
                passport.realized_pnl = closed_pnl - passport.commission
                passport.gross_pnl = closed_pnl
            else:
                passport.realized_pnl = passport.gross_pnl - passport.commission
            passport.exit_price = avg_price
            if not passport.exit_reason:
                passport.exit_reason = "RECON_FLAT"
            mgr.log_recon("FINALIZED", f"Паспорт финализирован по истории. closedPnl={closed_pnl}, fee={cum_fee}")
        else:
            mgr.log_recon("FINALIZED", "Позиция исчезла, история пуста → закрытие без PnL-реконструкции")
            if not passport.exit_reason:
                passport.exit_reason = "RECON_FLAT"

        # Форс-переход в CLOSED (через разрешённые промежуточные, если нужно)
        cur = mgr.current_state()
        for step in self._path_to_closed(cur):
            mgr.set_state(step, reason=f"RECON:{reason}", source="RECON")
        passport.closed_at = passport.closed_at or datetime.now(timezone.utc)
        self.storage.save(passport)
        logger.info(f"🛰️ [RECON] [{passport.passport_id}] Финализирован → CLOSED ({reason})")

    async def _reprotect(self, passport, mgr, reason: str):
        """Позиция жива, а защиты на бирже нет → выставляем заново."""
        if not self.om:
            mgr.log_recon("REPROTECT_SKIPPED", "OrderManager недоступен")
            return
        try:
            await self.om.place_protective_orders(passport)
            mgr.log_recon("REPROTECTED", "Защитные ордера восстановлены по паспорту")
            self.storage.save(passport)
            logger.warning(f"🛰️ [RECON] [{passport.passport_id}] Защита восстановлена ({reason})")
        except Exception as e:
            mgr.log_recon("REPROTECT_FAILED", str(e))
            logger.error(f"🛰️ [RECON] Не удалось восстановить защиту: {e}")

    async def _cancel_orphan(self, order_id: str, reason: str):
        try:
            await self.rest.cancel_order(self.category, self.symbol, order_id)
            logger.warning(f"🛰️ [RECON] Ордер-сирота отменён: {order_id} ({reason})")
        except Exception as e:
            logger.error(f"🛰️ [RECON] Не удалось отменить сироту {order_id}: {e}")

    async def _adopt_phantom(self, exchange_size: float, positions: list, reason: str):
        """Позиция на бирже есть, паспорта нет → алерт + попытка защитить консервативным SL."""
        pos = next((p for p in positions if p.get("symbol") == self.symbol), {})
        side_raw = pos.get("side", "")
        entry = float(pos.get("avgPrice", 0))
        side = "LONG" if side_raw == "Buy" else "SHORT"
        logger.critical(
            f"🚨 [RECON] ФАНТОМНАЯ ПОЗИЦИЯ: {exchange_size} {self.symbol} {side} @ {entry}. "
            f"Паспорта нет! Требуется ручное вмешательство. ({reason})"
        )
        # Публикуем событие для внешнего алерта / будущей авто-логики
        await self.bus.publish("RECON:PHANTOM_DETECTED", {
            "symbol": self.symbol, "size": exchange_size, "side": side, "entry": entry
        })
        # Консервативная защита фантома (широкий SL ~1%), чтобы не улетел в ликвидацию
        if self.om and entry > 0:
            try:
                from execution.passport import TradePassport
                phantom = TradePassport(
                    symbol=self.symbol, side=side, entry_price=entry, fill_price=entry,
                    planned_size=exchange_size, position_size=exchange_size, filled_qty=exchange_size,
                    sl_price=entry * (0.99 if side == "LONG" else 1.01),
                    tp1_price=entry * (1.005 if side == "LONG" else 0.995),
                    tp2_price=entry * (1.01 if side == "LONG" else 0.99),
                    exchange_verified=True,
                )
                phantom.add_event("RECON: PHANTOM_ADOPTED",
                                  f"Усыновлена фантомная позиция {exchange_size} @ {entry}", source="RECON")
                await self.bus.publish("EXEC:NEW_TRADE", phantom)
                await self.om.place_protective_orders(phantom)
                logger.warning(f"🛰️ [RECON] Фантом усыновлён и защищён консервативными TP/SL: {phantom.passport_id}")
            except Exception as e:
                logger.error(f"🛰️ [RECON] Не удалось усыновить фантом: {e}")

    # ------------------------------------------------------------
    # Вспомогательное: путь из любого состояния в CLOSED по матрице
    # ------------------------------------------------------------
    @staticmethod
    def _path_to_closed(current: str) -> List[str]:
        routes = {
            "IDLE":              ["CLOSED"],  # нелегально напрямую — set_state заругается; обработаем ниже
            "SIGNAL_GENERATED":  ["ERROR_STATE"],
            "ORDER_PREPARING":   ["ERROR_STATE"],
            "ORDER_SENT":        ["ORDER_CANCELLED"],
            "ORDER_PENDING":     ["ORDER_CANCELLED"],
            "PARTIAL_FILL":      ["POSITION_OPEN", "TP_FULL_CLOSE", "CLOSED"],
            "POSITION_OPEN":     ["TP_FULL_CLOSE", "CLOSED"],
            "TP1_PARTIAL_CLOSE": ["TP_FULL_CLOSE", "CLOSED"],
            "TP_FULL_CLOSE":     ["CLOSED"],
            "SL_TRIGGERED":      ["CLOSED"],
            "MANUAL_CLOSE":      ["CLOSED"],
        }
        # Для состояний без прямого пути в CLOSED идём через ERROR_STATE? Нет —
        # RECON имеет право закрыть любую активную сделку. Используем прямой переход,
        # а таблица в passport_manager уже разрешает POSITION_OPEN/TP1->CLOSED.
        # Для ранних состояний (нет позиции) корректнее ORDER_CANCELLED/ERROR_STATE.
        if current in ("IDLE", "SIGNAL_GENERATED", "ORDER_PREPARING"):
            return ["ERROR_STATE"]
        if current in ("ORDER_SENT", "ORDER_PENDING"):
            return ["ORDER_CANCELLED"]
        return routes.get(current, ["CLOSED"])