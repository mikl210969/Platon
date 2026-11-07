"""
Wall Persistence Tracker for Golden Bounce Strategy.
Stateful service that tracks wall confirmation, fill/pull detection via trade tape,
and adaptive timeouts. Emits lifecycle events to the strategy layer.
"""

import logging
import math
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("plato.wall_tracker")


class WallStatus(Enum):
    """Lifecycle states of a tracked wall."""
    DETECTING = "DETECTING"       
    CONFIRMED = "CONFIRMED"       
    PARTIALLY_FILLED = "PARTIALLY_FILLED"  
    CONSUMED = "CONSUMED"         
    GONE = "GONE"                 


@dataclass
class TrackedWall:
    """Internal state for a single tracked price level."""
    price: float
    side: str  # "bid" or "ask"
    created_at: datetime
    
    # Stickiness & Confirmation
    consecutive_ticks: int = 0
    first_detected_at: Optional[datetime] = None
    confirmed_at: Optional[datetime] = None
    status: WallStatus = WallStatus.DETECTING
    
    # Peak Volume (time-decayed)
    peak_volume: float = 0.0
    last_update_at: Optional[datetime] = None
    
    # Fill/Pull Detection
    partial_fill_start: Optional[datetime] = None
    consumption_rate_ewm: float = 0.0  # contracts/sec, EWM smoothed
    executed_since_partial: float = 0.0
    
    # Refill Scoring
    fill_refills: int = 0
    pull_cycles: int = 0
    last_refill_at: Optional[datetime] = None
    score_multiplier: float = 1.0
    
    # Deduplication
    last_trade_ts: int = 0  # Bybit trade timestamp (ms)


class WallPersistenceTracker:
    """
    Tracks walls at specific price levels with anti-spoofing logic.
    Stateful service: holds all raw state internally.
    """

    def __init__(self, config: dict):
        self.cfg = self._validate_config(config)
        
        # Active tracked walls: key = (price_rounded, side)
        self.walls: Dict[Tuple[float, str], TrackedWall] = {}
        
        # Trade tape buffer per symbol: deque[(ts_ms, price, qty)]
        self.tape_buffers: Dict[str, deque] = {}
        
        logger.info(f"🧱 WallPersistenceTracker initialized with cfg={self.cfg}")

    @staticmethod
    def _validate_config(cfg: dict) -> dict:
        """Validate and return sanitized config."""
        required = [
            "min_consecutive_ticks", "detect_window_sec", 
            "same_wall_price_tolerance_ticks", "min_wall_duration_sec",
            "half_life_sec", "partial_fill_min_ratio",
            "min_timeout_sec", "max_timeout_sec",
            "tape_window_ms", "tape_price_tolerance_ticks",
            "refill_ttl_sec", "refill_boost_per_event", 
            "refill_boost_max", "pull_cycle_demerit", "max_pull_cycles"
        ]
        for k in required:
            if k not in cfg:
                raise ValueError(f"Missing wall_persistence config key: {k}")
        
        assert 0 < cfg["partial_fill_min_ratio"] < 1, \
            f"partial_fill_min_ratio must be in (0,1), got {cfg['partial_fill_min_ratio']}"
        assert cfg["min_timeout_sec"] < cfg["max_timeout_sec"], \
            f"min_timeout ({cfg['min_timeout_sec']}) >= max_timeout ({cfg['max_timeout_sec']})"
        assert all(cfg[k] > 0 for k in ["detect_window_sec", "half_life_sec", 
                                          "tape_window_ms", "refill_ttl_sec"]), \
            "All timing params must be positive"
            
        return cfg

    def _get_tape_buffer(self, symbol: str) -> deque:
        """Get or create tape buffer for symbol."""
        if symbol not in self.tape_buffers:
            window_size = int(self.cfg["tape_window_ms"] / 50)  # ~50ms avg trade interval
            self.tape_buffers[symbol] = deque(maxlen=max(window_size, 100))
        return self.tape_buffers[symbol]

    async def on_trades(self, trades: List[Dict], symbol: Optional[str] = None) -> None:
        """
        Ingest batch of trades from EVENT:TAP:{symbol}.
        ✅ ИСПРАВЛЕНО: Принимает trades как первый аргумент (от EventBus).
        Символ извлекается из контекста вызова или передается явно.
        """
        # Если symbol не передан явно, пытаемся определить его из контекста
        # В текущей архитектуре лучше передавать symbol явно при подписке,
        # но для совместимости с EventBus делаем гибкую обработку
        
        if symbol is None:
            # Fallback: если symbol не передан, пропускаем (или логируем)
            logger.warning("on_trades called without symbol, skipping")
            return
            
        buf = self._get_tape_buffer(symbol)
        dedup_count = 0
        
        for t in trades:
            ts_ms = t.get("T", 0)  # Bybit trade time in ms
            if ts_ms <= 0:
                continue
                
            # Dedup: skip if same or older than last seen
            if ts_ms <= self._last_seen_trade_ts(symbol):
                dedup_count += 1
                continue
                
            price = float(t.get("p", 0))
            qty = float(t.get("v", 0))
            buf.append((ts_ms, price, qty))
            self._set_last_seen_trade_ts(symbol, ts_ms)
        
        if dedup_count > 0:
            logger.debug(f"[{symbol}] Deduplicated {dedup_count} stale trades")

    def _last_seen_trade_ts(self, symbol: str) -> int:
        """Track last seen trade TS across all walls for dedup."""
        max_ts = 0
        for w in self.walls.values():
            if w.last_trade_ts > max_ts:
                max_ts = w.last_trade_ts
        return max_ts

    def _set_last_seen_trade_ts(self, symbol: str, ts: int):
        for w in self.walls.values():
            if w.last_trade_ts < ts:
                w.last_trade_ts = ts

    async def update_ob(
        self, 
        symbol: str, 
        price: float, 
        volume: float, 
        side: str,
        timestamp: datetime
    ) -> Optional[Dict]:
        """
        Process orderbook update for a specific price level.
        Returns event dict if status changed, else None.
        """
        key = (round(price, 8), side)
        now = timestamp
        
        # Get or create wall
        if key not in self.walls:
            wall = TrackedWall(price=price, side=side, created_at=now)
            self.walls[key] = wall
            logger.info(f"[{symbol}] New wall detected @ {price} ({side}) vol={volume:.0f}")
        else:
            wall = self.walls[key]
        
        # Skip if already terminal
        if wall.status in (WallStatus.CONSUMED, WallStatus.GONE):
            return None
        
        # Update peak volume with time decay
        self._update_peak(wall, volume, now)
        
        # Check if price moved outside tolerance → new wall
        tol_ticks = self.cfg["same_wall_price_tolerance_ticks"]
        tick_size = self._get_tick_size(symbol, price)
        price_diff_ticks = abs(price - wall.price) / tick_size
        
        if price_diff_ticks > tol_ticks:
            # Old wall gone, new one starts
            self._remove_wall(key)
            return await self.update_ob(symbol, price, volume, side, timestamp)
        
        # Main state machine
        event = await self._process_state(wall, volume, now, symbol)
        
        # Cleanup dead walls periodically
        self._cleanup_dead_walls(now)
        
        return event

    def _update_peak(self, wall: TrackedWall, current_vol: float, now: datetime):
        """Update peak volume with exponential time decay."""
        half_life = self.cfg["half_life_sec"]
        
        if wall.last_update_at is None:
            wall.peak_volume = current_vol
        else:
            elapsed = (now - wall.last_update_at).total_seconds()
            decay_factor = 2 ** (-elapsed / half_life)
            decayed_peak = wall.peak_volume * decay_factor
            
            # Floor: never decay below threshold-equivalent
            decayed_peak = max(decayed_peak, current_vol * 0.1)
            
            wall.peak_volume = max(current_vol, decayed_peak)
        
        wall.last_update_at = now

    async def _process_state(
        self, wall: TrackedWall, volume: float, now: datetime, symbol: str
    ) -> Optional[Dict]:
        """Core state machine logic."""
        old_status = wall.status
        event = None
        
        if wall.status == WallStatus.DETECTING:
            event = await self._handle_detecting(wall, volume, now, symbol)
        elif wall.status == WallStatus.CONFIRMED:
            event = await self._handle_confirmed(wall, volume, now, symbol)
        elif wall.status == WallStatus.PARTIALLY_FILLED:
            event = await self._handle_partially_filled(wall, volume, now, symbol)
        
        # Log only transitions
        if event and wall.status != old_status:
            logger.info(
                f"[{symbol}] Wall {wall.price} ({wall.side}): "
                f"{old_status.value} → {wall.status.value} | "
                f"score={wall.score_multiplier:.2f}, "
                f"peak={wall.peak_volume:.0f}, "
                f"refills={wall.fill_refills}, "
                f"pulls={wall.pull_cycles}"
            )
        
        return event

    async def _handle_detecting(
        self, wall: TrackedWall, volume: float, now: datetime, symbol: str
    ) -> Optional[Dict]:
        """Hybrid stickiness filter: ticks OR time window."""
        wall.consecutive_ticks += 1
        
        min_ticks = self.cfg["min_consecutive_ticks"]
        detect_window = self.cfg["detect_window_sec"]
        
        passed_stickiness = False
        
        if wall.first_detected_at is None:
            wall.first_detected_at = now
        
        elapsed = (now - wall.first_detected_at).total_seconds()
        
        if wall.consecutive_ticks >= min_ticks or elapsed >= detect_window:
            passed_stickiness = True
        
        if not passed_stickiness:
            return None
        
        # Check minimum duration
        min_dur = self.cfg["min_wall_duration_sec"]
        if elapsed < min_dur:
            return None
        
        # CONFIRMED!
        wall.status = WallStatus.CONFIRMED
        wall.confirmed_at = now
        return {
            "event": "WALL_CONFIRMED",
            "symbol": symbol,
            "price": wall.price,
            "side": wall.side,
            "peak_volume": wall.peak_volume,
            "timestamp": now.isoformat(),
        }

    async def _handle_confirmed(
        self, wall: TrackedWall, volume: float, now: datetime, symbol: str
    ) -> Optional[Dict]:
        """Monitor for partial fill or disappearance."""
        ratio = volume / max(wall.peak_volume, 1.0)
        min_ratio = self.cfg["partial_fill_min_ratio"]
        
        if ratio < min_ratio:
            # Volume dropped significantly — check tape for fill vs pull
            exec_vol = self._get_executed_at_level(symbol, wall.price, now)
            
            if exec_vol > 0:
                # FILL confirmed via tape
                wall.status = WallStatus.PARTIALLY_FILLED
                wall.partial_fill_start = now
                wall.executed_since_partial = exec_vol
                wall.consumption_rate_ewm = exec_vol / 1.0  # initial rate
                return {
                    "event": "WALL_PARTIALLY_FILLED",
                    "symbol": symbol,
                    "price": wall.price,
                    "side": wall.side,
                    "executed_volume": exec_vol,
                    "remaining_ratio": ratio,
                    "timestamp": now.isoformat(),
                }
            else:
                # PULL detected — no tape confirmation
                wall.pull_cycles += 1
                demerit = self.cfg["pull_cycle_demerit"]
                wall.score_multiplier = max(
                    0.1, 
                    wall.score_multiplier * (1.0 - demerit)
                )
                
                max_pulls = self.cfg["max_pull_cycles"]
                if wall.pull_cycles >= max_pulls:
                    wall.status = WallStatus.GONE
                    return {
                        "event": "WALL_GONE_PULL_EXCEEDED",
                        "symbol": symbol,
                        "price": wall.price,
                        "side": wall.side,
                        "pull_cycles": wall.pull_cycles,
                        "timestamp": now.isoformat(),
                    }
                return None
        else:
            # Volume recovered — check if it's a refill after fill
            if wall.partial_fill_start is not None:
                # Was partially filled, now refilled
                refill_age = (now - wall.partial_fill_start).total_seconds()
                refill_ttl = self.cfg["refill_ttl_sec"]
                
                if refill_age <= refill_ttl:
                    wall.fill_refills += 1
                    boost = self.cfg["refill_boost_per_event"]
                    max_boost = self.cfg["refill_boost_max"]
                    wall.score_multiplier = min(
                        max_boost,
                        wall.score_multiplier + boost
                    )
                    wall.last_refill_at = now
                    wall.partial_fill_start = None  # reset partial state
                    
                    return {
                        "event": "WALL_REFILLED",
                        "symbol": symbol,
                        "price": wall.price,
                        "side": wall.side,
                        "fill_refills": wall.fill_refills,
                        "score_multiplier": wall.score_multiplier,
                        "timestamp": now.isoformat(),
                    }
        
        return None

    async def _handle_partially_filled(
        self, wall: TrackedWall, volume: float, now: datetime, symbol: str
    ) -> Optional[Dict]:
        """Adaptive timeout based on consumption rate."""
        exec_vol = self._get_executed_at_level(symbol, wall.price, now)
        
        if exec_vol > 0:
            # Update EWM consumption rate
            alpha = 0.3
            wall.consumption_rate_ewm = (
                alpha * (exec_vol / 1.0) + 
                (1 - alpha) * wall.consumption_rate_ewm
            )
            wall.executed_since_partial += exec_vol
        
        remaining = max(wall.peak_volume * self.cfg["partial_fill_min_ratio"] - volume, 0)
        
        # Adaptive timeout
        rate = max(wall.consumption_rate_ewm, 0.01)
        expected_time = remaining / rate
        k = 1.5
        timeout = max(
            self.cfg["min_timeout_sec"],
            min(self.cfg["max_timeout_sec"], expected_time * k)
        )
        
        if wall.partial_fill_start:
            elapsed = (now - wall.partial_fill_start).total_seconds()
            if elapsed > timeout:
                wall.status = WallStatus.GONE
                return {
                    "event": "WALL_GONE_TIMEOUT",
                    "symbol": symbol,
                    "price": wall.price,
                    "side": wall.side,
                    "elapsed": elapsed,
                    "timeout": timeout,
                    "timestamp": now.isoformat(),
                }
        
        # Check if fully consumed
        ratio = volume / max(wall.peak_volume, 1.0)
        if ratio < 0.05:  # <5% remaining
            wall.status = WallStatus.CONSUMED
            return {
                "event": "WALL_CONSUMED",
                "symbol": symbol,
                "price": wall.price,
                "side": wall.side,
                "total_executed": wall.executed_since_partial,
                "timestamp": now.isoformat(),
            }
        
        return None

    def _get_executed_at_level(
        self, symbol: str, price: float, now: datetime
    ) -> float:
        """Sum executed volume at price ± tolerance within tape window."""
        buf = self._get_tape_buffer(symbol)
        if not buf:
            return 0.0
        
        tol_ticks = self.cfg["tape_price_tolerance_ticks"]
        tick_size = self._get_tick_size(symbol, price)
        price_tol = tol_ticks * tick_size
        
        window_ms = self.cfg["tape_window_ms"]
        cutoff_ts = int(now.timestamp() * 1000) - window_ms
        
        total = 0.0
        for ts_ms, p, q in buf:
            if ts_ms < cutoff_ts:
                continue
            if abs(p - price) <= price_tol:
                total += q
        
        return total

    @staticmethod
    def _get_tick_size(symbol: str, price: float) -> float:
        """Return tick size for symbol. Simplified; replace with exchange metadata."""
        TICK_SIZES = {
            "SOLUSDT": 0.01,
            "LINKUSDT": 0.001,
            "ETHUSDT": 0.01,
            "BTCUSDT": 0.1,
        }
        return TICK_SIZES.get(symbol, 0.01)

    def _remove_wall(self, key: Tuple[float, str]):
        """Remove wall from tracking."""
        self.walls.pop(key, None)

    def _cleanup_dead_walls(self, now: datetime):
        """Remove walls that have been terminal for >60s."""
        to_remove = []
        for key, w in self.walls.items():
            if w.status in (WallStatus.GONE, WallStatus.CONSUMED):
                if w.last_update_at and (now - w.last_update_at).total_seconds() > 60:
                    to_remove.append(key)
        for key in to_remove:
            del self.walls[key]

    def get_active_walls(self, symbol: Optional[str] = None) -> List[Dict]:
        """Debug/monitoring: return snapshot of active walls."""
        result = []
        for (price, side), w in self.walls.items():
            # ✅ ИСПРАВЛЕНИЕ: Чистая и безопасная логика фильтрации
            if symbol is not None:
                base_symbol = symbol.split("_")[0] if "_" in symbol else symbol
                if w.side != base_symbol:
                    continue
            
            result.append({
                "price": price,
                "side": side,
                "status": w.status.value,
                "peak_volume": round(w.peak_volume, 2),
                "score_multiplier": round(w.score_multiplier, 3),
                "fill_refills": w.fill_refills,
                "pull_cycles": w.pull_cycles,
                "age_sec": round((datetime.now(timezone.utc) - w.created_at).total_seconds(), 1),
            })
        return result