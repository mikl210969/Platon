"""
ПОДБОР ТАЙМАУТОВ ЗОНЫ: DEPARTURE_MAX_BARS × RETURN_MAX_BARS
База: Wall_Mult = 4.0, tp2_protect, Жесткий R:R (1.5x / 3.0x), без fallback
"""
import pandas as pd
import numpy as np
import os
from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple

# ================= CONFIGURATION =================
DATA_DIR = r"C:\Users\ongul\YandexDisk\Data"
DATES = ["2026-09-28", "2026-09-29", "2026-09-30", "2026-10-01"]
SYMBOL = "SOLUSDT"

# Общие параметры
WALL_FLOOR = 1000.0
K_ATR_DEPARTURE = 1.5
MAX_DEPARTURE_PCT = 4.0
ZONE_CORRIDOR_PCT = 0.5
COMMISSION_RATE = 0.0004
SOFT_TTL_BARS = 576

# Параметры риска
TARGET_RISK_USDT = 30.0
MIN_QTY_SOL = 0.1
MAX_QTY_SOL = 10.0

# Параметры фильтрации
WALL_MULTIPLE = 4.0
WALL_BASELINE_BARS = 28
SL_BUFFER_MULT_5M = 1.0

# Жесткий R:R
TP1_RR_MULT = 1.5
TP2_RR_MULT = 3.0

# === ПАРАМЕТРЫ ДЛЯ ТЕСТА ТАЙМАУТОВ ===
# (departure_max_bars, return_max_bars)
TIMEOUT_COMBOS = [
    (24, 24),   # 2ч + 2ч = до 4ч всего
    (30, 24),   # 2.5ч + 2ч = до 4.5ч всего
    (36, 24),   # 3ч + 2ч = до 5ч всего
    (24, 36),   # 2ч + 3ч = до 5ч всего
    (36, 36),   # 3ч + 3ч = до 6ч всего
    (48, 24),   # 4ч + 2ч = до 6ч всего
    (48, 48),   # 4ч + 4ч = до 8ч всего
]

# ================================================

@dataclass
class TradeResult:
    date: str
    time: str
    direction: str
    entry_price: float
    sl_price: float
    tp1_price: float
    tp2_price: float
    quantity_sol: float
    risk_usdt: float
    exit_reason: str
    gross_pnl_pct: float
    net_pnl_pct: float
    pnl_usdt: float
    hold_hours: float
    bars_to_entry: int       # баров от создания зоны до входа
    dep_bars: int            # баров от создания зоны до отката
    ret_bars: int            # баров от отката до возврата


def load_and_prepare_data():
    print("📂 Загрузка данных...")
    all_depth, all_trades = [], []

    for d in DATES:
        depth_file = os.path.join(DATA_DIR, f"{SYMBOL}_depth_{d}.csv")
        trades_file = os.path.join(DATA_DIR, f"{SYMBOL}_aggTrades_{d}.csv")
        if not os.path.exists(depth_file) or not os.path.exists(trades_file):
            continue
        df_d = pd.read_csv(depth_file)
        df_d['timestamp'] = pd.to_datetime(df_d['timestamp'])
        all_depth.append(df_d)

        df_t = pd.read_csv(trades_file)
        df_t['timestamp'] = pd.to_datetime(df_t['timestamp'], unit='ms')
        all_trades.append(df_t)

    if not all_depth:
        raise FileNotFoundError("Данные не найдены!")

    df_depth_all = pd.concat(all_depth, ignore_index=True)
    df_trades_all = pd.concat(all_trades, ignore_index=True)

    df_depth_all['interval_5m'] = df_depth_all['timestamp'].dt.floor('5min')
    depth_5m = df_depth_all.groupby('interval_5m').agg({
        'bid_v_1': ['max', 'mean'], 'ask_v_1': ['max', 'mean'],
        'bid_p_1': 'last', 'ask_p_1': 'last'
    }).reset_index()
    depth_5m.columns = ['interval_5m', 'bid_v1_max', 'bid_v1_mean', 'ask_v1_max', 'ask_v1_mean', 'bid_p1', 'ask_p1']

    df_trades_all['is_buyer'] = ~df_trades_all['is_buyer_maker'].astype(bool)
    df_trades_all['interval_5m'] = df_trades_all['timestamp'].dt.floor('5min')

    candles_5m = df_trades_all.groupby('interval_5m').agg({
        'price': ['first', 'max', 'min', 'last'], 'quantity': 'sum'
    }).reset_index()
    candles_5m.columns = ['interval_5m', 'open', 'high', 'low', 'close', 'volume']

    buyer_vol = df_trades_all[df_trades_all['is_buyer']].groupby('interval_5m')['quantity'].sum()
    seller_vol = df_trades_all[~df_trades_all['is_buyer']].groupby('interval_5m')['quantity'].sum()
    candles_5m = candles_5m.merge(buyer_vol.rename('buyer_vol'), on='interval_5m', how='left').fillna(0)
    candles_5m = candles_5m.merge(seller_vol.rename('seller_vol'), on='interval_5m', how='left').fillna(0)
    candles_5m['delta'] = candles_5m['buyer_vol'] - candles_5m['seller_vol']

    df_5m = candles_5m.merge(depth_5m, on='interval_5m', how='left').sort_values('interval_5m').reset_index(drop=True)
    for col in ['bid_v1_max', 'ask_v1_max', 'bid_v1_mean', 'ask_v1_mean']:
        if col in df_5m.columns:
            df_5m[col] = df_5m[col].ffill().fillna(0)

    high_low = df_5m['high'] - df_5m['low']
    high_close = np.abs(df_5m['high'] - df_5m['close'].shift())
    low_close = np.abs(df_5m['low'] - df_5m['close'].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = np.max(ranges, axis=1)
    df_5m['atr_5m'] = true_range.rolling(window=14).mean()

    df_5m['date_str'] = df_5m['interval_5m'].dt.strftime('%Y-%m-%d')

    print(f"✅ Данные готовы: {len(df_5m)} свечей 5m.")
    return df_5m


def find_zones(df: pd.DataFrame) -> List[dict]:
    zones = []
    for idx, row in df.iterrows():
        if np.isnan(row['atr_5m']):
            continue
        for col, typ, direction in [('bid_v1_max', 'BID', 'LONG'), ('ask_v1_max', 'ASK', 'SHORT')]:
            v = row[col]
            if not np.isfinite(v) or v <= WALL_FLOOR:
                continue
            hist = df[col].iloc[max(0, idx - WALL_BASELINE_BARS):idx]
            hist = hist[hist > 0]
            if len(hist) < 5:
                continue
            ref = float(hist.median())
            if ref <= 0 or v < WALL_MULTIPLE * ref:
                continue
            zones.append({
                'idx': idx, 'date': row['date_str'], 'time': row['interval_5m'],
                'price': row['close'], 'type': typ, 'direction': direction,
                'volume': v, 'atr_5m': row['atr_5m']
            })
    return zones


def detect_real_return(zone: dict, df: pd.DataFrame,
                       dep_max_bars: int, ret_max_bars: int) -> Optional[dict]:
    """Причинный поиск отката и возврата с настраиваемыми таймаутами."""
    z, p = zone['idx'], zone['price']
    is_long = zone['direction'] == 'LONG'
    min_dep = K_ATR_DEPARTURE * zone['atr_5m']
    max_dep = p * MAX_DEPARTURE_PCT / 100
    corridor = p * ZONE_CORRIDOR_PCT / 100
    dep_idx, extreme = None, None

    for j in range(z + 1, min(len(df), z + 1 + dep_max_bars)):
        r = df.iloc[j]
        adverse = (p - r['low']) if is_long else (r['high'] - p)

        if dep_idx is None:
            if adverse >= min_dep:
                if adverse > max_dep:
                    return None
                dep_idx, extreme = j, (r['low'] if is_long else r['high'])
            continue

        extreme = min(extreme, r['low']) if is_long else max(extreme, r['high'])
        dep_abs = (p - extreme) if is_long else (extreme - p)

        if dep_abs > max_dep:
            return None

        if j == dep_idx or j > dep_idx + ret_max_bars:
            continue

        if abs(r['close'] - p) <= corridor:
            return {
                'r_idx': int(j),
                'extreme': float(extreme),
                'dep_abs': float(dep_abs),
                'dep_idx': int(dep_idx)
            }
    return None


def simulate_trade(zone: dict, df: pd.DataFrame,
                   dep_max_bars: int, ret_max_bars: int) -> Optional[TradeResult]:
    z_idx = zone['idx']
    is_long = zone['direction'] == 'LONG'

    ret = detect_real_return(zone, df, dep_max_bars, ret_max_bars)
    if ret is None:
        return None

    r_idx = ret['r_idx']
    dep_idx = ret['dep_idx']
    entry = float(df.iloc[r_idx]['close'])

    buffer = zone['atr_5m'] * SL_BUFFER_MULT_5M
    sl = (ret['extreme'] - buffer) if is_long else (ret['extreme'] + buffer)

    risk_dist = abs(entry - sl)
    tp1 = entry + (risk_dist * TP1_RR_MULT) if is_long else entry - (risk_dist * TP1_RR_MULT)
    tp2 = entry + (risk_dist * TP2_RR_MULT) if is_long else entry - (risk_dist * TP2_RR_MULT)

    # Проверка дельты
    delta = df.loc[z_idx:r_idx, 'delta'].sum()
    if is_long and delta >= 0:
        return None
    if not is_long and delta <= 0:
        return None

    # Расчет объема
    sl_dist_usd = abs(entry - sl)
    final_qty = max(MIN_QTY_SOL, min(
        TARGET_RISK_USDT / sl_dist_usd if sl_dist_usd > 0 else MIN_QTY_SOL,
        MAX_QTY_SOL
    ))
    actual_risk_usdt = sl_dist_usd * final_qty

    # Метрики времени
    bars_to_entry = r_idx - z_idx
    dep_bars = dep_idx - z_idx
    ret_bars = r_idx - dep_idx

    # Симуляция выхода (tp2_protect)
    exit_details, gross_pnl = "", 0.0
    tp1_hit, sl_hit, tp2_hit = False, False, False
    exit_idx = r_idx
    max_idx = min(r_idx + SOFT_TTL_BARS, len(df) - 1)
    current_sl = sl
    tp2_buffer = entry * COMMISSION_RATE * 3.0

    for ex_idx in range(r_idx + 1, max_idx + 1):
        ec = df.iloc[ex_idx]

        if is_long:
            if ec['low'] <= current_sl and not tp1_hit:
                sl_hit = True; exit_idx = ex_idx
                exit_details = "SL"
                gross_pnl = -((entry - current_sl) / entry * 100); break
            if ec['high'] >= tp1 and not tp1_hit:
                tp1_hit = True; exit_idx = ex_idx
                pnl_50 = ((tp1 - entry) / entry * 100) * 0.5
                current_sl = tp2 + tp2_buffer
                if ec['high'] >= tp2:
                    tp2_hit = True; exit_idx = ex_idx
                    exit_details = "TP1+TP2"
                    gross_pnl = pnl_50 + ((tp2 - entry) / entry * 100) * 0.5; break
            elif tp1_hit and not (tp2_hit or sl_hit):
                if ec['low'] <= current_sl:
                    sl_hit = True; exit_idx = ex_idx
                    exit_details = "TP1+TP2_Protect"
                    gross_pnl += ((current_sl - entry) / entry * 100) * 0.5; break
                elif ec['high'] >= tp2:
                    tp2_hit = True; exit_idx = ex_idx
                    exit_details = "TP1+TP2"
                    gross_pnl += ((tp2 - entry) / entry * 100) * 0.5; break
        else:
            if ec['high'] >= current_sl and not tp1_hit:
                sl_hit = True; exit_idx = ex_idx
                exit_details = "SL"
                gross_pnl = -((current_sl - entry) / entry * 100); break
            if ec['low'] <= tp1 and not tp1_hit:
                tp1_hit = True; exit_idx = ex_idx
                pnl_50 = ((entry - tp1) / entry * 100) * 0.5
                current_sl = tp2 - tp2_buffer
                if ec['low'] <= tp2:
                    tp2_hit = True; exit_idx = ex_idx
                    exit_details = "TP1+TP2"
                    gross_pnl = pnl_50 + ((entry - tp2) / entry * 100) * 0.5; break
            elif tp1_hit and not (tp2_hit or sl_hit):
                if ec['high'] >= current_sl:
                    sl_hit = True; exit_idx = ex_idx
                    exit_details = "TP1+TP2_Protect"
                    gross_pnl += ((entry - current_sl) / entry * 100) * 0.5; break
                elif ec['low'] <= tp2:
                    tp2_hit = True; exit_idx = ex_idx
                    exit_details = "TP1+TP2"
                    gross_pnl += ((entry - tp2) / entry * 100) * 0.5; break

    if not (sl_hit or tp2_hit):
        last_c = df.iloc[max_idx]
        if tp1_hit:
            gross_pnl += ((last_c['close'] - entry) / entry * 100) * 0.5 \
                if is_long else ((entry - last_c['close']) / entry * 100) * 0.5
            exit_details = "TP1+EOD"
        else:
            gross_pnl = ((last_c['close'] - entry) / entry * 100) \
                if is_long else ((entry - last_c['close']) / entry * 100)
            exit_details = "EOD"
        exit_idx = max_idx

    fee_actions = 1
    if "TP1" in exit_details:
        fee_actions += 1
    if any(k in exit_details for k in ("TP2", "EOD", "SL", "Protect")):
        fee_actions += 1
    total_fee = fee_actions * COMMISSION_RATE * 100
    net_pnl = gross_pnl - total_fee
    pnl_usdt = (net_pnl / 100) * (final_qty * entry)
    hold_hours = (exit_idx - r_idx) * 5 / 60

    return TradeResult(
        zone['date'], zone['time'].strftime('%H:%M'), zone['direction'],
        entry, sl, tp1, tp2, final_qty, actual_risk_usdt, exit_details,
        gross_pnl, net_pnl, pnl_usdt, hold_hours,
        bars_to_entry, dep_bars, ret_bars
    )


def calculate_max_drawdown(pnls: List[float]) -> float:
    equity, peak, max_dd = 100.0, 100.0, 0.0
    for p in pnls:
        equity *= (1 + p / 100)
        if equity > peak:
            peak = equity
        dd = (peak - equity) / peak * 100
        if dd > max_dd:
            max_dd = dd
    return max_dd


def analyze_results(trades: List[TradeResult]) -> dict:
    if not trades:
        return {
            "Trades": 0, "Win Rate %": 0, "Net P&L %": 0,
            "Total PnL $": 0, "Profit Factor": 0, "Max Drawdown %": 0,
            "Avg Bars to Entry": 0, "Max Bars to Entry": 0,
            "Avg Dep Bars": 0, "Avg Ret Bars": 0, "trades": []
        }
    total_net_pct = sum(t.net_pnl_pct for t in trades)
    total_pnl_usdt = sum(t.pnl_usdt for t in trades)
    wins = sum(1 for t in trades if t.net_pnl_pct > 0)
    wr = wins / len(trades) * 100
    pf_num = sum(t.net_pnl_pct for t in trades if t.net_pnl_pct > 0)
    pf_den = abs(sum(t.net_pnl_pct for t in trades if t.net_pnl_pct < 0))
    pf = pf_num / pf_den if pf_den > 0 else 999
    max_dd = calculate_max_drawdown([t.net_pnl_pct for t in trades])

    return {
        "Trades": len(trades),
        "Win Rate %": round(wr, 1),
        "Net P&L %": round(total_net_pct, 2),
        "Total PnL $": round(total_pnl_usdt, 2),
        "Profit Factor": round(pf, 2),
        "Max Drawdown %": round(max_dd, 2),
        "Avg Bars to Entry": round(np.mean([t.bars_to_entry for t in trades]), 1),
        "Max Bars to Entry": max(t.bars_to_entry for t in trades),
        "Avg Dep Bars": round(np.mean([t.dep_bars for t in trades]), 1),
        "Avg Ret Bars": round(np.mean([t.ret_bars for t in trades]), 1),
        "trades": trades
    }


def print_exit_analysis(trades: List[TradeResult], label: str):
    print(f"\n{'='*95}")
    print(f" 🔬 ДЕТАЛЬНЫЙ АНАЛИЗ ВЫХОДОВ ({label})")
    print(f"{'='*95}")
    exit_reasons: Dict[str, list] = {}
    for t in trades:
        exit_reasons.setdefault(t.exit_reason, []).append(t)

    print(f"\n{'Причина':<20} | {'Кол-во':<6} | {'%':<6} | {'Avg PnL%':<10} | {'Total $':<12} | {'Avg Hold':<10}")
    print("-" * 95)
    total = len(trades)
    for reason in sorted(exit_reasons.keys()):
        g = exit_reasons[reason]
        cnt = len(g)
        avg_p = np.mean([t.net_pnl_pct for t in g])
        tot_p = sum(t.pnl_usdt for t in g)
        avg_h = np.mean([t.hold_hours for t in g])
        print(f"{reason:<20} | {cnt:<6} | {cnt/total*100:>5.1f}% | {avg_p:>9.3f}% | ${tot_p:>10.2f} | {avg_h:>8.1f}ч")
    print("=" * 95)


def run_test():
    print("=" * 100)
    print(" ПОДБОР ТАЙМАУТОВ: DEPARTURE_MAX_BARS × RETURN_MAX_BARS")
    print(" База: Wall_Mult=4.0, tp2_protect, R:R 1.5/3.0, без fallback")
    print("=" * 100)

    df = load_and_prepare_data()
    zones = find_zones(df)
    total_zones = len(zones)
    print(f"🧱 Найдено {total_zones} пуленепробиваемых зон (Wall_Mult = 4.0).\n")

    results: Dict[Tuple[int, int], dict] = {}

    print("▶️ Запуск параметрического теста...")
    for dep_bars, ret_bars in TIMEOUT_COMBOS:
        label = f"Dep={dep_bars} Ret={ret_bars}"
        print(f"  • {label}...", end=" ")
        trades = []
        for zone in zones:
            res = simulate_trade(zone, df, dep_bars, ret_bars)
            if res is not None:
                trades.append(res)
        results[(dep_bars, ret_bars)] = analyze_results(trades)
        print(f"{len(trades)} сделок")

    # === СВОДНАЯ ТАБЛИЦА ===
    print("\n" + "=" * 120)
    print(" 📊 СВОДНАЯ ТАБЛИЦА РЕЗУЛЬТАТОВ")
    print("=" * 120)
    hdr = (f"{'Dep':>4} {'Ret':>4} | {'Всего':>5} | {'Сделок':>6} | {'Конв%':>6} | "
           f"{'WR%':>6} | {'NetPnL%':>8} | {'PnL$':>10} | {'PF':>6} | {'DD%':>6} | "
           f"{'AvgBars':>7} | {'MaxBars':>7} | {'AvgDep':>6} | {'AvgRet':>6} | Вердикт")
    print(hdr)
    print("-" * 120)

    best_key, best_pf = None, -1
    for dep_bars, ret_bars in TIMEOUT_COMBOS:
        r = results[(dep_bars, ret_bars)]
        conv = r['Trades'] / total_zones * 100 if total_zones > 0 else 0
        verdict = ""
        if r['Trades'] >= 5 and r['Profit Factor'] > best_pf:
            best_pf = r['Profit Factor']
            best_key = (dep_bars, ret_bars)
            verdict = "🏆"
        print(f"{dep_bars:>4} {ret_bars:>4} | {total_zones:>5} | {r['Trades']:>6} | {conv:>5.1f}% | "
              f"{r['Win Rate %']:>5.1f} | {r['Net P&L %']:>7.2f} | ${r['Total PnL $']:>8.2f} | "
              f"{r['Profit Factor']:>5.2f} | {r['Max Drawdown %']:>5.2f} | "
              f"{r['Avg Bars to Entry']:>6.1f} | {r['Max Bars to Entry']:>7} | "
              f"{r['Avg Dep Bars']:>5.1f} | {r['Avg Ret Bars']:>5.1f} | {verdict}")

    print("=" * 120)

    if best_key:
        dep, ret = best_key
        print(f"\n🏆 ПОБЕДИТЕЛЬ: DEPARTURE_MAX_BARS = {dep}, RETURN_MAX_BARS = {ret}")
        print(f"   Profit Factor: {best_pf}, Сделок: {results[best_key]['Trades']} из {total_zones}")
        print_exit_analysis(results[best_key]['trades'], f"Dep={dep} Ret={ret}")
    else:
        print("\n⚠️ Ни одна комбинация не дала PF > 0 при ≥5 сделках.")

    # === РЕКОМЕНДАЦИЯ ===
    print("\n💡 РЕКОМЕНДАЦИЯ:")
    if best_key:
        dep, ret = best_key
        total_hours = (dep + ret) * 5 / 60
        print(f"   Установите в config.yaml:")
        print(f"     departure_max_bars: {dep}   # {(dep*5/60):.1f}ч на поиск отката")
        print(f"     return_max_bars: {ret}      # {(ret*5/60):.1f}ч на поиск возврата")
        print(f"   Общее окно: до {total_hours:.1f} часов от создания зоны до входа")
    print("=" * 100)


if __name__ == "__main__":
    run_test()