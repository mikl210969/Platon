"""
ФИНАЛЬНОЕ СРАВНЕНИЕ: С fallback vs Без fallback
База: Wall_Mult = 4.0, tp2_protect, Жесткий R:R (1.5x / 3.0x), 5m ATR Buffer
"""
import pandas as pd
import numpy as np
import os
from dataclasses import dataclass
from typing import List, Optional

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

# Параметры реального отката
DEPARTURE_MAX_BARS = 24
RETURN_MAX_BARS = 24

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

# Параметры для Fallback
ATR_PERIOD_1M = 14
SL_MULTIPLIER_ATR = 1.7
RR_TP1_ATR = 1.5
RR_TP2_ATR = 3.0

# Режимы теста
FALLBACK_MODES = ['without_fallback', 'with_fallback']

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
    mode: str

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
        if col in df_5m.columns: df_5m[col] = df_5m[col].ffill().fillna(0)
    
    high_low = df_5m['high'] - df_5m['low']
    high_close = np.abs(df_5m['high'] - df_5m['close'].shift())
    low_close = np.abs(df_5m['low'] - df_5m['close'].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = np.max(ranges, axis=1)
    df_5m['atr_5m'] = true_range.rolling(window=14).mean()
    
    # ATR 1m для fallback
    df_trades_all['interval_1m'] = df_trades_all['timestamp'].dt.floor('1min')
    candles_1m = df_trades_all.groupby('interval_1m').agg({
        'price': ['first', 'max', 'min', 'last'], 'quantity': 'sum'
    }).reset_index()
    candles_1m.columns = ['interval_1m', 'open', 'high', 'low', 'close', 'volume']
    
    h_l = candles_1m['high'] - candles_1m['low']
    h_c = np.abs(candles_1m['high'] - candles_1m['close'].shift())
    l_c = np.abs(candles_1m['low'] - candles_1m['close'].shift())
    tr_1m = np.max(pd.concat([h_l, h_c, l_c], axis=1), axis=1)
    candles_1m['atr_1m'] = tr_1m.rolling(window=ATR_PERIOD_1M).mean()
    
    atr_1m_map = dict(zip(candles_1m['interval_1m'], candles_1m['atr_1m']))
    
    def get_atr_1m_for_time(ts):
        minute_ts = ts.floor('1min')
        return atr_1m_map.get(minute_ts, 0.5) 
        
    df_5m['atr_1m_context'] = df_5m['interval_5m'].apply(get_atr_1m_for_time)
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
                'volume': v, 'atr_5m': row['atr_5m'], 'atr_1m': row['atr_1m_context']
            })
    return zones

def detect_real_return(zone: dict, df: pd.DataFrame) -> Optional[dict]:
    z, p = zone['idx'], zone['price']
    is_long = zone['direction'] == 'LONG'
    min_dep = K_ATR_DEPARTURE * zone['atr_5m']
    max_dep = p * MAX_DEPARTURE_PCT / 100
    corridor = p * ZONE_CORRIDOR_PCT / 100
    dep_idx, extreme = None, None
    
    for j in range(z + 1, min(len(df), z + 1 + DEPARTURE_MAX_BARS)):
        r = df.iloc[j]
        adverse = (p - r['low']) if is_long else (r['high'] - p)
        if dep_idx is None:
            if adverse >= min_dep:
                if adverse > max_dep: return None
                dep_idx, extreme = j, (r['low'] if is_long else r['high'])
            continue
        extreme = min(extreme, r['low']) if is_long else max(extreme, r['high'])
        dep_abs = (p - extreme) if is_long else (extreme - p)
        if dep_abs > max_dep: return None
        if j == dep_idx or j > dep_idx + RETURN_MAX_BARS: continue
        if abs(r['close'] - p) <= corridor:
            return {'r_idx': int(j), 'extreme': float(extreme), 'dep_abs': float(dep_abs), 'dep_idx': int(dep_idx)}
    return None

def simulate_trade(zone: dict, df: pd.DataFrame, mode: str) -> Optional[TradeResult]:
    z_idx = zone['idx']
    is_long = zone['direction'] == 'LONG'
    
    # Попытка найти реальный откат
    ret = detect_real_return(zone, df)
    
    if ret is not None:
        # Есть реальный откат — используем структуру
        r_idx = ret['r_idx']
        entry = float(df.iloc[r_idx]['close'])
        
        buffer = zone['atr_5m'] * SL_BUFFER_MULT_5M
        sl = (ret['extreme'] - buffer) if is_long else (ret['extreme'] + buffer)
        
        risk_dist = abs(entry - sl)
        tp1 = entry + (risk_dist * TP1_RR_MULT) if is_long else entry - (risk_dist * TP1_RR_MULT)
        tp2 = entry + (risk_dist * TP2_RR_MULT) if is_long else entry - (risk_dist * TP2_RR_MULT)
        
        # Проверка дельты
        delta = df.loc[z_idx:r_idx, 'delta'].sum()
        if is_long and delta >= 0: return None
        if not is_long and delta <= 0: return None
        
    elif mode == 'with_fallback':
        # Нет реального отката, но fallback разрешен — входим по цене стены с ATR-уровнями
        r_idx = z_idx
        entry = float(zone['price'])
        
        sl_dist = zone['atr_1m'] * SL_MULTIPLIER_ATR
        sl = entry - sl_dist if is_long else entry + sl_dist
        tp1 = entry + sl_dist * RR_TP1_ATR if is_long else entry - sl_dist * RR_TP1_ATR
        tp2 = entry + sl_dist * RR_TP2_ATR if is_long else entry - sl_dist * RR_TP2_ATR
        
        # Проверка дельты (для fallback проверяем delta за 1 бар)
        delta = df.loc[z_idx:z_idx, 'delta'].sum()
        if is_long and delta >= 0: return None
        if not is_long and delta <= 0: return None
        
    else:
        # Нет реального отката и fallback запрещен
        return None

    # Расчет объема
    sl_dist_usd = abs(entry - sl)
    final_qty = max(MIN_QTY_SOL, min(TARGET_RISK_USDT / sl_dist_usd if sl_dist_usd > 0 else MIN_QTY_SOL, MAX_QTY_SOL))
    actual_risk_usdt = sl_dist_usd * final_qty

    # Симуляция выхода (tp2_protect логика)
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
                sl_hit = True; exit_idx = ex_idx; exit_details = "SL"; gross_pnl = -((entry - current_sl) / entry * 100); break
            if ec['high'] >= tp1 and not tp1_hit:
                tp1_hit = True; exit_idx = ex_idx
                pnl_50 = ((tp1 - entry) / entry * 100) * 0.5
                current_sl = tp2 + tp2_buffer  # tp2_protect
                
                if ec['high'] >= tp2:
                    tp2_hit = True; exit_idx = ex_idx; exit_details = "TP1+TP2"; gross_pnl = pnl_50 + ((tp2 - entry) / entry * 100) * 0.5; break
            elif tp1_hit and not (tp2_hit or sl_hit):
                if ec['low'] <= current_sl:
                    sl_hit = True; exit_idx = ex_idx; exit_details = "TP1+TP2_Protect"; gross_pnl += ((current_sl - entry) / entry * 100) * 0.5; break
                elif ec['high'] >= tp2:
                    tp2_hit = True; exit_idx = ex_idx; exit_details = "TP1+TP2"; gross_pnl += ((tp2 - entry) / entry * 100) * 0.5; break
        else:
            if ec['high'] >= current_sl and not tp1_hit:
                sl_hit = True; exit_idx = ex_idx; exit_details = "SL"; gross_pnl = -((current_sl - entry) / entry * 100); break
            if ec['low'] <= tp1 and not tp1_hit:
                tp1_hit = True; exit_idx = ex_idx
                pnl_50 = ((entry - tp1) / entry * 100) * 0.5
                current_sl = tp2 - tp2_buffer  # tp2_protect
                
                if ec['low'] <= tp2:
                    tp2_hit = True; exit_idx = ex_idx; exit_details = "TP1+TP2"; gross_pnl = pnl_50 + ((entry - tp2) / entry * 100) * 0.5; break
            elif tp1_hit and not (tp2_hit or sl_hit):
                if ec['high'] >= current_sl:
                    sl_hit = True; exit_idx = ex_idx; exit_details = "TP1+TP2_Protect"; gross_pnl += ((entry - current_sl) / entry * 100) * 0.5; break
                elif ec['low'] <= tp2:
                    tp2_hit = True; exit_idx = ex_idx; exit_details = "TP1+TP2"; gross_pnl += ((entry - tp2) / entry * 100) * 0.5; break

    if not (sl_hit or tp2_hit):
        last_c = df.iloc[max_idx]
        if tp1_hit:
            gross_pnl += ((last_c['close'] - entry) / entry * 100) * 0.5 if is_long else ((entry - last_c['close']) / entry * 100) * 0.5
            exit_details = "TP1+EOD"
        else:
            gross_pnl = ((last_c['close'] - entry) / entry * 100) if is_long else ((entry - last_c['close']) / entry * 100)
            exit_details = "EOD"
        exit_idx = max_idx

    fee_actions = 1 
    if "TP1" in exit_details: fee_actions += 1
    if "TP2" in exit_details or "EOD" in exit_details or "SL" in exit_details or "Protect" in exit_details: fee_actions += 1
    total_fee = fee_actions * COMMISSION_RATE * 100
    net_pnl = gross_pnl - total_fee
    pnl_usdt = (net_pnl / 100) * (final_qty * entry)
    hold_hours = (exit_idx - r_idx) * 5 / 60

    return TradeResult(
        zone['date'], zone['time'].strftime('%H:%M'), zone['direction'],
        entry, sl, tp1, tp2, final_qty, actual_risk_usdt, exit_details, 
        gross_pnl, net_pnl, pnl_usdt, hold_hours, mode
    )

def calculate_max_drawdown(pnls: List[float]) -> float:
    equity, peak, max_dd = 100.0, 100.0, 0.0
    for p in pnls:
        equity *= (1 + p / 100)
        if equity > peak: peak = equity
        dd = (peak - equity) / peak * 100
        if dd > max_dd: max_dd = dd
    return max_dd

def analyze_and_report(trades: List[TradeResult], mode_name: str) -> dict:
    if not trades:
        return {"Trades": 0, "Win Rate %": 0, "Net P&L %": 0, "Total PnL $": 0, "Profit Factor": 0, "Max Drawdown %": 0, "trades": []}
    total_net_pct = sum(t.net_pnl_pct for t in trades)
    total_pnl_usdt = sum(t.pnl_usdt for t in trades)
    wins = sum(1 for t in trades if t.net_pnl_pct > 0)
    wr = wins / len(trades) * 100
    pf_num = sum(t.net_pnl_pct for t in trades if t.net_pnl_pct > 0)
    pf_den = abs(sum(t.net_pnl_pct for t in trades if t.net_pnl_pct < 0))
    pf = pf_num / pf_den if pf_den > 0 else 999
    max_dd = calculate_max_drawdown([t.net_pnl_pct for t in trades])
    return {
        "Mode": mode_name, "Trades": len(trades), "Win Rate %": round(wr, 1),
        "Net P&L %": round(total_net_pct, 2), "Total PnL $": round(total_pnl_usdt, 2),
        "Profit Factor": round(pf, 2), "Max Drawdown %": round(max_dd, 2), "trades": trades
    }

def print_detailed_exit_analysis(trades: List[TradeResult], mode_name: str):
    print(f"\n{'='*95}")
    print(f" 🔬 ДЕТАЛЬНЫЙ АНАЛИЗ ПРИЧИН ВЫХОДА ({mode_name})")
    print(f"{'='*95}")
    exit_reasons = {}
    for t in trades:
        exit_reasons.setdefault(t.exit_reason, []).append(t)
    
    print(f"\n{'Причина выхода':<20} | {'Кол-во':<6} | {'%':<6} | {'Avg PnL %':<10} | {'Total PnL $':<12} | {'Avg Hold (ч)':<12}")
    print("-"*95)
    total_trades = len(trades)
    for reason in sorted(exit_reasons.keys()):
        group = exit_reasons[reason]
        count = len(group)
        avg_pnl = np.mean([t.net_pnl_pct for t in group])
        total_pnl = sum(t.pnl_usdt for t in group)
        avg_hold = np.mean([t.hold_hours for t in group])
        print(f"{reason:<20} | {count:<6} | {count/total_trades*100:>5.1f}% | {avg_pnl:>9.3f}% | ${total_pnl:>10.2f} | {avg_hold:>10.1f}")

    losing_trades = [t for t in trades if t.net_pnl_pct < 0]
    if losing_trades:
        print(f"\n⚠️ УБЫТОЧНЫЕ СДЕЛКИ ({len(losing_trades)} из {total_trades}):")
        losing_by_reason = {}
        for t in losing_trades: losing_by_reason.setdefault(t.exit_reason, []).append(t)
        for reason, group in sorted(losing_by_reason.items()):
            count = len(group)
            avg_loss = np.mean([t.net_pnl_pct for t in group])
            total_loss = sum(t.pnl_usdt for t in group)
            print(f"  {reason:<18} | {count:<6} | {avg_loss:>9.3f}% | ${total_loss:>10.2f}")
    print("="*95)

def run_test():
    print("="*95)
    print(" СРАВНЕНИЕ: Без Fallback vs С Fallback")
    print(" База: Wall_Mult = 4.0, tp2_protect, Жесткий R:R (1.5x / 3.0x)")
    print("="*95)
    
    df = load_and_prepare_data()
    zones = find_zones(df)
    print(f"🧱 Найдено {len(zones)} пуленепробиваемых зон (Wall_Mult = 4.0).")
    
    results = {}
    print("\n▶️ Запуск тестов...")
    for mode in FALLBACK_MODES:
        print(f"  • Тестирование '{mode}'...", end=" ")
        trades = [res for zone in zones if (res := simulate_trade(zone, df, mode)) is not None]
        results[mode] = analyze_and_report(trades, mode)
        print(f"Сделок: {len(trades)}")

    print("\n" + "="*95)
    print(" 📊 СВОДНАЯ ТАБЛИЦА РЕЗУЛЬТАТОВ")
    print("="*95)
    headers = ["Режим", "Сделок", "Win Rate %", "Net P&L %", "Total PnL $", "Profit Factor", "Max DD %", "Вердикт"]
    print(f"{headers[0]:<20} | {headers[1]:<8} | {headers[2]:<10} | {headers[3]:<10} | {headers[4]:<12} | {headers[5]:<13} | {headers[6]:<10} | {headers[7]}")
    print("-"*110)
    
    best_mode, best_pf = None, -1
    for mode in FALLBACK_MODES:
        res = results[mode]
        verdict = "🏆 ПОБЕДИТЕЛЬ" if res['Profit Factor'] > best_pf and res['Trades'] >= 5 else ""
        if res['Profit Factor'] > best_pf and res['Trades'] >= 5:
            best_pf, best_mode = res['Profit Factor'], mode
        print(f"{mode:<20} | {res['Trades']:<8} | {res['Win Rate %']:<10} | {res['Net P&L %']:<10} | ${res['Total PnL $']:<11} | {res['Profit Factor']:<13} | {res['Max Drawdown %']:<10} | {verdict}")
    print("="*95)
    
    if best_mode:
        print(f"\n💡 ВЫВОД: Режим '{best_mode}' показал наилучший Profit Factor ({best_pf}).")
        print_detailed_exit_analysis(results[best_mode]['trades'], best_mode)
    
    # Дополнительно: показать, сколько сделок добавил fallback
    without_count = results['without_fallback']['Trades']
    with_count = results['with_fallback']['Trades']
    fallback_trades = with_count - without_count
    print(f"\n📈 Fallback добавил {fallback_trades} сделок к базовым {without_count}.")

if __name__ == "__main__":
    run_test()