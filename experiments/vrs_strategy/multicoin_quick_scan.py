"""
MULTICOIN QUICK SCAN: Оценка эффективности Золотого Стандарта на собранных данных.
Data Path: C:/Users/ongul/YandexDisk/data21
Logic: Structure Mode (Wall > Threshold, Departure, Return, Delta Filter).
Execution: Simplified (Close of return candle).
"""

import pandas as pd
import numpy as np
import os
from pathlib import Path
from datetime import datetime, timedelta

# ================= CONFIGURATION =================
# ВАЖНО: Префикс r перед кавычками делает строку "сырой", игнорируя спецсимволы \
DATA_ROOT = r"C:\Users\ongul\YandexDisk\data21"

# Параметры стратегии (из ТЗ Golden Standard)
PARAMS = {
    "SOLUSDT": {"min_wall_usd": 50000, "k_atr": 1.5},
    "BTCUSDT": {"min_wall_usd": 750000, "k_atr": 2.0},
    "ETHUSDT": {"min_wall_usd": 200000, "k_atr": 1.8},
    "BNBUSDT": {"min_wall_usd": 200000, "k_atr": 1.6},
    "XRPUSDT": {"min_wall_usd": 100000, "k_atr": 1.7},
    "LINKUSDT": {"min_wall_usd": 80000, "k_atr": 1.6},
    "AVAXUSDT": {"min_wall_usd": 60000, "k_atr": 1.5},
}

CORRIDOR_PCT = 0.5      # Коридор возврата +/- %
MAX_DEPARTURE_PCT = 4.0 # Максимальный откат (%)
COMMISSION_RATE = 0.0004
TARGET_RISK_USDT = 30.0
MIN_QTY = 0.1
MAX_QTY = 10.0

# ================================================

def load_symbol_data(symbol: str):
    """Загружает и объединяет данные стакана и сделок для одного символа."""
    symbol_dir = Path(DATA_ROOT) / symbol
    if not symbol_dir.exists():
        return None, None
        
    # Ищем все файлы trades_raw_*.csv и ob_snapshot_*.csv
    trade_files = sorted(list(symbol_dir.glob("trades_raw_*.csv")))
    ob_files = sorted(list(symbol_dir.glob("ob_snapshot_*.csv")))
    
    if not trade_files or not ob_files:
        print(f"⚠️ No complete data pairs for {symbol}")
        return None, None

    dfs_trades = []
    dfs_ob = []
    
    # Читаем трейды
    for tf in trade_files:
        try:
            df_t = pd.read_csv(tf)
            # timestamp_ms -> datetime (это числовое поле, проблем быть не должно)
            df_t['timestamp'] = pd.to_datetime(df_t['timestamp_ms'], unit='ms', utc=True)
            # side Buy/Sell -> is_buyer boolean
            df_t['is_buyer'] = df_t['side'].str.strip().eq('Buy')
            dfs_trades.append(df_t[['timestamp', 'price', 'qty', 'is_buyer']])
        except Exception as e:
            print(f"❌ Error reading {tf}: {e}")
            
    # Читаем стакан
    for of in ob_files:
        try:
            df_o = pd.read_csv(of)
            
            # 🔥 ИСПРАВЛЕНИЕ ЗДЕСЬ: Используем format='mixed' или infer_datetime_format=True
            # Это позволяет Pandas обрабатывать разные форматы строк в одной колонке
            try:
                df_o['timestamp'] = pd.to_datetime(df_o['timestamp_utc'], utc=True, format='mixed')
            except ValueError:
                # Fallback если mixed не сработал (старые версии pandas)
                df_o['timestamp'] = pd.to_datetime(df_o['timestamp_utc'], utc=True, errors='coerce')
                
            # Удаляем строки с ошибкой парсинга (NaT)
            df_o.dropna(subset=['timestamp'], inplace=True)
            
            # Переименуем колонки под наш стандарт
            df_o.rename(columns={
                'bid_p_1': 'best_bid_price', 'bid_v_1': 'best_bid_qty',
                'ask_p_1': 'best_ask_price', 'ask_v_1': 'best_ask_qty'
            }, inplace=True)
            dfs_ob.append(df_o[['timestamp', 'best_bid_price', 'best_bid_qty', 'best_ask_price', 'best_ask_qty']])
        except Exception as e:
            print(f"❌ Error reading {of}: {e}")

    if not dfs_trades or not dfs_ob:
        return None, None

    # Объединяем и сортируем
    df_trades_all = pd.concat(dfs_trades, ignore_index=True).sort_values('timestamp').reset_index(drop=True)
    df_ob_all = pd.concat(dfs_ob, ignore_index=True).sort_values('timestamp').reset_index(drop=True)
    
    return df_trades_all, df_ob_all

def prepare_candles_and_walls(df_trades, df_ob, symbol_cfg):
    """Агрегирует свечи 5m и добавляет данные о стенах."""
    
    # 1. Агрегация Trades в 5m OHLCV + Delta
    df_trades['interval'] = df_trades['timestamp'].dt.floor('5min')
    candles = df_trades.groupby('interval').agg({
        'price': ['first', 'max', 'min', 'last'],
        'qty': 'sum'
    }).reset_index()
    candles.columns = ['interval', 'open', 'high', 'low', 'close', 'volume']
    
    buyer_vol = df_trades[df_trades['is_buyer']].groupby('interval')['qty'].sum()
    seller_vol = df_trades[~df_trades['is_buyer']].groupby('interval')['qty'].sum()
    candles = candles.merge(buyer_vol.rename('buyer_vol'), on='interval', how='left').fillna(0)
    candles = candles.merge(seller_vol.rename('seller_vol'), on='interval', how='left').fillna(0)
    candles['delta'] = candles['buyer_vol'] - candles['seller_vol']
    
    # 2. Агрегация OrderBook в 5m (Max Wall Volume per candle)
    df_ob['interval'] = df_ob['timestamp'].dt.floor('5min')
    walls_agg = df_ob.groupby('interval').agg({
        'best_bid_qty': 'max',
        'best_ask_qty': 'max',
        'best_bid_price': 'last',
        'best_ask_price': 'last'
    }).reset_index()
    walls_agg.rename(columns={
        'best_bid_qty': 'bid_v1_max',
        'best_ask_qty': 'ask_v1_max',
        'best_bid_price': 'bid_p_last',
        'best_ask_price': 'ask_p_last'
    }, inplace=True)
    
    # Merge
    merged = candles.merge(walls_agg, on='interval', how='inner').sort_values('interval').reset_index(drop=True)
    
    # 3. Расчет ATR(14)
    high_low = merged['high'] - merged['low']
    high_close = np.abs(merged['high'] - merged['close'].shift())
    low_close = np.abs(merged['low'] - merged['close'].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = np.max(ranges, axis=1)
    merged['atr_5m'] = true_range.rolling(window=14).mean()
    
    return merged

def run_strategy_simulation(df_candles, symbol: str, cfg: dict):
    """Эмуляция логики AdaptiveZoneV1 (Structure Mode)."""
    trades = []
    
    min_wall_usd = cfg['min_wall_usd']
    k_atr = cfg['k_atr']
    
    # Итерируемся по свечам для поиска зон
    # Оптимизация: пропускаем первые 14 свечей (ATR warmup)
    start_idx = 14 
    
    for idx in range(start_idx, len(df_candles)):
        row = df_candles.iloc[idx]
        
        # --- DETECTION OF LONG ZONE (BID WALL) ---
        current_price = row['close']
        bid_vol = row['bid_v1_max']
        bid_wall_usd = bid_vol * current_price
        
        if bid_wall_usd > min_wall_usd:
            z_idx = idx
            zone_ref_price = current_price
            
            # Поиск будущего экстремума (Departure)
            future_slice = df_candles.loc[z_idx:]
            if len(future_slice) < 2: continue # Мало данных для анализа
            
            min_price_future = future_slice['low'].min()
            departure_abs = zone_ref_price - min_price_future
            
            if departure_abs <= 0: continue
            
            dep_pct = (departure_abs / zone_ref_price) * 100
            atr_val = row['atr_5m']
            if np.isnan(atr_val) or atr_val <= 0: continue
            
            min_dep_req = k_atr * atr_val
            max_dep_abs = zone_ref_price * (MAX_DEPARTURE_PCT / 100)
            
            # Фильтр отката
            if not (min_dep_req <= departure_abs <= max_dep_abs):
                continue
                
            # Поиск возврата (Return Candidate)
            # Находим индекс минимума
            min_idx_rel = future_slice['low'].idxmin()
            # Смотрим свечи ПОСЛЕ минимума
            ret_search_area = df_candles.loc[min_idx_rel:]
            
            corridor_low = zone_ref_price * (1 - CORRIDOR_PCT/100)
            corridor_high = zone_ref_price * (1 + CORRIDOR_PCT/100)
            
            valid_returns = ret_search_area[(ret_search_area['close'] >= corridor_low) & 
                                            (ret_search_area['close'] <= corridor_high)]
                                            
            if len(valid_returns) == 0: continue
            
            r_idx = valid_returns.index[0]
            r_row = df_candles.loc[r_idx]
            
            # Фильтр Дельты (Absorption)
            # Сумма дельты от зоны до момента возврата должна быть < 0 (для LONG)
            delta_sum = df_candles.loc[z_idx:r_idx, 'delta'].sum()
            if delta_sum >= 0: continue
            
            # СИГНАЛ НАЙДЕН!
            entry_price = r_row['close']
            entry_time = r_row['interval']
            
            # Расчет уровней (Structure Based)
            dep_val = departure_abs # Абсолютная величина отката
            sl_price = min_price_future * 0.998
            tp1_price = entry_price + (dep_val * 0.5)
            tp2_price = entry_price + (dep_val * 1.0)
            
            # Risk Management
            risk_dist = abs(entry_price - sl_price)
            qty = TARGET_RISK_USDT / risk_dist if risk_dist > 0 else MIN_QTY
            qty = max(MIN_QTY, min(qty, MAX_QTY))
            
            # Симуляция выхода (Упрощенная: смотрим будущие свечи)
            exit_reason = "OPEN"
            pnl_pct = 0.0
            actual_exit_time = entry_time
            
            # Проверяем следующие свечи до TTL или TP/SL
            search_end_idx = min(r_idx + 576, len(df_candles)-1) # 48 hours approx
            
            hit_sl = False
            hit_tp1 = False
            hit_tp2 = False
            
            for ex_idx in range(r_idx + 1, search_end_idx + 1):
                ec = df_candles.iloc[ex_idx]
                
                # Check SL first (Conservative)
                if ec['low'] <= sl_price:
                    hit_sl = True
                    exit_reason = "SL"
                    pnl_pct = -((entry_price - sl_price) / entry_price) * 100
                    actual_exit_time = ec['interval']
                    break
                    
                # Check TP2 (Full profit)
                if ec['high'] >= tp2_price:
                    hit_tp2 = True
                    exit_reason = "TP1+TP2"
                    # PnL calculation with split position
                    pnl_part1 = ((tp1_price - entry_price) / entry_price) * 100 * 0.5
                    pnl_part2 = ((tp2_price - entry_price) / entry_price) * 100 * 0.5
                    pnl_pct = pnl_part1 + pnl_part2
                    actual_exit_time = ec['interval']
                    break
                    
                # Check TP1 (Partial)
                if ec['high'] >= tp1_price and not hit_tp1:
                    hit_tp1 = True
                    # Continue searching for TP2 or SL for remaining part
                    # But for simplified sim, let's assume we hold rest until SL or EOD
                    pass 
            
            # If loop finished without break (EOD/TTL)
            if not (hit_sl or hit_tp2):
                last_c = df_candles.iloc[search_end_idx]
                if hit_tp1:
                    # Remaining 50% closed at market/EOD price roughly
                    pnl_part1 = ((tp1_price - entry_price) / entry_price) * 100 * 0.5
                    pnl_part2 = ((last_c['close'] - entry_price) / entry_price) * 100 * 0.5
                    pnl_pct = pnl_part1 + pnl_part2
                    exit_reason = "TP1+EOD"
                else:
                    pnl_pct = ((last_c['close'] - entry_price) / entry_price) * 100
                    exit_reason = "EOD"
                actual_exit_time = last_c['interval']

            # Fees
            fee_actions = 1 # Entry
            if "TP1" in exit_reason: fee_actions += 1
            if "TP2" in exit_reason or "EOD" in exit_reason or "SL" in exit_reason: fee_actions += 1
            total_fee_pct = fee_actions * COMMISSION_RATE * 100
            net_pnl_pct = pnl_pct - total_fee_pct
            
            trades.append({
                'time_entry': entry_time,
                'side': 'LONG',
                'entry': entry_price,
                'sl': sl_price,
                'tp1': tp1_price,
                'tp2': tp2_price,
                'qty': qty,
                'exit_reason': exit_reason,
                'gross_pnl_pct': pnl_pct,
                'net_pnl_pct': net_pnl_pct,
                'risk_usdt': risk_dist * qty
            })

        # --- DETECTION OF SHORT ZONE (ASK WALL) ---
        ask_vol = row['ask_v1_max']
        ask_wall_usd = ask_vol * current_price
        
        if ask_wall_usd > min_wall_usd:
            z_idx = idx
            zone_ref_price = current_price
            
            future_slice = df_candles.loc[z_idx:]
            if len(future_slice) < 2: continue
            
            max_price_future = future_slice['high'].max()
            departure_abs = max_price_future - zone_ref_price
            
            if departure_abs <= 0: continue
            
            dep_pct = (departure_abs / zone_ref_price) * 100
            atr_val = row['atr_5m']
            if np.isnan(atr_val) or atr_val <= 0: continue
            
            min_dep_req = k_atr * atr_val
            max_dep_abs = zone_ref_price * (MAX_DEPARTURE_PCT / 100)
            
            if not (min_dep_req <= departure_abs <= max_dep_abs):
                continue
                
            max_idx_rel = future_slice['high'].idxmax()
            ret_search_area = df_candles.loc[max_idx_rel:]
            
            corridor_low = zone_ref_price * (1 - CORRIDOR_PCT/100)
            corridor_high = zone_ref_price * (1 + CORRIDOR_PCT/100)
            
            valid_returns = ret_search_area[(ret_search_area['close'] >= corridor_low) & 
                                            (ret_search_area['close'] <= corridor_high)]
                                            
            if len(valid_returns) == 0: continue
            
            r_idx = valid_returns.index[0]
            r_row = df_candles.loc[r_idx]
            
            # Delta Filter for SHORT (> 0)
            delta_sum = df_candles.loc[z_idx:r_idx, 'delta'].sum()
            if delta_sum <= 0: continue
            
            entry_price = r_row['close']
            entry_time = r_row['interval']
            
            dep_val = departure_abs
            sl_price = max_price_future * 1.002
            tp1_price = entry_price - (dep_val * 0.5)
            tp2_price = entry_price - (dep_val * 1.0)
            
            risk_dist = abs(entry_price - sl_price)
            qty = TARGET_RISK_USDT / risk_dist if risk_dist > 0 else MIN_QTY
            qty = max(MIN_QTY, min(qty, MAX_QTY))
            
            exit_reason = "OPEN"
            pnl_pct = 0.0
            actual_exit_time = entry_time
            
            search_end_idx = min(r_idx + 576, len(df_candles)-1)
            
            hit_sl = False
            hit_tp1 = False
            hit_tp2 = False
            
            for ex_idx in range(r_idx + 1, search_end_idx + 1):
                ec = df_candles.iloc[ex_idx]
                
                # Check SL first (High >= SL)
                if ec['high'] >= sl_price:
                    hit_sl = True
                    exit_reason = "SL"
                    pnl_pct = -((sl_price - entry_price) / entry_price) * 100
                    actual_exit_time = ec['interval']
                    break
                    
                # Check TP2 (Low <= TP2)
                if ec['low'] <= tp2_price:
                    hit_tp2 = True
                    exit_reason = "TP1+TP2"
                    pnl_part1 = ((entry_price - tp1_price) / entry_price) * 100 * 0.5
                    pnl_part2 = ((entry_price - tp2_price) / entry_price) * 100 * 0.5
                    pnl_pct = pnl_part1 + pnl_part2
                    actual_exit_time = ec['interval']
                    break
                    
                # Check TP1
                if ec['low'] <= tp1_price and not hit_tp1:
                    hit_tp1 = True
            
            if not (hit_sl or hit_tp2):
                last_c = df_candles.iloc[search_end_idx]
                if hit_tp1:
                    pnl_part1 = ((entry_price - tp1_price) / entry_price) * 100 * 0.5
                    pnl_part2 = ((entry_price - last_c['close']) / entry_price) * 100 * 0.5
                    pnl_pct = pnl_part1 + pnl_part2
                    exit_reason = "TP1+EOD"
                else:
                    pnl_pct = ((entry_price - last_c['close']) / entry_price) * 100
                    exit_reason = "EOD"
                actual_exit_time = last_c['interval']

            fee_actions = 1
            if "TP1" in exit_reason: fee_actions += 1
            if "TP2" in exit_reason or "EOD" in exit_reason or "SL" in exit_reason: fee_actions += 1
            total_fee_pct = fee_actions * COMMISSION_RATE * 100
            net_pnl_pct = pnl_pct - total_fee_pct
            
            trades.append({
                'time_entry': entry_time,
                'side': 'SHORT',
                'entry': entry_price,
                'sl': sl_price,
                'tp1': tp1_price,
                'tp2': tp2_price,
                'qty': qty,
                'exit_reason': exit_reason,
                'gross_pnl_pct': pnl_pct,
                'net_pnl_pct': net_pnl_pct,
                'risk_usdt': risk_dist * qty
            })

    return trades

def main():
    print("="*80)
    print(" MULTICOIN QUICK SCAN: GOLDEN STANDARD VALIDATION")
    print("="*80)
    
    summary_rows = []
    
    for symbol, cfg in PARAMS.items():
        print(f"\n🔍 Processing {symbol}...")
        
        df_trades, df_ob = load_symbol_data(symbol)
        if df_trades is None or df_ob is None:
            print(f"   ❌ Skipping {symbol} due to missing data.")
            continue
            
        df_candles = prepare_candles_and_walls(df_trades, df_ob, cfg)
        print(f"   📊 Prepared {len(df_candles)} candles.")
        
        if len(df_candles) < 20:
            print(f"   ⚠️ Not enough candles for analysis.")
            continue
            
        results = run_strategy_simulation(df_candles, symbol, cfg)
        
        if not results:
            print(f"   😐 No signals triggered.")
            win_rate = 0
            pf = 0
            net_pl = 0
            avg_risk = 0
        else:
            wins = sum(1 for t in results if t['net_pnl_pct'] > 0)
            losses = len(results) - wins
            win_rate = (wins / len(results)) * 100
            
            gross_profit = sum(t['net_pnl_pct'] for t in results if t['net_pnl_pct'] > 0)
            gross_loss = abs(sum(t['net_pnl_pct'] for t in results if t['net_pnl_pct'] < 0))
            pf = gross_profit / gross_loss if gross_loss > 0 else 999
            
            net_pl = sum(t['net_pnl_pct'] for t in results)
            avg_risk = np.mean([t['risk_usdt'] for t in results])
            
            print(f"   ✅ Signals: {len(results)} | WR: {win_rate:.1f}% | PF: {pf:.2f} | Net P&L: {net_pl:+.2f}%")
            
        summary_rows.append({
            "Symbol": symbol,
            "Trades": len(results),
            "Win Rate %": round(win_rate, 1),
            "Profit Factor": round(pf, 2),
            "Net P&L %": round(net_pl, 2),
            "Avg Risk $": round(avg_risk, 2)
        })
        
    # Final Table
    if summary_rows:
        df_summary = pd.DataFrame(summary_rows)
        print("\n" + "="*80)
        print(" FINAL SUMMARY TABLE")
        print("="*80)
        print(df_summary.to_string(index=False))
        print("="*80)
    else:
        print("\nNo data processed successfully.")

if __name__ == "__main__":
    main()