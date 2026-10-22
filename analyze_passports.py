"""
PLATO Passport Analytics Dashboard v1.0
Анализирует закрытые сделки из папки passports/ и выводит статистику 
по эффективности адаптивных механизмов (Smart Sizing, Adaptive SL, Regime).
"""

import os
import json
import glob
from collections import defaultdict
from datetime import datetime

# Настройки
PASSPORTS_DIR = "data.passports"

def load_closed_passports():
    """Загружает все паспорта со статусом CLOSED."""
    files = glob.glob(os.path.join(PASSPORTS_DIR, "*.json"))
    trades = []
    
    for file in files:
        try:
            with open(file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if data.get("status") == "CLOSED":
                    trades.append(data)
        except Exception as e:
            print(f"⚠️ Ошибка чтения {file}: {e}")
            
    return trades

def calculate_pnl(trade):
    """Вычисляет PnL, если он не указан явно."""
    pnl = trade.get('gross_pnl') or trade.get('realized_pnl')
    if pnl is not None:
        return float(pnl)
    
    # Fallback расчет
    entry = trade.get('entry_price', 0)
    exit_price = trade.get('exit_price', 0)
    qty = trade.get('final_quantity', 0) or trade.get('sizing_info', {}).get('final_quantity', 0)
    side = trade.get('side', 'long')
    
    if entry and exit_price and qty:
        multiplier = 1 if side == 'long' else -1
        return float(qty * (exit_price - entry) * multiplier)
    return 0.0

def analyze(trades):
    """Агрегирует статистику."""
    stats = {
        'total_trades': len(trades),
        'wins': 0,
        'losses': 0,
        'breakeven': 0,
        'total_pnl': 0.0,
        'by_strategy': defaultdict(lambda: {'trades': 0, 'wins': 0, 'pnl': 0.0}),
        'by_smart_mult': defaultdict(lambda: {'trades': 0, 'wins': 0, 'pnl': 0.0}),
        'by_sl_mult': defaultdict(lambda: {'trades': 0, 'wins': 0, 'pnl': 0.0, 'sl_hits': 0}),
        'by_btc_trend': defaultdict(lambda: {'trades': 0, 'wins': 0, 'pnl': 0.0}),
        'by_exit_reason': defaultdict(int)
    }

    for t in trades:
        pnl = calculate_pnl(t)
        stats['total_pnl'] += pnl
        
        if pnl > 0.01:
            stats['wins'] += 1
            is_win = True
        elif pnl < -0.01:
            stats['losses'] += 1
            is_win = False
        else:
            stats['breakeven'] += 1
            is_win = False # Считаем безубыток как "не прибыль" для консервативной статистики

        strategy = t.get('strategy', 'Unknown')
        sizing = t.get('sizing_info', {})
        smart_mult = sizing.get('smart_multiplier', 1.0)
        sl_mult = sizing.get('sl_multiplier', 1.0)
        btc_trend = sizing.get('btc_trend', 'UNKNOWN')
        exit_reason = t.get('exit_reason', 'UNKNOWN')

        # 1. По стратегиям
        stats['by_strategy'][strategy]['trades'] += 1
        if is_win: stats['by_strategy'][strategy]['wins'] += 1
        stats['by_strategy'][strategy]['pnl'] += pnl

        # 2. По Smart Sizing
        stats['by_smart_mult'][smart_mult]['trades'] += 1
        if is_win: stats['by_smart_mult'][smart_mult]['wins'] += 1
        stats['by_smart_mult'][smart_mult]['pnl'] += pnl

        # 3. По Adaptive SL
        stats['by_sl_mult'][sl_mult]['trades'] += 1
        if is_win: stats['by_sl_mult'][sl_mult]['wins'] += 1
        stats['by_sl_mult'][sl_mult]['pnl'] += pnl
        if exit_reason == 'SL_HIT':
            stats['by_sl_mult'][sl_mult]['sl_hits'] += 1

        # 4. По тренду BTC
        stats['by_btc_trend'][btc_trend]['trades'] += 1
        if is_win: stats['by_btc_trend'][btc_trend]['wins'] += 1
        stats['by_btc_trend'][btc_trend]['pnl'] += pnl

        # 5. Причины выхода
        stats['by_exit_reason'][exit_reason] += 1

    return stats

def print_dashboard(stats, trades):
    """Выводит красивый текстовый отчет."""
    print("\n" + "="*70)
    print(" 🚀 PLATO TRADING ANALYTICS DASHBOARD v1.1")
    print(f" 📅 Отчет сгенерирован: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

    total = stats['total_trades']
    if total == 0:
        print("\n⚠️ Закрытых сделок (статус CLOSED) пока нет. Запустите платформу и подождите сигналов.")
        return

    win_rate = (stats['wins'] / total) * 100
    avg_pnl = stats['total_pnl'] / total
    
    print(f"\n📊 ОБЩАЯ СТАТИСТИКА")
    print(f"   Всего сделок:      {total}")
    print(f"   Прибыльных:        {stats['wins']} ({win_rate:.1f}%)")
    print(f"   Убыточных:         {stats['losses']}")
    print(f"   Безубыточных:      {stats['breakeven']}")
    print(f"   Общий PnL:         {stats['total_pnl']:+.2f} USDT")
    print(f"   Средний PnL/сделку:{avg_pnl:+.2f} USDT")

    print(f"\n📈 ЭФФЕКТИВНОСТЬ ПО СТРАТЕГИЯМ")
    print(f"   {'Стратегия':<20} | {'Сделок':<6} | {'Win Rate':<8} | {'PnL (USDT)':<10}")
    print("-" * 55)
    for strat, data in sorted(stats['by_strategy'].items(), key=lambda x: x[1]['pnl'], reverse=True):
        wr = (data['wins'] / data['trades'] * 100) if data['trades'] > 0 else 0
        print(f"   {strat:<20} | {data['trades']:<6} | {wr:>5.1f}%   | {data['pnl']:>+9.2f}")

    print(f"\n💰 ЭФФЕКТИВНОСТЬ SMART SIZING (Множитель риска)")
    print(f"   {'Множитель':<12} | {'Сделок':<6} | {'Win Rate':<8} | {'PnL (USDT)':<10} | {'Гипотеза'}")
    print("-" * 65)
    for mult in sorted(stats['by_smart_mult'].keys()):
        data = stats['by_smart_mult'][mult]
        wr = (data['wins'] / data['trades'] * 100) if data['trades'] > 0 else 0
        
        if mult == 1.5: hypothesis = "Подтверждение (ожидаем высокий PnL)"
        elif mult == 0.5: hypothesis = "Контртренд (ожидаем низкий риск)"
        elif mult == 0.7: hypothesis = "IMPULSIVE (защита от ножей)"
        else: hypothesis = "Нейтрально"
            
        print(f"   ×{mult:<10} | {data['trades']:<6} | {wr:>5.1f}%   | {data['pnl']:>+9.2f} | {hypothesis}")

    print(f"\n🎯 ЭФФЕКТИВНОСТЬ ADAPTIVE SL (Множитель ширины стопа)")
    print(f"   {'Множитель':<12} | {'Сделок':<6} | {'Win Rate':<8} | {'SL Hits':<7} | {'PnL (USDT)'}")
    print("-" * 60)
    for mult in sorted(stats['by_sl_mult'].keys()):
        data = stats['by_sl_mult'][mult]
        wr = (data['wins'] / data['trades'] * 100) if data['trades'] > 0 else 0
        sl_rate = (data['sl_hits'] / data['trades'] * 100) if data['trades'] > 0 else 0
        
        if mult == 0.6: desc = "Узкий (сильная дельта)"
        elif mult == 1.4: desc = "Широкий (слабая дельта)"
        else: desc = "Базовый"
            
        print(f"   ×{mult:<10} | {data['trades']:<6} | {wr:>5.1f}%   | {data['sl_hits']:<4} ({sl_rate:>4.1f}%) | {data['pnl']:>+9.2f}  {desc}")

    print(f"\n🌪️ ВЛИЯНИЕ РЕЖИМА BTC (Trend на момент входа)")
    print(f"   {'Тренд BTC':<12} | {'Сделок':<6} | {'Win Rate':<8} | {'PnL (USDT)'}")
    print("-" * 45)
    for trend, data in stats['by_btc_trend'].items():
        wr = (data['wins'] / data['trades'] * 100) if data['trades'] > 0 else 0
        print(f"   {trend:<12} | {data['trades']:<6} | {wr:>5.1f}%   | {data['pnl']:>+9.2f}")

    print(f"\n🚪 ПРИЧИНЫ ЗАКРЫТИЯ ПОЗИЦИЙ")
    for reason, count in sorted(stats['by_exit_reason'].items(), key=lambda x: x[1], reverse=True):
        pct = (count / total) * 100
        print(f"   • {reason:<20} : {count} ({pct:.1f}%)")

    # 🔥 НОВОЕ: Секция проблемных сделок
    problem_trades = [
        t for t in trades 
        if t.get('exit_reason') in ('EXTERNAL_CLOSE', 'UNKNOWN') or 
           t.get('sizing_info', {}).get('btc_trend') == 'UNKNOWN'
    ]
    
    if problem_trades:
        print(f"\n⚠️ ПРОБЛЕМНЫЕ СДЕЛКИ (требуют проверки) — {len(problem_trades)} шт.")
        print(f"   {'#':<3} | {'Passport ID':<35} | {'Exit Reason':<15} | {'BTC Trend':<10} | {'PnL':>8}")
        print("   " + "-" * 85)
        for i, t in enumerate(problem_trades[:10], 1):  # Показываем максимум 10
            passport_id = t.get('passport_id', 'N/A')[:35]
            exit_reason = t.get('exit_reason', 'N/A')
            btc_trend = t.get('sizing_info', {}).get('btc_trend', 'N/A')
            pnl = calculate_pnl(t)
            print(f"   {i:<3} | {passport_id:<35} | {exit_reason:<15} | {btc_trend:<10} | {pnl:>+8.2f}")
        
        if len(problem_trades) > 10:
            print(f"   ... и ещё {len(problem_trades) - 10} сделок")
    else:
        print(f"\n✅ Проблемных сделок не обнаружено. Все данные корректны.")

    print("\n" + "="*70)
    print(" 💡 СОВЕТ: Если Win Rate при ×0.5 (контртренд) низкий, но PnL близок к 0,")
    print("    значит, Smart Sizing успешно спасает депозит от больших убытков!")
    print("="*70 + "\n")

if __name__ == "__main__":
    print("🔍 Сканирование папки passports/...")
    trades = load_closed_passports()
    stats = analyze(trades)
    print_dashboard(stats, trades)  # 🔥 Передаём trades для вывода проблемных сделок