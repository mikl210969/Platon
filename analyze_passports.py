"""
Анализ паспортов PLATO V2 с детальной разбивкой по инструментам.
Версия: 2.0 - Мультивалютная аналитика
"""

import json
from pathlib import Path
from datetime import datetime
from collections import defaultdict


def load_passports(passports_dir: str = "data/passports") -> list:
    """Загрузить все паспорта из директории."""
    passports = []
    path = Path(passports_dir)
    
    if not path.exists():
        print(f"❌ Директория не найдена: {passports_dir}")
        return passports
    
    for file in path.glob("*.json"):
        try:
            with open(file, "r", encoding="utf-8") as f:
                passport = json.load(f)
                passports.append(passport)
        except Exception as e:
            print(f"⚠️ Ошибка чтения {file.name}: {e}")
    
    return passports


def analyze_passport(passport: dict) -> dict:
    """Извлечь ключевые метрики из паспорта."""
    return {
        "passport_id": passport.get("passport_id", "UNKNOWN"),
        "symbol": passport.get("symbol", "UNKNOWN"),
        "side": passport.get("side", "UNKNOWN"),
        "status": passport.get("status", "UNKNOWN"),
        "entry_price": passport.get("entry_price", 0.0),
        "exit_price": passport.get("exit_price", 0.0),
        "sl_price": passport.get("sl_price", 0.0),
        "tp1_price": passport.get("tp1_price", 0.0),
        "tp2_price": passport.get("tp2_price", 0.0),
        "position_size": passport.get("position_size", 0.0),
        "realized_pnl": passport.get("realized_pnl", 0.0),
        "commission": passport.get("commission", 0.0),
        "exit_reason": passport.get("exit_reason", "UNKNOWN"),
        "created_at": passport.get("created_at", ""),
    }


def calculate_metrics(trades: list) -> dict:
    """Рассчитать метрики для списка сделок."""
    if not trades:
        return {
            "total": 0,
            "closed": 0,
            "wins": 0,
            "losses": 0,
            "breakeven": 0,
            "net_pnl": 0.0,
            "gross_profit": 0.0,
            "gross_loss": 0.0,
            "total_commission": 0.0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "avg_pnl": 0.0,
            "by_exit_reason": defaultdict(lambda: {"count": 0, "wins": 0, "pnl": 0.0}),
        }
    
    closed = [t for t in trades if t["status"] == "CLOSED"]
    wins = [t for t in closed if t["realized_pnl"] > 0.01]
    losses = [t for t in closed if t["realized_pnl"] < -0.01]
    breakeven = [t for t in closed if -0.01 <= t["realized_pnl"] <= 0.01]
    
    net_pnl = sum(t["realized_pnl"] for t in closed)
    gross_profit = sum(t["realized_pnl"] for t in wins)
    gross_loss = sum(t["realized_pnl"] for t in losses)
    total_commission = sum(t["commission"] for t in closed)
    
    win_rate = (len(wins) / len(closed) * 100) if closed else 0.0
    profit_factor = abs(gross_profit / gross_loss) if gross_loss != 0 else 0.0
    
    avg_win = (gross_profit / len(wins)) if wins else 0.0
    avg_loss = (gross_loss / len(losses)) if losses else 0.0
    avg_pnl = (net_pnl / len(closed)) if closed else 0.0
    
    by_exit_reason = defaultdict(lambda: {"count": 0, "wins": 0, "pnl": 0.0})
    for t in closed:
        reason = t["exit_reason"] if t["exit_reason"] else "UNKNOWN"
        by_exit_reason[reason]["count"] += 1
        by_exit_reason[reason]["pnl"] += t["realized_pnl"]
        if t["realized_pnl"] > 0.01:
            by_exit_reason[reason]["wins"] += 1
    
    return {
        "total": len(trades),
        "closed": len(closed),
        "wins": len(wins),
        "losses": len(losses),
        "breakeven": len(breakeven),
        "net_pnl": net_pnl,
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "total_commission": total_commission,
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "avg_pnl": avg_pnl,
        "by_exit_reason": dict(by_exit_reason),
    }


def print_separator(char: str = "─", length: int = 80):
    """Вывести разделительную линию."""
    print(char * length)


def print_metrics_section(title: str, metrics: dict, show_details: bool = True):
    """Вывести секцию с метриками."""
    print(f"\n📈 {title}")
    print_separator("─")
    print(f"  Всего паспортов:      {metrics['total']}")
    print(f"  Закрытых сделок:      {metrics['closed']}")
    print(f"  Выигрышных:           {metrics['wins']}")
    print(f"  Проигрышных:          {metrics['losses']}")
    print(f"  Безубыточных:         {metrics['breakeven']}")
    print()
    print(f"  Net P&L:              {metrics['net_pnl']:+.2f} USDT")
    print(f"  Gross Profit:         +{metrics['gross_profit']:.2f} USDT")
    print(f"  Gross Loss:           {metrics['gross_loss']:.2f} USDT")
    print(f"  Total Commission:     {metrics['total_commission']:.2f} USDT")
    print()
    
    if metrics['closed'] > 0:
        print(f"  Win Rate:             {metrics['win_rate']:.1f}% ({metrics['wins']}/{metrics['closed']})")
        print(f"  Profit Factor:        {metrics['profit_factor']:.2f}")
        print(f"  Avg Win:              +{metrics['avg_win']:.2f} USDT")
        print(f"  Avg Loss:             {metrics['avg_loss']:.2f} USDT")
        print(f"  Avg P&L per trade:    {metrics['avg_pnl']:+.2f} USDT")
    
    if show_details and metrics['by_exit_reason']:
        print()
        print(f"  🎯 РАЗБИВКА ПО ПРИЧИНЕ ВЫХОДА")
        print_separator("─", 60)
        for reason, data in sorted(metrics['by_exit_reason'].items()):
            win_rate = (data['wins'] / data['count'] * 100) if data['count'] > 0 else 0
            print(f"  {reason:<15} | Сделок: {data['count']:<3} | Win: {win_rate:>5.1f}% | P&L: {data['pnl']:+.2f} USDT")


def print_symbol_section(symbol: str, trades: list, metrics: dict):
    """Вывести детальную секцию по символу."""
    print(f"\n{'='*80}")
    print(f"📊 {symbol}")
    print(f"{'='*80}")
    
    # Сводка по сторонам
    long_trades = [t for t in trades if t["side"] == "LONG"]
    short_trades = [t for t in trades if t["side"] == "SHORT"]
    
    print(f"  LONG сделок:   {len(long_trades)}")
    print(f"  SHORT сделок:  {len(short_trades)}")
    
    # Метрики
    print_metrics_section("МЕТРИКИ", metrics, show_details=True)
    
    # Последние 5 сделок по символу
    if trades:
        print(f"\n  🕐 ПОСЛЕДНИЕ 5 СДЕЛОК ПО {symbol}")
        print_separator("─", 80)
        recent = sorted(trades, key=lambda x: x["created_at"], reverse=True)[:5]
        for t in recent:
            status_icon = "✅" if t["status"] == "CLOSED" else "⏳" if t["status"] == "POSITION_OPEN" else "⚠️"
            pnl_str = f"{t['realized_pnl']:+.2f}" if t["realized_pnl"] != 0 else "0.00"
            exit_str = f"{t['exit_price']:.2f}" if t["exit_price"] > 0 else "—"
            print(f"  {status_icon} {t['side']:<5} | Entry: {t['entry_price']:<10.2f} | Exit: {exit_str:<10} | {t['exit_reason']:<10} | P&L: {pnl_str} USDT")


def main():
    """Главная функция анализа."""
    print("="*80)
    print(f"📊 АНАЛИЗ ПАСПОРТОВ PLATO V2 | {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("="*80)
    
    # Загрузка паспортов
    passports = load_passports()
    if not passports:
        print("❌ Паспорта не найдены!")
        return
    
    print(f"\n📁 Загружено паспортов: {len(passports)}")
    
    # Обработка паспортов
    all_trades = [analyze_passport(p) for p in passports]
    
    # Группировка по символам
    by_symbol = defaultdict(list)
    for trade in all_trades:
        by_symbol[trade["symbol"]].append(trade)
    
    # Общий анализ
    total_metrics = calculate_metrics(all_trades)
    
    # Вывод общего итога
    print_metrics_section("ОБЩИЕ МЕТРИКИ (ВСЕ ИНСТРУМЕНТЫ)", total_metrics, show_details=True)
    
    # Аномалии (ERROR_STATE)
    errors = [t for t in all_trades if t["status"] == "ERROR_STATE"]
    if errors:
        print(f"\n⚠️ АНОМАЛИИ (требуют проверки)")
        print_separator("─")
        for t in errors:
            print(f"  [{t['passport_id']}] {t['side']} @ {t['entry_price']}")
            print(f"    SL: {t['sl_price']} | Exit: {t['exit_price'] if t['exit_price'] > 0 else '—'} | P&L: {t['realized_pnl']:+.2f}$")
            print(f"    ⚠️ Статус ERROR_STATE")
            print(f"    📅 {t['created_at']}")
            print()
    
    # Детальный анализ по каждому символу
    print(f"\n{'='*80}")
    print(f"📊 ДЕТАЛЬНЫЙ АНАЛИЗ ПО ИНСТРУМЕНТАМ")
    print(f"{'='*80}")
    
    for symbol in sorted(by_symbol.keys()):
        trades = by_symbol[symbol]
        metrics = calculate_metrics(trades)
        print_symbol_section(symbol, trades, metrics)
    
    # Сравнительная таблица по символам
    print(f"\n{'='*80}")
    print(f"📊 СРАВНИТЕЛЬНАЯ ТАБЛИЦА ПО ИНСТРУМЕНТАМ")
    print(f"{'='*80}")
    print(f"{'Символ':<10} | {'Сделок':>6} | {'Закрыто':>7} | {'Win Rate':>8} | {'PF':>6} | {'Net P&L':>10} | {'Gross Profit':>12} | {'Gross Loss':>11}")
    print_separator("─", 80)
    
    for symbol in sorted(by_symbol.keys()):
        trades = by_symbol[symbol]
        metrics = calculate_metrics(trades)
        print(f"{symbol:<10} | {metrics['total']:>6} | {metrics['closed']:>7} | {metrics['win_rate']:>7.1f}% | {metrics['profit_factor']:>6.2f} | {metrics['net_pnl']:>+10.2f} | {metrics['gross_profit']:>+12.2f} | {metrics['gross_loss']:>+11.2f}")
    
    print(f"\n{'='*80}")
    print(f"✅ Анализ завершен.")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()