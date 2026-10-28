"""
Анализатор паспортов PLATO V2.
Запуск: python analyze_passports.py
"""

import json
from pathlib import Path
from collections import defaultdict
from datetime import datetime

PASSPORTS_DIR = Path("data/passports")

def load_passports():
    passports = []
    if not PASSPORTS_DIR.exists():
        print(f"❌ Директория {PASSPORTS_DIR} не найдена!")
        return passports
    
    for file in sorted(PASSPORTS_DIR.glob("*.json")):
        try:
            with open(file, 'r', encoding='utf-8') as f:
                passports.append(json.load(f))
        except Exception as e:
            print(f"️ Ошибка чтения {file}: {e}")
    return passports

def analyze(passports):
    if not passports:
        print(" Паспортов не найдено.")
        return
    
    closed = [p for p in passports if p.get("status") in ("CLOSED", "ERROR_STATE")]
    cancelled = [p for p in passports if p.get("status") == "ORDER_CANCELLED"]
    open_trades = [p for p in passports if p.get("status") == "POSITION_OPEN"]
    
    print("=" * 80)
    print(f"📊 АНАЛИЗ ПАСПОРТОВ PLATO V2 | {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 80)
    print(f"📁 Всего паспортов: {len(passports)}")
    print(f"✅ Закрытых сделок: {len(closed)}")
    print(f"⏳ Отмененных ордеров: {len(cancelled)}")
    print(f"🔓 Открытых позиций: {len(open_trades)}")
    print()
    
    if not closed:
        print("⚠️ Нет закрытых сделок для анализа.")
        return
    
    # === ОБЩИЕ МЕТРИКИ ===
    wins = [p for p in closed if p.get("realized_pnl", 0) > 0]
    losses = [p for p in closed if p.get("realized_pnl", 0) < 0]
    breakeven = [p for p in closed if p.get("realized_pnl", 0) == 0]
    
    total_pnl = sum(p.get("realized_pnl", 0) for p in closed)
    gross_profit = sum(p.get("realized_pnl", 0) for p in wins)
    gross_loss = abs(sum(p.get("realized_pnl", 0) for p in losses))
    total_commission = sum(p.get("commission", 0) for p in closed)
    
    win_rate = (len(wins) / len(closed) * 100) if closed else 0
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else 0
    
    print("📈 ОБЩИЕ МЕТРИКИ")
    print("-" * 80)
    print(f"  Net P&L:              {total_pnl:+.2f} USDT")
    print(f"  Gross Profit:         {gross_profit:+.2f} USDT")
    print(f"  Gross Loss:           -{gross_loss:.2f} USDT")
    print(f"  Total Commission:     -{total_commission:.2f} USDT")
    print(f"  Win Rate:             {win_rate:.1f}% ({len(wins)}/{len(closed)})")
    print(f"  Profit Factor:        {profit_factor:.2f}")
    print(f"  Break-even trades:    {len(breakeven)}")
    print()
    
    # === РАЗБИВКА ПО EXIT_REASON ===
    print("🎯 РАЗБИВКА ПО ПРИЧИНЕ ВЫХОДА")
    print("-" * 80)
    exit_stats = defaultdict(lambda: {"count": 0, "pnl": 0.0, "wins": 0})
    for p in closed:
        reason = p.get("exit_reason") or "UNKNOWN"
        pnl = p.get("realized_pnl", 0)
        exit_stats[reason]["count"] += 1
        exit_stats[reason]["pnl"] += pnl
        if pnl > 0:
            exit_stats[reason]["wins"] += 1
    
    for reason, stats in sorted(exit_stats.items(), key=lambda x: -x[1]["pnl"]):
        wr = (stats["wins"] / stats["count"] * 100) if stats["count"] > 0 else 0
        print(f"  {reason:<15} | Сделок: {stats['count']:<3} | Win: {wr:.0f}% | P&L: {stats['pnl']:+.2f} USDT")
    print()
    
    # === АНОМАЛИИ ===
    print("⚠️  АНОМАЛИИ (требуют проверки)")
    print("-" * 80)
    anomalies = []
    
    for p in closed:
        issues = []
        
        # 1. Закрытие по BE
        if p.get("exit_reason") == "BE":
            issues.append("Закрытие по BE")
        
        # 2. ERROR_STATE
        if p.get("status") == "ERROR_STATE":
            issues.append(f"Статус ERROR_STATE")
        
        # 3. Выход по TP с убытком
        if p.get("exit_reason") in ("TP1", "TP2") and p.get("realized_pnl", 0) < 0:
            issues.append(f"Выход по {p.get('exit_reason')} с убытком")
        
        # 4. SL ниже entry для SHORT (баг!)
        if p.get("side") == "SHORT" and p.get("sl_price", 0) < p.get("entry_price", 0):
            issues.append(f"SL ({p.get('sl_price')}) НИЖЕ entry ({p.get('entry_price')}) для SHORT!")
        
        # 5. SL выше entry для LONG (баг!)
        if p.get("side") == "LONG" and p.get("sl_price", 0) > p.get("entry_price", 0):
            issues.append(f"SL ({p.get('sl_price')}) ВЫШЕ entry ({p.get('entry_price')}) для LONG!")
        
        # 6. Ошибка защиты
        timeline_str = json.dumps(p.get("timeline", []))
        if "PROTECTIVE_FAILED" in timeline_str:
            issues.append("Ошибка выставления защиты (PROTECTIVE_FAILED)")
        
        if issues:
            anomalies.append({
                "passport": p["passport_id"],
                "side": p.get("side", "?"),
                "entry": p.get("entry_price", 0),
                "sl": p.get("sl_price", 0),
                "pnl": p.get("realized_pnl", 0),
                "exit": p.get("exit_reason", "?"),
                "issues": ", ".join(issues),
                "created_at": p.get("created_at", "")
            })
    
    if anomalies:
        for a in anomalies:
            print(f"  [{a['passport']}] {a['side']} @ {a['entry']}")
            print(f"    SL: {a['sl']} | Exit: {a['exit']} | P&L: {a['pnl']:+.2f}$")
            print(f"    ️  {a['issues']}")
            print(f"    📅 {a['created_at']}")
            print()
    else:
        print("  ✅ Аномалий не обнаружено.")
    print()
    
    # === ПОСЛЕДНИЕ 10 СДЕЛОК ===
    print("🕐 ПОСЛЕДНИЕ 10 СДЕЛОК")
    print("-" * 80)
    for p in sorted(closed, key=lambda x: x.get("closed_at", ""), reverse=True)[:10]:
        side = p.get("side", "?")
        pnl = p.get("realized_pnl", 0)
        reason = p.get("exit_reason", "?")
        entry = p.get("entry_price", 0)
        exit_p = p.get("exit_price", 0)
        sl = p.get("sl_price", 0)
        print(f"  {side:<5} | Entry: {entry:<8} | SL: {sl:<8} | Exit: {exit_p:<8} | {reason:<10} | P&L: {pnl:+.2f} USDT")
    print()
    
    print("=" * 80)
    print("✅ Анализ завершен.")

if __name__ == "__main__":
    passports = load_passports()
    analyze(passports)