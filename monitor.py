"""
Мониторинг платформы Platon V2.
"""

import sys
import time
from pathlib import Path
from datetime import datetime

sys.path.append(str(Path(__file__).parent))

from core.metrics import metrics
from core.config import Config

def print_header(text: str):
    print(f"\n{'='*60}")
    print(f" {text}")
    print(f"{'='*60}")

def print_section(text: str):
    print(f"\n--- {text} ---")

def main():
    print_header("МОНИТОРИНГ ПЛАТФОРМЫ PLATON V2")
    
    print_section("СТАТУС ПЛАТФОРМЫ")
    config = Config("config.yaml")
    use_demo = config.get("bybit.use_demo")
    print(f"Режим: {'DEMO' if use_demo else 'TESTNET'}")
    print(f"Инструмент: {config.get('trading.symbol', 'SOLUSDT')}")
    print(f"Объем: {config.get('trading.planned_size', 7.0)} SOL")
    
    print_section("МЕТРИКИ REST API")
    rest = metrics.rest
    print(f"Всего запросов: {rest.total_requests}")
    print(f"Успешных: {rest.successful_requests}")
    print(f"Ошибок: {rest.failed_requests}")
    print(f"Успешность: {rest.success_rate:.1f}%")
    print(f"Среднее время ответа: {rest.avg_response_time*1000:.0f} мс")
    
    print_section("МЕТРИКИ UDS (WebSocket)")
    uds = metrics.uds
    print(f"Всего сообщений: {uds.total_messages}")
    print(f"Успешных подключений: {uds.successful_connections}")
    print(f"Переподключений: {uds.reconnections}")
    print(f"Аптайм: {round(uds.uptime_seconds / 60, 1)} мин")
    print(f"Сообщений в минуту: {uds.messages_per_min:.1f}")
    
    print_section("СТАТИСТИКА ПАСПОРТОВ")
    passports_dir = Path("data/passports")
    if passports_dir.exists():
        passports = list(passports_dir.glob("*.json"))
        print(f"Всего паспортов: {len(passports)}")
        
        if passports:
            import json
            statuses = {}
            reasons = {}
            total_pnl = 0.0
            
            for passport_file in passports:
                with open(passport_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    status = data.get('status', 'UNKNOWN')
                    statuses[status] = statuses.get(status, 0) + 1
                    if status == 'CLOSED':
                        reasons[data.get('exit_reason', 'UNKNOWN')] = reasons.get(data.get('exit_reason', 'UNKNOWN'), 0) + 1
                        total_pnl += data.get('net_pnl', 0)
            
            print("По статусам:")
            for status, count in sorted(statuses.items()):
                print(f"  {status}: {count}")
            
            if reasons:
                print("По причинам закрытия:")
                for reason, count in sorted(reasons.items()):
                    print(f"  {reason}: {count}")
            
            print(f"Общий P&L: {total_pnl:.2f} USDT")

    print_section("ПОСЛЕДНИЕ СОБЫТИЯ (лог)")
    log_file = Path("platform_run.log")
    if log_file.exists():
        # ИСПРАВЛЕНИЕ: жестко задаем utf-8 и игнорируем битые байты
        with open(log_file, 'r', encoding='utf-8', errors='replace') as f:
            lines = f.readlines()
            print("Последние 10 строк лога:")
            for line in lines[-10:]:
                print(f"  {line.rstrip()}")
    else:
        print("Лог-файл не найден")
    
    print_header("КОНЕЦ ОТЧЕТА")

if __name__ == "__main__":
    main()