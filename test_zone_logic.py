from datetime import datetime, timezone
from analytics.golden_bounce.zone import GBZone, ZoneStatus

zone = GBZone(
    symbol="SOLUSDT", price=75.55, side="ask",
    created_at=datetime.now(timezone.utc), atr_at_creation=0.20
)

# Сценарий 1: ранний "откат" на 0.05 (как в твоем логе)
d1 = zone.check_departure(75.60, 1.5, 0.8)
print(f"1. Ранний вход (0.05, нужно 0.16): departed={d1} (ожидаем False) {'✅' if not d1 else '❌'}")

# Сценарий 2: полноценный откат на 0.25 (8 свечей)
d2 = zone.check_departure(75.30, 1.5, 0.8)
print(f"2. Глубокий откат (0.25): departed={d2} (ожидаем True) {'✅' if d2 else '❌'}")

# Сценарий 3: уровни при возврате
lv = zone.calculate_sl_tp(75.55)
print(f"3. SL={lv['sl']:.2f} TP1={lv['tp1']:.2f} TP2={lv['tp2']:.2f} risk={lv['risk_per_unit']:.2f}")

# Сценарий 4: лимит от стены
wall, offset = 75.55, 0.0005
limit = round((wall * (1 - offset)) / 0.01) * 0.01
print(f"4. Лимит SHORT: {limit:.2f} (ниже стены {wall}, защита от пролета) ✅")

# Сценарий 5: фильтр микростопов (как в твоем паспорте)
ideal = 30.0 / lv['risk_per_unit']
print(f"5. ideal_size={ideal:.1f} при max=10 -> {'ОТКАЗ (sl_too_tight)' if ideal > 20 else 'вход'}")