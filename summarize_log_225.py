import re
from collections import OrderedDict

log_file = "simulation_daily_report_225.txt"
summary = OrderedDict()

with open(log_file, "r") as f:
    lines = f.readlines()

current_date = None
current_targets = ""

for line in lines:
    m_date = re.match(r"\[(\d{4}-\d{2}-\d{2}) の運用結果\]", line)
    if m_date:
        current_date = m_date.group(1)
        continue
    
    if current_date and "対象:" in line:
        current_targets = line.split("対象:")[1].strip()
        continue
        
    m_profit = re.search(r"損益:\s+([+-]?[\d,]+)\s*円\s*\|\s*資金:\s*([\d,]+)\s*円", line)
    if current_date and current_targets and m_profit:
        profit = int(m_profit.group(1).replace(",", ""))
        capital = int(m_profit.group(2).replace(",", ""))
        
        summary[current_date] = {
            "targets": current_targets,
            "profit": profit,
            "capital": capital
        }

total_profit = 0
print("======================================================================")
print("💰 日経225(約215銘柄) 過去60日間の日別損益レポート (100万円・ハイブリッド戦略)")
print("======================================================================\n")

for date, data in summary.items():
    p = data['profit']
    c = data['capital']
    t = data['targets']
    total_profit += p
    print(f"[{date}] 損益: {p:+7,} 円 | 資金: {c:>10,} 円 | 対象: {t}")

print("\n======================================================================")
print(f"  合計損益: {total_profit:+10,} 円")
print("======================================================================")
