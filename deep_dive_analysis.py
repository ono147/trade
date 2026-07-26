import json
from collections import Counter, defaultdict

import pandas as pd

from nikkei225_list import NIKKEI225
from simulation_realistic import (
    get_earnings_tickers,
    load_market_data,
    run_all_virtual_trades,
    run_daily_selection,
)
from us_robustness_validation import DOW30, load_market_data as load_us_like_market_data


def build_targets_by_date(symbols_with_name, data_15m, test_dates, excluded_by_date=None):
    targets = {}
    for d in test_dates:
        excluded = excluded_by_date.get(d, set()) if excluded_by_date else set()
        scores = []
        for sym, name in symbols_with_name:
            if sym in excluded:
                continue
            df = data_15m.get(sym, pd.DataFrame())
            if df.empty:
                continue
            prev = df[df.index.strftime("%Y-%m-%d") < d]
            if len(prev) < 130:
                continue
            s = run_daily_selection(prev)
            scores.append((sym, s, name))
        scores.sort(key=lambda x: x[1], reverse=True)
        targets[d] = scores[:80]
    return targets


def run_with_targets(intra_data, test_dates, targets_by_date, mult):
    initial_cash = 1_000_000
    cash = initial_cash
    logs = []
    daily = []
    for d in test_dates:
        targets = targets_by_date.get(d, [])
        pnl, day_logs = run_all_virtual_trades(intra_data, targets, cash, d, mult)
        cash += pnl
        daily.append((d, pnl))
        for x in day_logs:
            x["date"] = d
            logs.append(x)
    return initial_cash, cash, daily, logs


def summarize_logs(initial_cash, final_cash, daily, logs):
    pnls = [float(x["pnl"]) for x in logs]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    reason_counts = Counter([x["reason"] for x in logs])
    reason_pnl = defaultdict(float)
    for x in logs:
        reason_pnl[x["reason"]] += float(x["pnl"])

    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    nonzero_days = [p for _, p in daily if p != 0]
    pos_days = len([p for _, p in daily if p > 0])
    neg_days = len([p for _, p in daily if p < 0])

    return {
        "final_cash": final_cash,
        "return_pct": (final_cash - initial_cash) / initial_cash * 100.0,
        "trades": len(logs),
        "win_rate": (len(wins) / (len(wins) + len(losses))) if (wins or losses) else 0.0,
        "avg_win": (sum(wins) / len(wins)) if wins else 0.0,
        "avg_loss": (sum(losses) / len(losses)) if losses else 0.0,
        "expectancy_per_trade": (sum(pnls) / len(pnls)) if pnls else 0.0,
        "profit_factor": (gross_profit / gross_loss) if gross_loss > 0 else None,
        "median_trade_pnl": float(pd.Series(pnls).median()) if pnls else 0.0,
        "positive_days": pos_days,
        "negative_days": neg_days,
        "nonzero_days": len(nonzero_days),
        "avg_daily_pnl_nonzero": (sum(nonzero_days) / len(nonzero_days)) if nonzero_days else 0.0,
        "reason_counts": dict(reason_counts),
        "reason_pnl": dict(reason_pnl),
    }


def sweep_mults(intra_data, test_dates, targets_by_date, mults):
    rows = []
    for m in mults:
        initial_cash, final_cash, daily, logs = run_with_targets(intra_data, test_dates, targets_by_date, m)
        rows.append(
            {
                "mult": m,
                "return_pct": (final_cash - initial_cash) / initial_cash * 100.0,
                "trades": len(logs),
                "expectancy_per_trade": (sum(float(x["pnl"]) for x in logs) / len(logs)) if logs else 0.0,
            }
        )
    return rows


def main():
    result = {"jp": {}, "markets": {}}

    # JP
    d15_jp, d5_jp, dates_jp = load_market_data()
    earnings = {d: set(get_earnings_tickers(d)) for d in dates_jp}
    targets_jp = build_targets_by_date(NIKKEI225, d15_jp, dates_jp, earnings)
    jp_mults = [round(1.0 + 0.05 * i, 2) for i in range(21)]
    sweep_jp = sweep_mults(d5_jp, dates_jp, targets_jp, jp_mults)
    best_jp = max(sweep_jp, key=lambda x: x["return_pct"])
    i0, f0, daily0, logs0 = run_with_targets(d5_jp, dates_jp, targets_jp, best_jp["mult"])
    result["jp"] = {
        "dates": len(dates_jp),
        "sweep": sweep_jp,
        "best_mult": best_jp["mult"],
        "best_stats": summarize_logs(i0, f0, daily0, logs0),
    }

    universes = {
        "US_DOW30": DOW30,
        "US_SP500_20": [
            ("AAPL", "AAPL"), ("MSFT", "MSFT"), ("NVDA", "NVDA"), ("AMZN", "AMZN"), ("META", "META"),
            ("GOOGL", "GOOGL"), ("BRK-B", "BRK-B"), ("JPM", "JPM"), ("XOM", "XOM"), ("UNH", "UNH"),
            ("JNJ", "JNJ"), ("V", "V"), ("PG", "PG"), ("MA", "MA"), ("HD", "HD"),
            ("CVX", "CVX"), ("ABBV", "ABBV"), ("MRK", "MRK"), ("COST", "COST"), ("KO", "KO"),
        ],
        "UK_LSE_TOP10": [("SHEL.L", "SHEL.L"), ("AZN.L", "AZN.L"), ("HSBA.L", "HSBA.L"), ("ULVR.L", "ULVR.L"), ("BP.L", "BP.L"), ("GSK.L", "GSK.L"), ("RIO.L", "RIO.L"), ("BATS.L", "BATS.L"), ("DGE.L", "DGE.L"), ("LLOY.L", "LLOY.L")],
        "DE_XETRA_TOP10": [("SAP.DE", "SAP.DE"), ("SIE.DE", "SIE.DE"), ("ALV.DE", "ALV.DE"), ("BAS.DE", "BAS.DE"), ("MBG.DE", "MBG.DE"), ("BMW.DE", "BMW.DE"), ("DTE.DE", "DTE.DE"), ("IFX.DE", "IFX.DE"), ("VOW3.DE", "VOW3.DE"), ("ADS.DE", "ADS.DE")],
        "FR_PARIS_TOP10": [("MC.PA", "MC.PA"), ("OR.PA", "OR.PA"), ("TTE.PA", "TTE.PA"), ("SAN.PA", "SAN.PA"), ("BNP.PA", "BNP.PA"), ("AIR.PA", "AIR.PA"), ("SU.PA", "SU.PA"), ("EL.PA", "EL.PA"), ("CS.PA", "CS.PA"), ("RI.PA", "RI.PA")],
    }

    for name, symbols_with_name in universes.items():
        syms = sorted(list(dict.fromkeys([x[0] for x in symbols_with_name])))
        name_map = {x[0]: x[1] for x in symbols_with_name}
        d15, d5, dates, active, failed15, failed5 = load_us_like_market_data(syms, chunk_size=80)
        active_pairs = [(s, name_map.get(s, s)) for s in active]
        targets = build_targets_by_date(active_pairs, d15, dates, excluded_by_date=None)
        coarse = [round(1.0 + 0.1 * i, 2) for i in range(11)]
        sweep_coarse = sweep_mults(d5, dates, targets, coarse)
        coarse_best = max(sweep_coarse, key=lambda x: x["return_pct"])
        c = coarse_best["mult"]
        refine = sorted(list(dict.fromkeys([round(x, 2) for x in [c - 0.1, c - 0.05, c, c + 0.05, c + 0.1] if 1.0 <= x <= 2.0])))
        sweep_refine = sweep_mults(d5, dates, targets, refine)
        sweep = sorted({x["mult"]: x for x in (sweep_coarse + sweep_refine)}.values(), key=lambda z: z["mult"])
        best = max(sweep, key=lambda x: x["return_pct"])
        i1, f1, daily1, logs1 = run_with_targets(d5, dates, targets, result["jp"]["best_mult"])
        i2, f2, daily2, logs2 = run_with_targets(d5, dates, targets, best["mult"])
        result["markets"][name] = {
            "dates": len(dates),
            "active_symbols": len(active),
            "failed_15m_count": len(failed15),
            "failed_5m_count": len(failed5),
            "sweep": sweep,
            "best_mult_local": best["mult"],
            "stats_at_jp_best_mult": summarize_logs(i1, f1, daily1, logs1),
            "stats_at_local_best_mult": summarize_logs(i2, f2, daily2, logs2),
        }

    with open("deep_dive_analysis.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print("saved: deep_dive_analysis.json")


if __name__ == "__main__":
    main()
