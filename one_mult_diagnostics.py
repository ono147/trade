import json
from collections import Counter, defaultdict

import pandas as pd

from nikkei225_list import NIKKEI225
from simulation_realistic import (
    DEFAULT_RANK_FRACTION,
    compute_rank_top_n,
    get_earnings_tickers,
    get_session_profile,
    load_market_data,
    run_all_virtual_trades,
    run_daily_selection,
)
from us_robustness_validation import DOW30, load_market_data as load_us_like_market_data


def build_targets(
    symbols_with_name,
    d15,
    dates,
    excluded_by_date=None,
    *,
    index_constituent_count: int,
    rank_fraction: float | None = None,
):
    rf = DEFAULT_RANK_FRACTION if rank_fraction is None else float(rank_fraction)
    top_n = compute_rank_top_n(index_constituent_count, rf, None)
    out = {}
    for d in dates:
        excluded = excluded_by_date.get(d, set()) if excluded_by_date else set()
        scores = []
        for sym, name in symbols_with_name:
            df = d15.get(sym, pd.DataFrame())
            if df.empty or sym in excluded:
                continue
            prev = df[df.index.strftime("%Y-%m-%d") < d]
            if len(prev) < 130:
                continue
            score = run_daily_selection(prev)
            scores.append((sym, score, name))
        scores.sort(key=lambda x: x[1], reverse=True)
        out[d] = scores[: min(top_n, len(scores))]
    return out


def run_market(intra_data, dates, targets, mult, session):
    initial_cash = 1_000_000
    cash = initial_cash
    logs = []
    daily = []
    for d in dates:
        pnl, day_logs = run_all_virtual_trades(
            intra_data, targets.get(d, []), cash, d, mult, session=session
        )
        cash += pnl
        daily.append(pnl)
        for x in day_logs:
            x["date"] = d
            logs.append(x)
    return initial_cash, cash, daily, logs


def summarize(initial_cash, cash, daily, logs):
    pnls = [float(x["pnl"]) for x in logs]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    reason_counts = Counter([x["reason"] for x in logs])
    reason_pnl = defaultdict(float)
    for x in logs:
        reason_pnl[x["reason"]] += float(x["pnl"])
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    return {
        "return_pct": (cash - initial_cash) / initial_cash * 100.0,
        "trades": len(logs),
        "win_rate": (len(wins) / (len(wins) + len(losses))) if (wins or losses) else 0.0,
        "avg_win": (sum(wins) / len(wins)) if wins else 0.0,
        "avg_loss": (sum(losses) / len(losses)) if losses else 0.0,
        "profit_factor": (gross_profit / gross_loss) if gross_loss else None,
        "expectancy": (sum(pnls) / len(pnls)) if pnls else 0.0,
        "daily_positive": len([x for x in daily if x > 0]),
        "daily_negative": len([x for x in daily if x < 0]),
        "reason_counts": dict(reason_counts),
        "reason_pnl": dict(reason_pnl),
    }


def main():
    mult = 1.35
    result = {"mult": mult, "markets": {}}

    d15_jp, d5_jp, dates_jp = load_market_data()
    earnings = {d: set(get_earnings_tickers(d)) for d in dates_jp}
    t_jp = build_targets(
        NIKKEI225, d15_jp, dates_jp, earnings, index_constituent_count=len(NIKKEI225)
    )
    i, c, daily, logs = run_market(d5_jp, dates_jp, t_jp, mult, get_session_profile("JP"))
    result["markets"]["JP_NIKKEI225"] = {"dates": len(dates_jp), **summarize(i, c, daily, logs)}

    universes = {
        "US_DOW30": (DOW30, "America/New_York", "US", 30),
        "US_SP500_20": (
            [
                ("AAPL", "AAPL"), ("MSFT", "MSFT"), ("NVDA", "NVDA"), ("AMZN", "AMZN"), ("META", "META"),
                ("GOOGL", "GOOGL"), ("BRK-B", "BRK-B"), ("JPM", "JPM"), ("XOM", "XOM"), ("UNH", "UNH"),
                ("JNJ", "JNJ"), ("V", "V"), ("PG", "PG"), ("MA", "MA"), ("HD", "HD"),
                ("CVX", "CVX"), ("ABBV", "ABBV"), ("MRK", "MRK"), ("COST", "COST"), ("KO", "KO"),
            ],
            "America/New_York",
            "US",
            503,
        ),
        "UK_LSE_TOP10": (
            [("SHEL.L", "SHEL.L"), ("AZN.L", "AZN.L"), ("HSBA.L", "HSBA.L"), ("ULVR.L", "ULVR.L"), ("BP.L", "BP.L"), ("GSK.L", "GSK.L"), ("RIO.L", "RIO.L"), ("BATS.L", "BATS.L"), ("DGE.L", "DGE.L"), ("LLOY.L", "LLOY.L")],
            "Europe/London",
            "UK",
            100,
        ),
        "DE_XETRA_TOP10": (
            [("SAP.DE", "SAP.DE"), ("SIE.DE", "SIE.DE"), ("ALV.DE", "ALV.DE"), ("BAS.DE", "BAS.DE"), ("MBG.DE", "MBG.DE"), ("BMW.DE", "BMW.DE"), ("DTE.DE", "DTE.DE"), ("IFX.DE", "IFX.DE"), ("VOW3.DE", "VOW3.DE"), ("ADS.DE", "ADS.DE")],
            "Europe/Berlin",
            "DE",
            40,
        ),
        "FR_PARIS_TOP10": (
            [("MC.PA", "MC.PA"), ("OR.PA", "OR.PA"), ("TTE.PA", "TTE.PA"), ("SAN.PA", "SAN.PA"), ("BNP.PA", "BNP.PA"), ("AIR.PA", "AIR.PA"), ("SU.PA", "SU.PA"), ("EL.PA", "EL.PA"), ("CS.PA", "CS.PA"), ("RI.PA", "RI.PA")],
            "Europe/Paris",
            "FR",
            40,
        ),
    }
    for name, (pairs, ex_tz, session_key, index_n) in universes.items():
        if pairs and isinstance(pairs[0], str):
            syms = sorted(list(dict.fromkeys(pairs)))
            name_map = {s: s for s in syms}
        else:
            syms = sorted(list(dict.fromkeys([x[0] for x in pairs])))
            name_map = {x[0]: x[1] for x in pairs}
        d15, d5, dates, active, f15, f5 = load_us_like_market_data(syms, chunk_size=80, exchange_tz=ex_tz)
        active_pairs = [(s, name_map.get(s, s)) for s in active]
        targets = build_targets(
            active_pairs, d15, dates, excluded_by_date=None, index_constituent_count=index_n
        )
        i, c, daily, logs = run_market(d5, dates, targets, mult, get_session_profile(session_key))
        result["markets"][name] = {
            "dates": len(dates),
            "active_symbols": len(active),
            "failed_15m": len(f15),
            "failed_5m": len(f5),
            **summarize(i, c, daily, logs),
        }

    with open("one_mult_diagnostics.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print("saved: one_mult_diagnostics.json")


if __name__ == "__main__":
    main()
