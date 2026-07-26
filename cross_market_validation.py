import json
import time

import pandas as pd

from simulation_realistic import (
    DEFAULT_RANK_FRACTION,
    compute_rank_top_n,
    get_session_profile,
    run_all_virtual_trades,
    run_daily_selection,
)
from us_robustness_validation import DOW30, load_market_data as load_us_like_market_data


def normalize_symbols(symbols: list[str]) -> list[str]:
    return sorted(list(dict.fromkeys(symbols)))


def build_target_stocks_by_date(
    symbols: list[str],
    data_15m: dict,
    test_dates: list[str],
    index_constituent_count: int,
    rank_fraction: float | None = None,
):
    rf = DEFAULT_RANK_FRACTION if rank_fraction is None else float(rank_fraction)
    top_n = compute_rank_top_n(index_constituent_count, rf, None)
    targets_by_date = {}
    sym_with_name = [(s, s) for s in symbols]
    for target_date in test_dates:
        symbol_scores = []
        for sym_code, sym_name in sym_with_name:
            d_df = data_15m.get(sym_code, pd.DataFrame())
            if d_df.empty:
                continue
            d_df_prev = d_df[d_df.index.strftime("%Y-%m-%d") < target_date]
            if len(d_df_prev) < 130:
                continue
            score = run_daily_selection(d_df_prev)
            symbol_scores.append((sym_code, score, sym_name))
        symbol_scores.sort(key=lambda x: x[1], reverse=True)
        targets_by_date[target_date] = symbol_scores[: min(top_n, len(symbol_scores))]
    return targets_by_date


def run_with_targets(
    intra_data: dict,
    test_dates: list[str],
    targets_by_date: dict,
    volume_surge_mult: float,
    *,
    session,
):
    initial_cash = 1_000_000
    current_cash = initial_cash
    total_profit = 0.0
    trades = 0
    for d in test_dates:
        target_stocks = targets_by_date.get(d, [])
        daily_profit, daily_logs = run_all_virtual_trades(
            intra_data, target_stocks, current_cash, d, volume_surge_mult, session=session
        )
        current_cash += daily_profit
        total_profit += daily_profit
        trades += len(daily_logs)
    ret = (current_cash - initial_cash) / initial_cash * 100.0
    return {
        "total_profit": float(total_profit),
        "final_cash": float(current_cash),
        "trades": int(trades),
        "return_pct": float(ret),
    }


def main():
    t0 = time.time()
    best_mult_jp = 1.35
    result = {"meta": {"best_mult_from_jp": best_mult_jp}, "markets": {}}

    # Major liquid stocks from each market (Yahoo Finance tickers)
    universes = {
        "US_DOW30": (DOW30, "US", 30),
        "US_SP500": (
            [
                "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "BRK-B", "JPM", "XOM", "UNH",
                "JNJ", "V", "PG", "MA", "HD", "CVX", "ABBV", "MRK", "COST", "KO",
            ],
            "US",
            503,
        ),
        "UK_LSE_TOP": (
            ["SHEL.L", "AZN.L", "HSBA.L", "ULVR.L", "BP.L", "GSK.L", "RIO.L", "BATS.L", "DGE.L", "LLOY.L"],
            "UK",
            100,
        ),
        "DE_XETRA_TOP": (
            ["SAP.DE", "SIE.DE", "ALV.DE", "BAS.DE", "MBG.DE", "BMW.DE", "DTE.DE", "IFX.DE", "VOW3.DE", "ADS.DE"],
            "DE",
            40,
        ),
        "FR_PARIS_TOP": (
            ["MC.PA", "OR.PA", "TTE.PA", "SAN.PA", "BNP.PA", "AIR.PA", "SU.PA", "EL.PA", "CS.PA", "RI.PA"],
            "FR",
            40,
        ),
    }

    exchange_tz_by_name = {
        "US_DOW30": "America/New_York",
        "US_SP500": "America/New_York",
        "UK_LSE_TOP": "Europe/London",
        "DE_XETRA_TOP": "Europe/Berlin",
        "FR_PARIS_TOP": "Europe/Paris",
    }

    for name, (raw_symbols, session_key, index_n) in universes.items():
        try:
            symbols = normalize_symbols(raw_symbols)
            d15, d5, dates, active_symbols, failed15, failed5 = load_us_like_market_data(
                symbols, chunk_size=80, exchange_tz=exchange_tz_by_name[name]
            )
            sess = get_session_profile(session_key)
            targets = build_target_stocks_by_date(active_symbols, d15, dates, index_n)
            metrics = run_with_targets(d5, dates, targets, best_mult_jp, session=sess)
            result["markets"][name] = {
                "universe_size": len(symbols),
                "active_symbols": len(active_symbols),
                "dates": len(dates),
                "failed_15m": len(failed15),
                "failed_5m": len(failed5),
                **metrics,
            }
            print(
                f"[{name}] active={len(active_symbols)} dates={len(dates)} "
                f"ret={metrics['return_pct']:+.2f}% pnl={metrics['total_profit']:+,.0f} trades={metrics['trades']}"
            )
        except Exception as e:
            result["markets"][name] = {"error": str(e)}
            print(f"[{name}] ERROR {e}")

    result["meta"]["elapsed_sec"] = round(time.time() - t0, 2)
    with open("cross_market_validation_results.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print("saved: cross_market_validation_results.json")


if __name__ == "__main__":
    main()
