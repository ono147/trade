import json
import time
from io import StringIO

import pandas as pd
import requests

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


def build_target_stocks_by_date(
    symbols_with_name,
    data_15m: dict,
    test_dates: list[str],
    excluded_by_date: dict[str, set[str]] | None = None,
    *,
    index_constituent_count: int | None = None,
    rank_fraction: float | None = None,
):
    idx_n = int(index_constituent_count) if index_constituent_count is not None else len(NIKKEI225)
    rf = DEFAULT_RANK_FRACTION if rank_fraction is None else float(rank_fraction)
    top_n = compute_rank_top_n(idx_n, rf, None)
    targets_by_date = {}
    for target_date in test_dates:
        excluded = excluded_by_date.get(target_date, set()) if excluded_by_date else set()
        symbol_scores = []
        for sym_code, sym_name in symbols_with_name:
            if sym_code in excluded:
                continue
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


def run_backtest_with_targets(
    intra_data: dict,
    test_dates: list[str],
    target_stocks_by_date: dict[str, list[tuple]],
    volume_surge_mult: float,
    *,
    session=None,
):
    initial_cash = 1_000_000
    current_cash = initial_cash
    total_profit = 0.0
    trade_count = 0

    for target_date in test_dates:
        target_stocks = target_stocks_by_date.get(target_date, [])
        daily_profit, daily_logs = run_all_virtual_trades(
            intra_data,
            target_stocks,
            current_cash,
            target_date,
            volume_surge_mult,
            session=session,
        )
        current_cash += daily_profit
        total_profit += daily_profit
        trade_count += len(daily_logs)

    ret_pct = (current_cash - initial_cash) / initial_cash * 100.0
    return {
        "total_profit": float(total_profit),
        "final_cash": float(current_cash),
        "trades": int(trade_count),
        "return_pct": float(ret_pct),
    }


def get_table_symbols(url: str, symbol_col: str) -> list[str]:
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    table = pd.read_html(StringIO(resp.text))[0]
    return table[symbol_col].astype(str).tolist()


def get_sp500_symbols() -> list[str]:
    symbols = get_table_symbols("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", "Symbol")
    return [s.replace(".", "-") for s in symbols]


def get_ftse100_symbols() -> list[str]:
    symbols = get_table_symbols("https://en.wikipedia.org/wiki/FTSE_100_Index", "EPIC")
    return [f"{s}.L" for s in symbols]


def get_dax40_symbols() -> list[str]:
    symbols = get_table_symbols("https://en.wikipedia.org/wiki/DAX", "Ticker symbol")
    return [s if "." in s else f"{s}.DE" for s in symbols]


def get_cac40_symbols() -> list[str]:
    symbols = get_table_symbols("https://en.wikipedia.org/wiki/CAC_40", "Ticker")
    return [s if "." in s else f"{s}.PA" for s in symbols]


def main():
    started = time.time()
    result = {
        "meta": {"started_at": time.strftime("%Y-%m-%d %H:%M:%S")},
        "jp_sweep_step_005": [],
        "cross_market_check": {},
    }

    # Japan sweep: 1.00 - 2.00 step 0.05
    data_15m_jp, intra_jp, dates_jp = load_market_data()
    earnings_by_date = {d: set(get_earnings_tickers(d)) for d in dates_jp}
    jp_mults = [round(1.0 + i * 0.05, 2) for i in range(21)]
    jp_targets_by_date = build_target_stocks_by_date(
        NIKKEI225,
        data_15m_jp,
        dates_jp,
        earnings_by_date,
        index_constituent_count=len(NIKKEI225),
    )
    jp_sess = get_session_profile("JP")
    for m in jp_mults:
        metrics = run_backtest_with_targets(
            intra_jp, dates_jp, jp_targets_by_date, m, session=jp_sess
        )
        result["jp_sweep_step_005"].append({"mult": m, **metrics})
        print(
            f"[JP] x{m:.2f} | ret {metrics['return_pct']:+.2f}% | pnl {metrics['total_profit']:+,.0f} | trades {metrics['trades']}"
        )

    best_jp = max(result["jp_sweep_step_005"], key=lambda x: x["return_pct"])
    best_mult = float(best_jp["mult"])
    result["meta"]["jp_best_mult"] = best_mult
    result["meta"]["jp_dates"] = len(dates_jp)

    # Cross-market check at JP best multiplier
    universes = {
        "US_DOW30": ([x for x in DOW30], "America/New_York", "US", 30),
        "US_SP500": ([(s, s) for s in get_sp500_symbols()], "America/New_York", "US", 503),
        "UK_FTSE100": ([(s, s) for s in get_ftse100_symbols()], "Europe/London", "UK", 100),
        "DE_DAX40": ([(s, s) for s in get_dax40_symbols()], "Europe/Berlin", "DE", 40),
        "FR_CAC40": ([(s, s) for s in get_cac40_symbols()], "Europe/Paris", "FR", 40),
    }

    for uname, (symbols_with_name, ex_tz, session_key, index_n) in universes.items():
        symbols = sorted(list(dict.fromkeys([s[0] for s in symbols_with_name])))
        name_map = {s: n for s, n in symbols_with_name}
        try:
            d15, d5, dates, active_symbols, failed15, failed5 = load_us_like_market_data(
                symbols, chunk_size=80, exchange_tz=ex_tz
            )
            active_with_name = [(s, name_map.get(s, s)) for s in active_symbols]
            targets_by_date = build_target_stocks_by_date(
                active_with_name,
                d15,
                dates,
                excluded_by_date=None,
                index_constituent_count=index_n,
            )
            metrics = run_backtest_with_targets(
                d5, dates, targets_by_date, best_mult, session=get_session_profile(session_key)
            )
            result["cross_market_check"][uname] = {
                "universe_size": len(symbols),
                "active_symbols": len(active_symbols),
                "dates": len(dates),
                "failed_15m": failed15,
                "failed_5m": failed5,
                "mult": best_mult,
                **metrics,
            }
            print(
                f"[{uname}] active={len(active_symbols)} dates={len(dates)} x{best_mult:.2f} "
                f"ret {metrics['return_pct']:+.2f}% pnl {metrics['total_profit']:+,.0f} trades {metrics['trades']}"
            )
        except Exception as e:
            result["cross_market_check"][uname] = {"error": str(e), "mult": best_mult}
            print(f"[{uname}] ERROR: {e}")

    result["meta"]["elapsed_sec"] = round(time.time() - started, 2)
    result["meta"]["finished_at"] = time.strftime("%Y-%m-%d %H:%M:%S")

    with open("global_volume_validation_results.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print("saved: global_volume_validation_results.json")


if __name__ == "__main__":
    main()
