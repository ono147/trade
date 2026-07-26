import json
import warnings
from collections import defaultdict

import numpy as np
import pandas as pd
import yfinance as yf

from simulation_realistic import (
    DEFAULT_RANK_FRACTION,
    compute_rank_top_n,
    get_session_profile,
    run_all_virtual_trades,
    run_daily_selection,
)

warnings.filterwarnings("ignore")


US_DOW30 = [
    ("AAPL", "Apple"),
    ("AMGN", "Amgen"),
    ("AXP", "American Express"),
    ("BA", "Boeing"),
    ("CAT", "Caterpillar"),
    ("CRM", "Salesforce"),
    ("CSCO", "Cisco"),
    ("CVX", "Chevron"),
    ("DIS", "Walt Disney"),
    ("GS", "Goldman Sachs"),
    ("HD", "Home Depot"),
    ("HON", "Honeywell"),
    ("IBM", "IBM"),
    ("INTC", "Intel"),
    ("JNJ", "Johnson & Johnson"),
    ("JPM", "JPMorgan"),
    ("KO", "Coca-Cola"),
    ("MCD", "McDonald's"),
    ("MMM", "3M"),
    ("MRK", "Merck"),
    ("MSFT", "Microsoft"),
    ("NKE", "Nike"),
    ("PG", "Procter & Gamble"),
    ("TRV", "Travelers"),
    ("UNH", "UnitedHealth"),
    ("V", "Visa"),
    ("VZ", "Verizon"),
    ("WBA", "Walgreens Boots Alliance"),
    ("WMT", "Walmart"),
    ("XOM", "Exxon Mobil"),
]


def download_with_retry(max_retries: int = 3, retry_wait_sec: float = 1.5, **download_kwargs) -> pd.DataFrame:
    import time

    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            data = yf.download(**download_kwargs)
            if data is not None and not data.empty:
                return data
            last_error = RuntimeError("empty data")
        except Exception as e:
            last_error = e

        if attempt < max_retries:
            wait_sec = retry_wait_sec * attempt
            print(
                f"retry {attempt}/{max_retries} interval={download_kwargs.get('interval', 'n/a')} wait={wait_sec:.1f}s"
            )
            time.sleep(wait_sec)

    print(f"failed finally interval={download_kwargs.get('interval', 'n/a')} err={last_error}")
    return pd.DataFrame()


def extract_symbol_frame(raw_data: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if raw_data is None or raw_data.empty:
        return pd.DataFrame()
    try:
        df = raw_data[symbol].copy().dropna()
        return df if not df.empty else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def fetch_single_symbol(symbol: str, interval: str) -> pd.DataFrame:
    single = download_with_retry(
        tickers=symbol,
        period="60d",
        interval=interval,
        auto_adjust=True,
        progress=False,
        threads=False,
    )
    if single is None or single.empty:
        return pd.DataFrame()
    return single.dropna()


def load_us_market_data():
    data_15m, intra_data = {}, {}
    symbols = [s[0] for s in US_DOW30]

    print("US data bulk download (15m)...")
    data_15m_raw = download_with_retry(
        tickers=" ".join(symbols),
        period="60d",
        interval="15m",
        group_by="ticker",
        auto_adjust=True,
        progress=False,
        threads=True,
    )
    print("US data bulk download (5m)...")
    intra_data_raw = download_with_retry(
        tickers=" ".join(symbols),
        period="60d",
        interval="5m",
        group_by="ticker",
        auto_adjust=True,
        progress=False,
        threads=True,
    )

    recovered_15m = 0
    recovered_5m = 0
    failed_15m = []
    failed_5m = []

    for sym in symbols:
        d15 = extract_symbol_frame(data_15m_raw, sym)
        if d15.empty:
            d15 = fetch_single_symbol(sym, "15m")
            if not d15.empty:
                recovered_15m += 1
            else:
                failed_15m.append(sym)
        if not d15.empty and d15.index.tz is not None:
            d15.index = d15.index.tz_convert("America/New_York").tz_localize(None)
        data_15m[sym] = d15

        d5 = extract_symbol_frame(intra_data_raw, sym)
        if d5.empty:
            d5 = fetch_single_symbol(sym, "5m")
            if not d5.empty:
                recovered_5m += 1
            else:
                failed_5m.append(sym)
        if not d5.empty and d5.index.tz is not None:
            d5.index = d5.index.tz_convert("America/New_York").tz_localize(None)
        intra_data[sym] = d5

    print(f"recovered: 15m={recovered_15m}, 5m={recovered_5m}")
    print(f"failed: 15m={len(failed_15m)} {failed_15m}, 5m={len(failed_5m)} {failed_5m}")

    all_dates = set()
    for df in data_15m.values():
        if not df.empty:
            all_dates.update(df.index.strftime("%Y-%m-%d"))
    common_dates = sorted(list(all_dates))
    test_dates = common_dates[10:] if len(common_dates) > 10 else []
    return data_15m, intra_data, test_dates


def run_full_backtest_us(
    data_15m: dict,
    intra_data: dict,
    test_dates: list,
    volume_surge_mult: float,
    *,
    index_constituent_count: int = 30,
    rank_fraction: float | None = None,
    rank_top_n: int | None = None,
):
    initial_cash = 1_000_000
    current_cash = initial_cash
    total_profit = 0.0
    all_trade_logs = []
    session = get_session_profile("US")
    rf = DEFAULT_RANK_FRACTION if rank_fraction is None else float(rank_fraction)
    top_n = compute_rank_top_n(int(index_constituent_count), rf, rank_top_n)

    for target_date in test_dates:
        symbol_scores = []
        for sym_code, sym_name in US_DOW30:
            d_df = data_15m.get(sym_code, pd.DataFrame())
            if d_df.empty:
                continue

            d_df_prev = d_df[d_df.index.strftime("%Y-%m-%d") < target_date]
            if len(d_df_prev) < 130:
                continue

            score = run_daily_selection(d_df_prev)
            symbol_scores.append((sym_code, score, sym_name))

        symbol_scores.sort(key=lambda x: x[1], reverse=True)
        target_stocks = symbol_scores[: min(top_n, len(symbol_scores))]

        daily_profit, daily_logs = run_all_virtual_trades(
            intra_data, target_stocks, current_cash, target_date, volume_surge_mult, session=session
        )
        current_cash += daily_profit
        total_profit += daily_profit

        symbol_map = {s[0]: s[2] for s in target_stocks}
        for log in daily_logs:
            log["date"] = target_date
            log["name"] = symbol_map.get(log["symbol"], log["symbol"])
            all_trade_logs.append(log)

    return total_profit, current_cash, all_trade_logs


def summarize_logs(logs: list[dict]) -> dict:
    pnls = [float(x["pnl"]) for x in logs]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    reasons = defaultdict(int)
    reason_pnl = defaultdict(float)

    for x in logs:
        reasons[x["reason"]] += 1
        reason_pnl[x["reason"]] += float(x["pnl"])

    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    return {
        "trades": len(logs),
        "win_rate": (len(wins) / (len(wins) + len(losses))) if (len(wins) + len(losses)) > 0 else 0.0,
        "avg_pnl": (sum(pnls) / len(pnls)) if pnls else 0.0,
        "profit_factor": (gross_profit / gross_loss) if gross_loss > 0 else None,
        "reason_counts": dict(reasons),
        "reason_pnl": dict(reason_pnl),
    }


def main() -> None:
    initial_cash = 1_000_000
    mults = [round(1.0 + i * 0.1, 2) for i in range(11)]  # 1.0 .. 2.0

    print("=" * 70)
    print("US (Dow30) volume-mult sweep 0.1 step (1.0-2.0)")
    print("=" * 70)

    data_15m, intra_data, test_dates = load_us_market_data()
    print(f"test dates: {len(test_dates)}")

    rows = []
    result_json = {}
    for m in mults:
        tp, fc, logs = run_full_backtest_us(data_15m, intra_data, test_dates, m)
        ret_pct = (fc - initial_cash) / initial_cash * 100.0
        n = len(logs)
        rows.append((m, tp, fc, n, ret_pct))
        info = summarize_logs(logs)
        info["total_profit"] = tp
        info["final_cash"] = fc
        info["return_pct"] = ret_pct
        result_json[f"{m:.2f}"] = info
        print(f"  x{m:.2f} | 損益 {tp:+,.0f} 円 | 資金 {fc:,.0f} 円 | 約定 {n} | {ret_pct:+.2f}%")

    print("\n" + "=" * 70)
    print(" US比較サマリー（0.1刻み）")
    print("=" * 70)
    print(f"  {'出来高倍率':>10} | {'合計損益(円)':>14} | {'最終資金(円)':>16} | {'約定数':>8} | {'リターン%':>10}")
    print("  " + "-" * 66)
    for m, tp, fc, n, ret in rows:
        print(f"  {m:>10.2f} | {tp:>+14,.0f} | {fc:>16,.0f} | {n:>8} | {ret:>+9.2f}%")
    best = max(rows, key=lambda x: x[4])
    print("=" * 70)
    print(f"BEST: x{best[0]:.2f} / 損益 {best[1]:+,.0f} 円 / 資金 {best[2]:,.0f} 円 / {best[4]:+.2f}%")
    print("=" * 70)

    with open("us_volume_sweep_analysis.json", "w", encoding="utf-8") as f:
        json.dump(result_json, f, ensure_ascii=False, indent=2)
    print("saved: us_volume_sweep_analysis.json")


if __name__ == "__main__":
    main()
