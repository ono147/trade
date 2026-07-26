import json
import time
import warnings
from collections import defaultdict
from io import StringIO

import pandas as pd
import requests
import yfinance as yf

from simulation_realistic import (
    DEFAULT_RANK_FRACTION,
    compute_rank_top_n,
    get_session_profile,
    run_all_virtual_trades,
    run_daily_selection,
)

warnings.filterwarnings("ignore")


def get_sp500_symbols() -> list[str]:
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    table = pd.read_html(StringIO(resp.text))[0]
    symbols = table["Symbol"].astype(str).tolist()
    # Yahoo Finance uses "-" instead of "." in ticker symbols.
    return [s.replace(".", "-") for s in symbols]


def chunked(items: list[str], size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def download_with_retry(max_retries: int = 3, retry_wait_sec: float = 1.5, **download_kwargs) -> pd.DataFrame:
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
                f"retry {attempt}/{max_retries}: interval={download_kwargs.get('interval', 'n/a')} "
                f"wait={wait_sec:.1f}s"
            )
            time.sleep(wait_sec)

    print(f"final fail: interval={download_kwargs.get('interval', 'n/a')} err={last_error}")
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


def load_sp500_market_data(symbols: list[str], chunk_size: int = 80):
    data_15m, intra_data = {}, {}
    failed_15m, failed_5m = [], []
    recovered_15m, recovered_5m = 0, 0

    print(f"bulk download 15m by chunks (n={len(symbols)}, chunk={chunk_size})")
    raw_15m_by_symbol = {}
    for batch_idx, batch in enumerate(chunked(symbols, chunk_size), 1):
        print(f"  15m chunk {batch_idx}: {len(batch)} symbols")
        raw = download_with_retry(
            tickers=" ".join(batch),
            period="60d",
            interval="15m",
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=True,
        )
        for sym in batch:
            raw_15m_by_symbol[sym] = extract_symbol_frame(raw, sym)

    print(f"bulk download 5m by chunks (n={len(symbols)}, chunk={chunk_size})")
    raw_5m_by_symbol = {}
    for batch_idx, batch in enumerate(chunked(symbols, chunk_size), 1):
        print(f"  5m chunk {batch_idx}: {len(batch)} symbols")
        raw = download_with_retry(
            tickers=" ".join(batch),
            period="60d",
            interval="5m",
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=True,
        )
        for sym in batch:
            raw_5m_by_symbol[sym] = extract_symbol_frame(raw, sym)

    for sym in symbols:
        d15 = raw_15m_by_symbol.get(sym, pd.DataFrame())
        if d15.empty:
            d15 = fetch_single_symbol(sym, "15m")
            if not d15.empty:
                recovered_15m += 1
            else:
                failed_15m.append(sym)
        if not d15.empty and d15.index.tz is not None:
            d15.index = d15.index.tz_convert("America/New_York").tz_localize(None)
        data_15m[sym] = d15

        d5 = raw_5m_by_symbol.get(sym, pd.DataFrame())
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
    print(f"failed: 15m={len(failed_15m)}, 5m={len(failed_5m)}")

    all_dates = set()
    for df in data_15m.values():
        if not df.empty:
            all_dates.update(df.index.strftime("%Y-%m-%d"))
    common_dates = sorted(list(all_dates))
    test_dates = common_dates[10:] if len(common_dates) > 10 else []
    return data_15m, intra_data, test_dates, failed_15m, failed_5m


def run_full_backtest_sp500(
    symbols: list[str],
    data_15m: dict,
    intra_data: dict,
    test_dates: list[str],
    volume_surge_mult: float,
    *,
    index_constituent_count: int,
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
        for sym_code in symbols:
            d_df = data_15m.get(sym_code, pd.DataFrame())
            if d_df.empty:
                continue

            d_df_prev = d_df[d_df.index.strftime("%Y-%m-%d") < target_date]
            if len(d_df_prev) < 130:
                continue

            score = run_daily_selection(d_df_prev)
            symbol_scores.append((sym_code, score, sym_code))

        symbol_scores.sort(key=lambda x: x[1], reverse=True)
        target_stocks = symbol_scores[: min(top_n, len(symbol_scores))]

        daily_profit, daily_logs = run_all_virtual_trades(
            intra_data, target_stocks, current_cash, target_date, volume_surge_mult, session=session
        )
        current_cash += daily_profit
        total_profit += daily_profit

        for log in daily_logs:
            log["date"] = target_date
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


def main():
    initial_cash = 1_000_000
    mults = [round(1.0 + i * 0.1, 2) for i in range(11)]

    print("=" * 70)
    print("US (S&P500) volume-mult sweep 0.1 step (1.0-2.0)")
    print("=" * 70)

    symbols = get_sp500_symbols()
    print(f"sp500 symbols: {len(symbols)}")

    data_15m, intra_data, test_dates, failed_15m, failed_5m = load_sp500_market_data(symbols, chunk_size=80)
    active_symbols = [s for s in symbols if (not data_15m[s].empty and not intra_data[s].empty)]
    print(f"active symbols: {len(active_symbols)}")
    print(f"test dates: {len(test_dates)}")

    rows = []
    result_json = {
        "_meta": {
            "universe_size": len(symbols),
            "active_symbols": len(active_symbols),
            "failed_15m": failed_15m,
            "failed_5m": failed_5m,
            "test_dates": len(test_dates),
        }
    }
    for m in mults:
        tp, fc, logs = run_full_backtest_sp500(
            active_symbols,
            data_15m,
            intra_data,
            test_dates,
            m,
            index_constituent_count=len(symbols),
        )
        ret_pct = (fc - initial_cash) / initial_cash * 100.0
        rows.append((m, tp, fc, len(logs), ret_pct))
        info = summarize_logs(logs)
        info["total_profit"] = tp
        info["final_cash"] = fc
        info["return_pct"] = ret_pct
        result_json[f"{m:.2f}"] = info
        print(f"  x{m:.2f} | 損益 {tp:+,.0f} 円 | 資金 {fc:,.0f} 円 | 約定 {len(logs)} | {ret_pct:+.2f}%")

    print("\n" + "=" * 70)
    print(" US(S&P500)比較サマリー（0.1刻み）")
    print("=" * 70)
    print(f"  {'出来高倍率':>10} | {'合計損益(円)':>14} | {'最終資金(円)':>16} | {'約定数':>8} | {'リターン%':>10}")
    print("  " + "-" * 66)
    for m, tp, fc, n, ret in rows:
        print(f"  {m:>10.2f} | {tp:>+14,.0f} | {fc:>16,.0f} | {n:>8} | {ret:>+9.2f}%")
    best = max(rows, key=lambda x: x[4])
    print("=" * 70)
    print(f"BEST: x{best[0]:.2f} / 損益 {best[1]:+,.0f} 円 / 資金 {best[2]:,.0f} 円 / {best[4]:+.2f}%")
    print("=" * 70)

    with open("us_sp500_volume_sweep_analysis.json", "w", encoding="utf-8") as f:
        json.dump(result_json, f, ensure_ascii=False, indent=2)
    print("saved: us_sp500_volume_sweep_analysis.json")


if __name__ == "__main__":
    main()
