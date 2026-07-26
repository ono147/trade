import json
import time
import warnings
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


DOW30 = [
    "AAPL", "AMGN", "AXP", "BA", "CAT", "CRM", "CSCO", "CVX", "DIS", "GS",
    "HD", "HON", "IBM", "INTC", "JNJ", "JPM", "KO", "MCD", "MMM", "MRK",
    "MSFT", "NKE", "PG", "TRV", "UNH", "V", "VZ", "WBA", "WMT", "XOM",
]


def get_table_symbols(url: str, symbol_col: str = "Symbol") -> list[str]:
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    table = pd.read_html(StringIO(resp.text))[0]
    symbols = table[symbol_col].astype(str).tolist()
    return [s.replace(".", "-") for s in symbols]


def get_sp500_symbols() -> list[str]:
    return get_table_symbols("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", "Symbol")


def get_sp100_symbols() -> list[str]:
    url = "https://en.wikipedia.org/wiki/S%26P_100"
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    tables = pd.read_html(StringIO(resp.text))
    target = None
    for t in tables:
        cols = [str(c) for c in t.columns]
        if any("Symbol" in c or "Ticker" in c for c in cols):
            target = t
            break
    if target is None:
        raise RuntimeError("Could not find S&P100 table")
    col = "Symbol" if "Symbol" in target.columns else "Ticker symbol"
    symbols = target[col].astype(str).tolist()
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
            print(f"retry {attempt}/{max_retries} interval={download_kwargs.get('interval')} wait={wait_sec:.1f}s")
            time.sleep(wait_sec)
    print(f"final fail interval={download_kwargs.get('interval')} err={last_error}")
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


def load_market_data(symbols: list[str], chunk_size: int = 80, exchange_tz: str = "America/New_York"):
    data_15m, intra_data = {}, {}
    failed_15m, failed_5m = [], []
    recovered_15m, recovered_5m = 0, 0

    print(f"download 15m chunks (n={len(symbols)}, chunk={chunk_size})")
    raw_15m_by_symbol = {}
    for i, batch in enumerate(chunked(symbols, chunk_size), 1):
        print(f"  15m chunk {i}: {len(batch)} symbols")
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

    print(f"download 5m chunks (n={len(symbols)}, chunk={chunk_size})")
    raw_5m_by_symbol = {}
    for i, batch in enumerate(chunked(symbols, chunk_size), 1):
        print(f"  5m chunk {i}: {len(batch)} symbols")
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
            d15.index = d15.index.tz_convert(exchange_tz).tz_localize(None)
        data_15m[sym] = d15

        d5 = raw_5m_by_symbol.get(sym, pd.DataFrame())
        if d5.empty:
            d5 = fetch_single_symbol(sym, "5m")
            if not d5.empty:
                recovered_5m += 1
            else:
                failed_5m.append(sym)
        if not d5.empty and d5.index.tz is not None:
            d5.index = d5.index.tz_convert(exchange_tz).tz_localize(None)
        intra_data[sym] = d5

    all_dates = set()
    for df in data_15m.values():
        if not df.empty:
            all_dates.update(df.index.strftime("%Y-%m-%d"))
    test_dates = sorted(list(all_dates))
    if len(test_dates) > 10:
        test_dates = test_dates[10:]
    else:
        test_dates = []

    active_symbols = [s for s in symbols if (not data_15m[s].empty and not intra_data[s].empty)]
    print(f"active={len(active_symbols)} recovered15m={recovered_15m} recovered5m={recovered_5m}")
    print(f"failed15m={len(failed_15m)} failed5m={len(failed_5m)}")
    print(f"test_dates={len(test_dates)}")
    return data_15m, intra_data, test_dates, active_symbols, failed_15m, failed_5m


def run_backtest_for_dates(
    symbols: list[str],
    data_15m: dict,
    intra_data: dict,
    test_dates: list[str],
    volume_surge_mult: float,
    *,
    session_key: str = "US",
    rank_fraction: float | None = None,
    index_constituent_count: int | None = None,
    rank_top_n: int | None = None,
):
    initial_cash = 1_000_000
    current_cash = initial_cash
    all_logs = []
    total_profit = 0.0
    session = get_session_profile(session_key)
    idx_n = int(index_constituent_count) if index_constituent_count is not None else len(symbols)
    rf = DEFAULT_RANK_FRACTION if rank_fraction is None else float(rank_fraction)
    top_n = compute_rank_top_n(idx_n, rf, rank_top_n)

    for target_date in test_dates:
        symbol_scores = []
        for sym in symbols:
            d_df = data_15m.get(sym, pd.DataFrame())
            if d_df.empty:
                continue
            d_df_prev = d_df[d_df.index.strftime("%Y-%m-%d") < target_date]
            if len(d_df_prev) < 130:
                continue
            score = run_daily_selection(d_df_prev)
            symbol_scores.append((sym, score, sym))

        symbol_scores.sort(key=lambda x: x[1], reverse=True)
        target_stocks = symbol_scores[: min(top_n, len(symbol_scores))]
        daily_profit, daily_logs = run_all_virtual_trades(
            intra_data, target_stocks, current_cash, target_date, volume_surge_mult, session=session
        )
        current_cash += daily_profit
        total_profit += daily_profit
        for log in daily_logs:
            log["date"] = target_date
            all_logs.append(log)

    return total_profit, current_cash, all_logs


def summarize_logs(logs: list[dict]) -> dict:
    pnls = [float(x["pnl"]) for x in logs]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    return {
        "trades": len(logs),
        "win_rate": (len(wins) / (len(wins) + len(losses))) if (len(wins) + len(losses)) > 0 else 0.0,
        "avg_pnl": (sum(pnls) / len(pnls)) if pnls else 0.0,
        "profit_factor": (gross_profit / gross_loss) if gross_loss > 0 else None,
    }


def sweep_on_dataset(name: str, symbols: list[str], data_15m: dict, intra_data: dict, dates: list[str]):
    initial_cash = 1_000_000
    mults = [round(1.0 + i * 0.1, 2) for i in range(11)]
    rows = []
    details = {}

    print(f"\n[{name}] sweep start: symbols={len(symbols)} dates={len(dates)}")
    for m in mults:
        tp, fc, logs = run_backtest_for_dates(symbols, data_15m, intra_data, dates, m)
        ret_pct = (fc - initial_cash) / initial_cash * 100.0
        rows.append((m, tp, fc, len(logs), ret_pct))
        info = summarize_logs(logs)
        info["total_profit"] = tp
        info["final_cash"] = fc
        info["return_pct"] = ret_pct
        details[f"{m:.2f}"] = info
        print(f"  x{m:.2f} | pnl {tp:+,.0f} | ret {ret_pct:+.2f}% | trades {len(logs)}")

    best = max(rows, key=lambda x: x[4]) if rows else None
    return {
        "rows": rows,
        "best": {
            "mult": best[0],
            "total_profit": best[1],
            "final_cash": best[2],
            "trades": best[3],
            "return_pct": best[4],
        }
        if best
        else None,
        "details": details,
    }


def main():
    t0 = time.time()
    result = {"meta": {"started_at": time.strftime("%Y-%m-%d %H:%M:%S")}, "experiments": {}}

    universes = {
        "DOW30": DOW30,
        "SP100": get_sp100_symbols(),
        "SP500": get_sp500_symbols(),
    }

    for uname, raw_symbols in universes.items():
        symbols = sorted(list(dict.fromkeys(raw_symbols)))
        print("\n" + "=" * 70)
        print(f"Universe: {uname} (raw={len(symbols)})")
        print("=" * 70)
        data_15m, intra_data, dates, active_symbols, failed15, failed5 = load_market_data(symbols, chunk_size=80)

        exp_main = sweep_on_dataset(f"{uname}_FULL", active_symbols, data_15m, intra_data, dates)
        result["experiments"][f"{uname}_FULL"] = {
            "universe_size_raw": len(symbols),
            "universe_size_active": len(active_symbols),
            "dates": len(dates),
            "failed_15m": failed15,
            "failed_5m": failed5,
            **exp_main,
        }

        if uname == "SP500" and len(dates) >= 20:
            mid = len(dates) // 2
            dates_first = dates[:mid]
            dates_second = dates[mid:]
            exp_f = sweep_on_dataset("SP500_FIRST_HALF", active_symbols, data_15m, intra_data, dates_first)
            exp_s = sweep_on_dataset("SP500_SECOND_HALF", active_symbols, data_15m, intra_data, dates_second)
            result["experiments"]["SP500_FIRST_HALF"] = {
                "universe_size_active": len(active_symbols),
                "dates": len(dates_first),
                **exp_f,
            }
            result["experiments"]["SP500_SECOND_HALF"] = {
                "universe_size_active": len(active_symbols),
                "dates": len(dates_second),
                **exp_s,
            }

    result["meta"]["elapsed_sec"] = round(time.time() - t0, 2)
    result["meta"]["finished_at"] = time.strftime("%Y-%m-%d %H:%M:%S")

    with open("us_robustness_validation.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print("\n" + "=" * 70)
    print("Best summary")
    print("=" * 70)
    for name, payload in result["experiments"].items():
        best = payload.get("best")
        if best:
            print(
                f"{name:>18} | best x{best['mult']:.2f} | ret {best['return_pct']:+.2f}% | "
                f"pnl {best['total_profit']:+,.0f} | trades {best['trades']}"
            )
    print("=" * 70)
    print("saved: us_robustness_validation.json")


if __name__ == "__main__":
    main()
