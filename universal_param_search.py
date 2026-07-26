"""
普遍的なパラメータ探索:
- markets ごとに「日次の銘柄スコア順（volume_mult非依存）」を事前計算してキャッシュ
- その後、rank_fraction と volume_mult を組み合わせて backtest する

スコープ:
- JP: 決算除外（simulation_realistic.py の get_earnings_tickers を使用）
- 非JP: 決算除外なし（元の海外検証スクリプトと揃える）
"""

from __future__ import annotations

import argparse
import json
import time
from typing import Any

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
from us_robustness_validation import DOW30, load_market_data as load_ohlcv_tz


INDEX_SIZES: dict[str, int] = {
    "JP": len(NIKKEI225),
    "US_DOW30": 30,
    "UK_FTSE100": 100,
    "DE_DAX40": 40,
    "FR_CAC40": 40,
}

SESSION_KEY_BY_MARKET: dict[str, str] = {
    "JP": "JP",
    "US_DOW30": "US",
    "UK_FTSE100": "UK",
    "DE_DAX40": "DE",
    "FR_CAC40": "FR",
}

SYMBOLS: dict[str, list[str]] = {
    "US_DOW30": DOW30,
    "UK_FTSE100": ["SHEL.L", "AZN.L", "HSBA.L", "ULVR.L", "BP.L", "GSK.L", "RIO.L", "BATS.L", "DGE.L", "LLOY.L"],
    "DE_DAX40": ["SAP.DE", "SIE.DE", "ALV.DE", "BAS.DE", "MBG.DE", "BMW.DE", "DTE.DE", "IFX.DE", "VOW3.DE", "ADS.DE"],
    "FR_CAC40": ["MC.PA", "OR.PA", "TTE.PA", "SAN.PA", "BNP.PA", "AIR.PA", "SU.PA", "EL.PA", "CS.PA", "RI.PA"],
}

TZ_BY_MARKET: dict[str, str] = {
    "US_DOW30": "America/New_York",
    "UK_FTSE100": "Europe/London",
    "DE_DAX40": "Europe/Berlin",
    "FR_CAC40": "Europe/Paris",
}


def precompute_ordered_by_date_jp(data_15m: dict[str, pd.DataFrame], test_dates: list[str]):
    earnings_by_date = {d: set(get_earnings_tickers(d)) for d in test_dates}
    ordered_by_date: dict[str, list[tuple[str, float, str]]] = {}
    for target_date in test_dates:
        excluded = earnings_by_date.get(target_date, set())
        scores: list[tuple[str, float, str]] = []
        cutoff_ts = pd.Timestamp(target_date)
        for sym_code, sym_name in NIKKEI225:
            if sym_code in excluded:
                continue
            d_df = data_15m.get(sym_code)
            if d_df is None or d_df.empty:
                continue
            d_df_prev = d_df[d_df.index < cutoff_ts]
            if len(d_df_prev) < 130:
                continue
            score = run_daily_selection(d_df_prev)
            scores.append((sym_code, float(score), sym_name))
        scores.sort(key=lambda x: x[1], reverse=True)
        ordered_by_date[target_date] = scores
    return ordered_by_date


def precompute_ordered_by_date_non_jp(
    symbols: list[str],
    data_15m: dict[str, pd.DataFrame],
    test_dates: list[str],
):
    ordered_by_date: dict[str, list[tuple[str, float, str]]] = {}
    for target_date in test_dates:
        scores: list[tuple[str, float, str]] = []
        cutoff_ts = pd.Timestamp(target_date)
        for sym in symbols:
            d_df = data_15m.get(sym)
            if d_df is None or d_df.empty:
                continue
            d_df_prev = d_df[d_df.index < cutoff_ts]
            if len(d_df_prev) < 130:
                continue
            score = run_daily_selection(d_df_prev)
            scores.append((sym, float(score), sym))
        scores.sort(key=lambda x: x[1], reverse=True)
        ordered_by_date[target_date] = scores
    return ordered_by_date


def run_market_backtest(
    market_key: str,
    intra_data: dict[str, pd.DataFrame],
    test_dates: list[str],
    ordered_by_date: dict[str, list[tuple[str, float, str]]],
    volume_mult: float,
    rank_fraction: float,
) -> dict[str, Any]:
    session_key = SESSION_KEY_BY_MARKET.get(market_key, "JP")
    session = get_session_profile(session_key)
    idx_n = INDEX_SIZES[market_key]
    top_n = compute_rank_top_n(idx_n, rank_fraction, None)

    initial_cash = 1_000_000.0
    cash = initial_cash
    total_profit = 0.0
    trade_count = 0
    for d in test_dates:
        targets = ordered_by_date.get(d, [])[:top_n]
        if not targets:
            continue
        daily_profit, daily_logs = run_all_virtual_trades(
            intra_data,
            targets,
            cash,
            d,
            volume_mult,
            session=session,
        )
        cash += daily_profit
        total_profit += daily_profit
        trade_count += len(daily_logs)

    ret_pct = (cash - initial_cash) / initial_cash * 100.0
    return {
        "return_pct": float(ret_pct),
        "total_profit": float(total_profit),
        "final_cash": float(cash),
        "trades": int(trade_count),
        "top_n_effective": int(top_n),
    }


def parse_float_list(s: str) -> list[float]:
    return [float(x.strip()) for x in s.split(",") if x.strip()]


def main():
    ap = argparse.ArgumentParser(description="universal param search (rank_fraction x volume_mult)")
    ap.add_argument(
        "--markets",
        type=str,
        default="JP,US_DOW30,UK_FTSE100,DE_DAX40,FR_CAC40",
        help="カンマ区切り市場キー",
    )
    ap.add_argument(
        "--rank-fractions",
        type=str,
        default="0.25,0.30,0.35",
        help="カンマ区切り",
    )
    ap.add_argument(
        "--volume-mults",
        type=str,
        default="1.0,1.1,1.2,1.3,1.4,1.5,1.6,1.7,1.8,1.9,2.0",
        help="カンマ区切り",
    )
    ap.add_argument(
        "--top-k",
        type=int,
        default=10,
        help="上位k件を結果に残す",
    )
    args = ap.parse_args()

    markets = [m.strip() for m in args.markets.split(",") if m.strip()]
    rank_fractions = parse_float_list(args.rank_fractions)
    volume_mults = parse_float_list(args.volume_mults)

    # 事前計算キャッシュ
    cache: dict[str, Any] = {}
    t0 = time.time()

    for mk in markets:
        print(f"Precompute: {mk} ...")
        if mk == "JP":
            data_15m, intra_data, test_dates = load_market_data()
            ordered_by_date = precompute_ordered_by_date_jp(data_15m, test_dates)
        else:
            symbols = SYMBOLS[mk]
            exchange_tz = TZ_BY_MARKET[mk]
            data_15m, intra_data, test_dates, active_symbols, failed15, failed5 = load_ohlcv_tz(
                symbols, chunk_size=80, exchange_tz=exchange_tz
            )
            # active_symbols は「データが取れたもの」だけ。以降のスコア順計算はそれに合わせる
            active_symbols = [s for s in active_symbols if s in symbols]
            ordered_by_date = precompute_ordered_by_date_non_jp(active_symbols, data_15m, test_dates)

        cache[mk] = {
            "test_dates": test_dates,
            "intra_data": intra_data,
            "ordered_by_date": ordered_by_date,
            "precompute_test_dates": len(test_dates),
        }

        print(f"Precompute done: {mk} dates={len(test_dates)}")

    candidates: list[dict[str, Any]] = []

    for rf in rank_fractions:
        for vm in volume_mults:
            row: dict[str, Any] = {
                "rank_fraction": rf,
                "volume_mult": vm,
                "markets": {},
            }
            mean_ret = 0.0
            min_ret = None
            for mk in markets:
                bt = run_market_backtest(
                    mk,
                    cache[mk]["intra_data"],
                    cache[mk]["test_dates"],
                    cache[mk]["ordered_by_date"],
                    vm,
                    rf,
                )
                row["markets"][mk] = bt
                mean_ret += bt["return_pct"]
                min_ret = bt["return_pct"] if min_ret is None else min(min_ret, bt["return_pct"])

            mean_ret /= len(markets)
            row["mean_return_pct"] = float(mean_ret)
            row["min_return_pct"] = float(min_ret if min_ret is not None else 0.0)

            # objective: mean を最大化、かつ min の致命傷を抑える
            #（min はマイナスになりやすいので、重みは小さく。）
            row["objective"] = float(mean_ret - 0.2 * abs(row["min_return_pct"]))

            candidates.append(row)
            print(
                f"Cand rf={rf:.2f} vm={vm:.2f} mean={row['mean_return_pct']:+.2f}% min={row['min_return_pct']:+.2f}% obj={row['objective']:+.2f}"
            )

    candidates.sort(key=lambda x: x["objective"], reverse=True)
    top = candidates[: max(1, int(args.top_k))]

    out = {
        "meta": {
            "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "elapsed_sec": round(time.time() - t0, 2),
            "markets": markets,
            "rank_fractions": rank_fractions,
            "volume_mults": volume_mults,
        },
        "top_candidates": top,
        "all_candidates_count": len(candidates),
    }

    with open("universal_param_search_results.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print("saved: universal_param_search_results.json")


if __name__ == "__main__":
    main()

