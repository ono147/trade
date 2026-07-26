"""
市場別セッション（寄り引け・TimeLimit）と指数構成比に基づく監視銘柄数を変え、
出来高倍率とあわせてグリッド検証する。
"""
from __future__ import annotations

import argparse
import json
import time

from nikkei225_list import NIKKEI225
from simulation_realistic import (
    DEFAULT_RANK_FRACTION,
    compute_rank_top_n,
    get_session_profile,
    load_market_data,
    run_full_backtest,
)
from us_robustness_validation import DOW30, get_sp500_symbols, load_market_data as load_ohlcv_tz
from us_robustness_validation import run_backtest_for_dates

# 指数の公式構成数（ダウンロード銘柄が少なくても、監視本数の母数として使用）
INDEX_SIZES = {
    "JP": len(NIKKEI225),
    "US_DOW30": 30,
    "US_SP500": 503,
    "UK_FTSE100": 100,
    "DE_DAX40": 40,
    "FR_CAC40": 40,
}

UK_SAMPLE = [
    "SHEL.L", "AZN.L", "HSBA.L", "ULVR.L", "BP.L", "GSK.L", "RIO.L", "BATS.L", "DGE.L", "LLOY.L",
]
DE_SAMPLE = ["SAP.DE", "SIE.DE", "ALV.DE", "BAS.DE", "MBG.DE", "BMW.DE", "DTE.DE", "IFX.DE", "VOW3.DE", "ADS.DE"]
FR_SAMPLE = ["MC.PA", "OR.PA", "TTE.PA", "SAN.PA", "BNP.PA", "AIR.PA", "SU.PA", "EL.PA", "CS.PA", "RI.PA"]


def parse_float_list(s: str) -> list[float]:
    return [float(x.strip()) for x in s.split(",") if x.strip()]


def run_jp(
    data_15m,
    intra_data,
    dates,
    volume_mult: float,
    rank_fraction: float,
    rank_top_n: int | None,
):
    idx_n = INDEX_SIZES["JP"]
    return run_full_backtest(
        data_15m,
        intra_data,
        dates,
        volume_mult,
        print_daily=False,
        session=get_session_profile("JP"),
        rank_fraction=rank_fraction,
        index_constituent_count=idx_n,
        rank_top_n=rank_top_n,
    )


def main():
    ap = argparse.ArgumentParser(description="Session + rank fraction + volume mult grid backtest")
    ap.add_argument(
        "--rank-fractions",
        type=str,
        default="0.25,0.30,0.35",
        help="カンマ区切り（例: 0.25,0.30,0.35）",
    )
    ap.add_argument(
        "--volume-mults",
        type=str,
        default="1.35",
        help="カンマ区切り出来高倍率",
    )
    ap.add_argument(
        "--markets",
        type=str,
        default="JP,US_DOW30,UK_FTSE100,DE_DAX40,FR_CAC40",
        help="カンマ区切り市場キー（US_SP500 は全件取得が重いので必要時のみ指定）",
    )
    ap.add_argument(
        "--skip-sp500",
        action="store_true",
        help="S&P500 全件取得が重い場合にスキップ",
    )
    args = ap.parse_args()

    rank_fractions = parse_float_list(args.rank_fractions)
    volume_mults = parse_float_list(args.volume_mults)
    market_keys = [x.strip() for x in args.markets.split(",") if x.strip()]

    initial_cash = 1_000_000
    t0 = time.time()
    rows = []
    cache_jp = None

    for mk in market_keys:
        if mk == "JP":
            if cache_jp is None:
                print("Loading JP (Nikkei225)...")
                cache_jp = load_market_data()
            data_15m, intra_data, dates = cache_jp
            for vm in volume_mults:
                for rf in rank_fractions:
                    top_n = compute_rank_top_n(INDEX_SIZES["JP"], rf, None)
                    tp, fc, logs = run_jp(data_15m, intra_data, dates, vm, rf, None)
                    ret = (fc - initial_cash) / initial_cash * 100.0
                    rows.append(
                        {
                            "market": mk,
                            "session": "JP",
                            "exchange_tz": "Asia/Tokyo",
                            "index_constituent_count": INDEX_SIZES["JP"],
                            "rank_fraction": rf,
                            "rank_top_n_effective": top_n,
                            "volume_mult": vm,
                            "return_pct": ret,
                            "total_profit": tp,
                            "final_cash": fc,
                            "trades": len(logs),
                            "test_dates": len(dates),
                        }
                    )
                    print(
                        f"[{mk}] rf={rf:.2f} vm={vm:.2f} top_n={top_n} | ret {ret:+.2f}% trades {len(logs)}"
                    )
            continue

        if mk == "US_DOW30":
            syms = list(DOW30)
            ex_tz, sk = "America/New_York", "US"
            idx_n = INDEX_SIZES["US_DOW30"]
        elif mk == "US_SP500":
            if args.skip_sp500:
                print(f"Skip {mk}")
                continue
            syms = get_sp500_symbols()
            ex_tz, sk = "America/New_York", "US"
            idx_n = INDEX_SIZES["US_SP500"]
        elif mk == "UK_FTSE100":
            syms = UK_SAMPLE
            ex_tz, sk = "Europe/London", "UK"
            idx_n = INDEX_SIZES["UK_FTSE100"]
        elif mk == "DE_DAX40":
            syms = DE_SAMPLE
            ex_tz, sk = "Europe/Berlin", "DE"
            idx_n = INDEX_SIZES["DE_DAX40"]
        elif mk == "FR_CAC40":
            syms = FR_SAMPLE
            ex_tz, sk = "Europe/Paris", "FR"
            idx_n = INDEX_SIZES["FR_CAC40"]
        else:
            raise SystemExit(f"unknown market: {mk}")

        print(f"Loading {mk} (n_symbols={len(syms)}, index_constituents={idx_n}, tz={ex_tz})...")
        d15, d5, dates, active, _, _ = load_ohlcv_tz(syms, chunk_size=80, exchange_tz=ex_tz)
        for vm in volume_mults:
            for rf in rank_fractions:
                top_n = compute_rank_top_n(idx_n, rf, None)
                tp, fc, logs = run_backtest_for_dates(
                    active,
                    d15,
                    d5,
                    dates,
                    vm,
                    session_key=sk,
                    rank_fraction=rf,
                    index_constituent_count=idx_n,
                    rank_top_n=None,
                )
                ret = (fc - initial_cash) / initial_cash * 100.0
                rows.append(
                    {
                        "market": mk,
                        "session": sk,
                        "exchange_tz": ex_tz,
                        "symbols_downloaded": len(syms),
                        "symbols_active": len(active),
                        "index_constituent_count": idx_n,
                        "rank_fraction": rf,
                        "rank_top_n_effective": top_n,
                        "volume_mult": vm,
                        "return_pct": ret,
                        "total_profit": tp,
                        "final_cash": fc,
                        "trades": len(logs),
                        "test_dates": len(dates),
                    }
                )
                print(f"[{mk}] rf={rf:.2f} vm={vm:.2f} top_n={top_n} | ret {ret:+.2f}% trades {len(logs)}")

    out = {
        "meta": {
            "default_rank_fraction": DEFAULT_RANK_FRACTION,
            "elapsed_sec": round(time.time() - t0, 2),
            "index_sizes": INDEX_SIZES,
        },
        "rows": rows,
    }
    with open("session_rank_sweep_results.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print("saved: session_rank_sweep_results.json")


if __name__ == "__main__":
    main()
