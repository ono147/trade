"""
週次フルグリッド（最大1681組）の所要時間見積り。
データ取得は1回、続けて N 組のバックテストを実行し平均秒/組から extrapolate。
"""
from __future__ import annotations

import argparse
import statistics
import time

from jp_scheduled_param_search import grid_weekly
from nikkei225_list import NIKKEI225
from simulation_realistic import get_session_profile, load_market_data, run_full_backtest


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--yf-period",
        type=str,
        default="59d",
        help="15m/5m は Yahoo が約60日上限のため 59d 推奨（90d は失敗・再試行で遅くなりやすい）",
    )
    ap.add_argument("--warmup-skip", type=int, default=10)
    ap.add_argument("--eval-trading-days", type=int, default=50)
    ap.add_argument("--sample", type=int, default=15, help="計測する組み合わせ数（先頭から）")
    ap.add_argument("--rf-min", type=float, default=0.1)
    ap.add_argument("--rf-max", type=float, default=0.5)
    ap.add_argument("--vm-min", type=float, default=1.1)
    ap.add_argument("--vm-max", type=float, default=1.5)
    ap.add_argument("--grid-step", type=float, default=0.01)
    args = ap.parse_args()

    combos = grid_weekly((args.rf_min, args.rf_max), (args.vm_min, args.vm_max), args.grid_step)
    total_combos = len(combos)
    n = min(max(1, args.sample), total_combos)
    sample = combos[:n]

    t0 = time.perf_counter()
    data_15m, intra_data, test_dates = load_market_data(
        period=args.yf_period,
        warmup_skip=args.warmup_skip,
        evaluation_trading_days=args.eval_trading_days,
    )
    t_load = time.perf_counter() - t0

    idx_n = len(NIKKEI225)
    session = get_session_profile("JP")

    times = []
    for i, (rf, vm) in enumerate(sample, 1):
        t_run = time.perf_counter()
        run_full_backtest(
            data_15m,
            intra_data,
            test_dates,
            vm,
            print_daily=False,
            session=session,
            rank_fraction=rf,
            index_constituent_count=idx_n,
            rank_top_n=None,
            return_daily_breakdown=False,
        )
        elapsed = time.perf_counter() - t_run
        times.append(elapsed)
        print(f"  [{i}/{n}] rf={rf:.2f} vm={vm:.2f} -> {elapsed:.2f}s", flush=True)

    mean_bt = statistics.fmean(times)
    extrap_bt = total_combos * mean_bt
    extrap_total = t_load + extrap_bt

    print()
    print(f"weekly_grid_total_combos={total_combos}")
    print(f"test_trading_days={len(test_dates)}")
    print(f"data_load_sec={t_load:.2f}")
    print(f"backtest_sample_size={n}")
    print(f"backtest_mean_sec={mean_bt:.3f}")
    print(f"backtest_min_sec={min(times):.3f} max_sec={max(times):.3f}")
    if n > 1:
        print(f"backtest_stdev_sec={statistics.stdev(times):.3f}")
    print()
    print(f"ETA_backtest_only_sec={extrap_bt:.0f}  (~{extrap_bt/60:.1f} min ~= {extrap_bt/3600:.2f} h)")
    print(f"ETA_with_load_sec={extrap_total:.0f}  (~{extrap_total/3600:.2f} h)")


if __name__ == "__main__":
    main()
