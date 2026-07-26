"""
日本株（日経225）のみ: rank_fraction × volume_mult のグリッドでバックテストし、
推奨目的関数スコアと実績指標を JSON に保存する。

目的関数（デフォルト）:
  score = median_daily_pnl
          - λ_mdd * max_drawdown_yen
          - λ_cvar * |CVaR5_daily|   （最悪5%日の平均損失の大きさ）
          - λ_conc * (top1_symbol_abs_pnl_share * 1_000_000)

※ λ は円・比率のスケールを揃えるための重み（CLI で変更可）
"""
from __future__ import annotations

import argparse
import json
import time
from collections import defaultdict

import numpy as np

from nikkei225_list import NIKKEI225
from simulation_realistic import (
    compute_rank_top_n,
    get_session_profile,
    load_market_data,
    run_full_backtest,
)


INITIAL_CASH = 1_000_000


def max_drawdown_yen(daily_pnls: list[float], initial: float = INITIAL_CASH) -> float:
    """累積資産曲線から最大ドローダウン（円）。"""
    eq = float(initial)
    peak = eq
    max_dd = 0.0
    for pnl in daily_pnls:
        eq += pnl
        peak = max(peak, eq)
        max_dd = max(max_dd, peak - eq)
    return float(max_dd)


def cvar_daily_loss_yen(daily_pnls: list[float], tail: float = 0.05) -> float:
    """
    日次損益の下側 tail 平均の絶対値（損失が大きいほど大きい正の数）。
    日数が少ない場合は最悪1日分を使う。
    """
    if not daily_pnls:
        return 0.0
    arr = np.array(daily_pnls, dtype=float)
    k = max(1, int(np.ceil(len(arr) * tail)))
    worst = np.sort(arr)[:k]
    mean_worst = float(np.mean(worst))
    if mean_worst >= 0:
        return 0.0
    return float(-mean_worst)


def top1_symbol_concentration(trade_logs: list[dict]) -> float:
    """銘柄別 |損益| のうち最大銘柄が占める割合 [0,1]。"""
    by_sym: dict[str, float] = defaultdict(float)
    for log in trade_logs:
        by_sym[str(log["symbol"])] += float(log["pnl"])
    if not by_sym:
        return 0.0
    abs_sum = sum(abs(v) for v in by_sym.values())
    if abs_sum <= 0:
        return 0.0
    mx = max(abs(v) for v in by_sym.values())
    return float(mx / abs_sum)


def compute_objective_and_perf(
    daily_pnls: list[float],
    trade_logs: list[dict],
    final_cash: float,
    total_profit: float,
    *,
    lambda_mdd: float,
    lambda_cvar: float,
    lambda_conc: float,
) -> dict:
    med = float(np.median(daily_pnls)) if daily_pnls else 0.0
    mdd_yen = max_drawdown_yen(daily_pnls)
    cvar_loss = cvar_daily_loss_yen(daily_pnls)
    conc = top1_symbol_concentration(trade_logs)

    score = (
        med
        - lambda_mdd * mdd_yen
        - lambda_cvar * cvar_loss
        - lambda_conc * conc * INITIAL_CASH
    )

    wins = [p for p in daily_pnls if p > 0]
    losses = [p for p in daily_pnls if p < 0]
    ret_pct = (final_cash - INITIAL_CASH) / INITIAL_CASH * 100.0

    return {
        "objective_score": float(score),
        "components": {
            "median_daily_pnl": med,
            "max_drawdown_yen": mdd_yen,
            "cvar5_daily_loss_yen": cvar_loss,
            "top1_symbol_abs_pnl_share": conc,
        },
        "performance": {
            "total_profit_yen": float(total_profit),
            "final_cash": float(final_cash),
            "return_pct": float(ret_pct),
            "trades": len(trade_logs),
            "n_days": len(daily_pnls),
            "daily_win_rate": float(len(wins) / len(daily_pnls)) if daily_pnls else 0.0,
            "mean_daily_pnl": float(np.mean(daily_pnls)) if daily_pnls else 0.0,
            "std_daily_pnl": float(np.std(daily_pnls)) if len(daily_pnls) > 1 else 0.0,
            "min_daily_pnl": float(np.min(daily_pnls)) if daily_pnls else 0.0,
            "max_daily_pnl": float(np.max(daily_pnls)) if daily_pnls else 0.0,
        },
        "weights": {
            "lambda_mdd": lambda_mdd,
            "lambda_cvar": lambda_cvar,
            "lambda_conc": lambda_conc,
        },
    }


def parse_float_list(s: str) -> list[float]:
    return [float(x.strip()) for x in s.split(",") if x.strip()]


def main():
    ap = argparse.ArgumentParser(description="JP objective sweep (rank_fraction x volume_mult)")
    ap.add_argument("--rank-fractions", type=str, default="0.25,0.30,0.35")
    ap.add_argument(
        "--volume-mults",
        type=str,
        default="1.0,1.1,1.2,1.3,1.4,1.5,1.6,1.7,1.8,1.9,2.0",
    )
    ap.add_argument("--lambda-mdd", type=float, default=0.0003, help="MDD(円)へのペナルティ係数")
    ap.add_argument("--lambda-cvar", type=float, default=0.5, help="CVaR5(円)へのペナルティ係数")
    ap.add_argument("--lambda-conc", type=float, default=0.15, help="集中度×初期資金スケールへの係数")
    ap.add_argument("--index-size", type=int, default=None, help="既定: len(NIKKEI225)")
    args = ap.parse_args()

    idx_n = int(args.index_size) if args.index_size is not None else len(NIKKEI225)
    rank_fractions = parse_float_list(args.rank_fractions)
    volume_mults = parse_float_list(args.volume_mults)
    session = get_session_profile("JP")

    t0 = time.time()
    print("データ取得中 (60d)...", flush=True)
    data_15m, intra_data, test_dates = load_market_data()
    print(f"test_dates={len(test_dates)}", flush=True)

    rows = []
    for rf in rank_fractions:
        for vm in volume_mults:
            tp, fc, logs, daily_rows = run_full_backtest(
                data_15m,
                intra_data,
                test_dates,
                vm,
                print_daily=False,
                session=session,
                rank_fraction=rf,
                index_constituent_count=idx_n,
                rank_top_n=None,
                return_daily_breakdown=True,
            )
            daily_pnls = [d["daily_pnl"] for d in daily_rows]
            top_n = compute_rank_top_n(idx_n, rf, None)
            metrics = compute_objective_and_perf(
                daily_pnls,
                logs,
                fc,
                tp,
                lambda_mdd=args.lambda_mdd,
                lambda_cvar=args.lambda_cvar,
                lambda_conc=args.lambda_conc,
            )
            row = {
                "rank_fraction": rf,
                "volume_mult": vm,
                "top_n_effective": top_n,
                **metrics,
            }
            rows.append(row)
            print(
                f"rf={rf:.2f} vm={vm:.2f} | score={metrics['objective_score']:,.0f} | "
                f"ret={metrics['performance']['return_pct']:+.2f}% | trades={metrics['performance']['trades']}",
                flush=True,
            )

    rows_sorted = sorted(rows, key=lambda x: x["objective_score"], reverse=True)

    out = {
        "meta": {
            "market": "JP_NIKKEI225",
            "index_constituent_count": idx_n,
            "test_dates": len(test_dates),
            "elapsed_sec": round(time.time() - t0, 2),
            "objective_formula": (
                "median_daily_pnl - lambda_mdd*max_dd_yen - lambda_cvar*cvar5_daily_loss_yen "
                "- lambda_conc*(top1_symbol_abs_share * initial_cash)"
            ),
            "lambda_mdd": args.lambda_mdd,
            "lambda_cvar": args.lambda_cvar,
            "lambda_conc": args.lambda_conc,
            "initial_cash": INITIAL_CASH,
        },
        "best_by_objective_score": rows_sorted[0] if rows_sorted else None,
        "all_runs": rows,
        "ranked_by_score": rows_sorted,
    }

    out_path = "jp_objective_sweep_results.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"saved: {out_path}", flush=True)


if __name__ == "__main__":
    main()
