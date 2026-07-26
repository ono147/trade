"""シミュレーション全期間の勝敗・平均損益・保有時間を集計する。"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import numpy as np

from simulation_realistic import get_session_profile, load_market_data, run_full_backtest

RF, VM = 0.48, 1.38
MAX_SYM = 135
WARMUP = 10
YF = "59d"
OUT = Path("sim_trade_stats.json")


def hold_minutes(entry: str, exit_: str) -> float:
    t1 = datetime.strptime(entry, "%H:%M:%S")
    t2 = datetime.strptime(exit_, "%H:%M:%S")
    return (t2 - t1).total_seconds() / 60.0


def stat_vals(vals: list[float]) -> dict:
    if not vals:
        return {"n": 0}
    arr = np.array(vals, dtype=float)
    return {
        "n": int(len(arr)),
        "mean": float(arr.mean()),
        "median": float(np.median(arr)),
        "sum": float(arr.sum()),
        "min": float(arr.min()),
        "max": float(arr.max()),
    }


def main() -> None:
    data_15m, intra_data, test_dates = load_market_data(period=YF, warmup_skip=WARMUP)
    tp, fc, logs, daily_rows = run_full_backtest(
        data_15m,
        intra_data,
        test_dates,
        VM,
        print_daily=False,
        session=get_session_profile("JP"),
        rank_fraction=RF,
        max_symbols=MAX_SYM,
        return_daily_breakdown=True,
    )

    for t in logs:
        t["hold_min"] = hold_minutes(t["entry_time"], t["exit_time"])

    wins = [t for t in logs if t["pnl"] > 0]
    losses = [t for t in logs if t["pnl"] < 0]
    flat = [t for t in logs if t["pnl"] == 0]

    gross_profit = sum(t["pnl"] for t in wins)
    gross_loss = abs(sum(t["pnl"] for t in losses))

    by_reason: dict = {}
    for reason in ("StopLoss", "DeadCross", "TimeLimit"):
        sub = [t for t in logs if t["reason"] == reason]
        by_reason[reason] = {
            "count": len(sub),
            "wins": sum(1 for t in sub if t["pnl"] > 0),
            "losses": sum(1 for t in sub if t["pnl"] < 0),
            "pnl_sum": float(sum(t["pnl"] for t in sub)),
            "pnl_mean": float(np.mean([t["pnl"] for t in sub])) if sub else 0.0,
            "hold_mean_min": float(np.mean([t["hold_min"] for t in sub])) if sub else 0.0,
        }

    daily_pnls = [d["daily_pnl"] for d in daily_rows]
    pos_days = sum(1 for p in daily_pnls if p > 0)
    neg_days = sum(1 for p in daily_pnls if p < 0)

    result = {
        "params": {
            "rank_fraction": RF,
            "volume_mult": VM,
            "max_symbols": MAX_SYM,
            "yf_period": YF,
        },
        "period": {
            "n_days": len(test_dates),
            "from": test_dates[0],
            "to": test_dates[-1],
        },
        "total_trades": len(logs),
        "total_pnl": float(tp),
        "final_cash": float(fc),
        "wins": stat_vals([t["pnl"] for t in wins]),
        "losses": stat_vals([t["pnl"] for t in losses]),
        "flat_count": len(flat),
        "win_rate_pct": len(wins) / len(logs) * 100 if logs else 0.0,
        "payoff_ratio": abs(stat_vals([t["pnl"] for t in wins])["mean"]
                            / stat_vals([t["pnl"] for t in losses])["mean"])
        if wins and losses else None,
        "profit_factor": gross_profit / gross_loss if gross_loss else None,
        "hold_min_wins": stat_vals([t["hold_min"] for t in wins]),
        "hold_min_losses": stat_vals([t["hold_min"] for t in losses]),
        "by_reason": by_reason,
        "daily": {
            "positive_days": pos_days,
            "negative_days": neg_days,
            "flat_days": len(daily_pnls) - pos_days - neg_days,
            "mean_daily_pnl": float(np.mean(daily_pnls)),
            "median_daily_pnl": float(np.median(daily_pnls)),
        },
        "hypothesis_check": {},
    }

    w_mean = result["wins"].get("mean", 0)
    l_mean = result["losses"].get("mean", 0)
    w_hold = result["hold_min_wins"].get("mean", 0)
    l_hold = result["hold_min_losses"].get("mean", 0)
    result["hypothesis_check"] = {
        "more_losses_than_wins": len(losses) > len(wins),
        "avg_loss_smaller_than_avg_win_abs": abs(l_mean) < abs(w_mean) if wins and losses else None,
        "winners_held_longer": w_hold > l_hold if wins and losses else None,
    }

    OUT.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    print("=" * 60)
    print(f"期間: {test_dates[0]} .. {test_dates[-1]} ({len(test_dates)}営業日)")
    print(f"パラメータ: rf={RF} vm={VM} max_symbols={MAX_SYM}")
    print(f"総損益: {tp:+,.0f}円  取引数: {len(logs)}")
    print("=" * 60)
    print(f"勝ち: {len(wins)}回 ({len(wins)/len(logs)*100:.1f}%)  平均 {w_mean:+,.0f}円  中央値 {result['wins']['median']:+,.0f}円")
    print(f"負け: {len(losses)}回 ({len(losses)/len(logs)*100:.1f}%)  平均 {l_mean:+,.0f}円  中央値 {result['losses']['median']:+,.0f}円")
    print(f"ペイオフ比 (平均勝ち/|平均負け|): {result['payoff_ratio']:.2f}")
    print(f"プロフィットファクター: {result['profit_factor']:.2f}")
    print("-" * 60)
    print(f"勝ちトレード平均保有: {w_hold:.0f}分  負けトレード平均保有: {l_hold:.0f}分")
    print("-" * 60)
    for reason, bd in by_reason.items():
        print(
            f"{reason:10s} {bd['count']:4d}件  "
            f"勝{bd['wins']}/負{bd['losses']}  "
            f"平均{bd['pnl_mean']:+8,.0f}円  "
            f"保有{bd['hold_mean_min']:.0f}分  "
            f"合計{bd['pnl_sum']:+,.0f}円"
        )
    print("-" * 60)
    hc = result["hypothesis_check"]
    print("想定検証:")
    print(f"  負けの方が多い: {'YES' if hc['more_losses_than_wins'] else 'NO'} ({len(losses)} vs {len(wins)})")
    print(f"  平均負け < 平均勝ち(絶対値): {'YES' if hc['avg_loss_smaller_than_avg_win_abs'] else 'NO'}")
    print(f"  勝ちの方が長く保有: {'YES' if hc['winners_held_longer'] else 'NO'} ({w_hold:.0f}分 vs {l_hold:.0f}分)")
    print(f"saved: {OUT}")


if __name__ == "__main__":
    main()
