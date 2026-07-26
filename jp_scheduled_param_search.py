"""
週次: rank_fraction∈[0.1,0.5]・volume_mult∈[1.1,1.5] を0.01刻みで全探索。
日次: 前回ベスト中心に各変数 ±0.05 を0.01刻み（範囲内にクリップ）。
評価: 直近 N 営業日（既定50）を load_market_data(..., evaluation_trading_days=N) で使用し、
採用スコアは jp_profit_focused_rescore のスケール不変式（ADOPTED_RATIO_WEIGHTS）。

例:
  python jp_scheduled_param_search.py --mode weekly
  python jp_scheduled_param_search.py --mode daily
  python jp_scheduled_param_search.py --mode auto
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np

from jp_objective_sweep import (
    cvar_daily_loss_yen,
    max_drawdown_yen,
    top1_symbol_concentration,
)
from jp_profit_focused_rescore import ADOPTED_RATIO_WEIGHTS, INITIAL_CASH, score_ratio_form
from nikkei225_list import NIKKEI225
from simulation_realistic import compute_rank_top_n, get_session_profile, load_market_data, run_full_backtest

STATE_VERSION = 1
DEFAULT_STATE_PATH = "jp_param_search_state.json"
DEFAULT_RESULT_PATH = "jp_scheduled_param_search_result.json"
DEFAULT_KABU_CONFIG_PATH = "kabu_config.json"
LIVE_MAX_SYMBOLS = 135
# 週次ベスト・kabu_config と揃えた日次探索の中心（状態ファイルが無い初回用）
DEFAULT_INITIAL_RF = 0.49
DEFAULT_INITIAL_VM = 1.38
DEFAULT_EVAL_TRADING_DAYS = 60


def _frange(a: float, b: float, step: float, nd: int = 2) -> list[float]:
    out = []
    x = a
    while x <= b + 1e-9:
        out.append(round(x, nd))
        x = round(x + step, nd + 2)
        if len(out) > 10000:
            break
    return out


def grid_weekly(rf_bounds: tuple[float, float], vm_bounds: tuple[float, float], step: float) -> list[tuple[float, float]]:
    rfs = _frange(rf_bounds[0], rf_bounds[1], step)
    vms = _frange(vm_bounds[0], vm_bounds[1], step)
    return [(rf, vm) for rf in rfs for vm in vms]


def grid_daily_local(
    center_rf: float,
    center_vm: float,
    half: float,
    step: float,
    rf_bounds: tuple[float, float],
    vm_bounds: tuple[float, float],
) -> list[tuple[float, float]]:
    rfc = round(center_rf, 2)
    vmc = round(center_vm, 2)
    rfs = sorted(
        {
            round(x, 2)
            for x in _frange(rfc - half, rfc + half, step)
            if rf_bounds[0] - 1e-9 <= x <= rf_bounds[1] + 1e-9
        }
    )
    vms = sorted(
        {
            round(x, 2)
            for x in _frange(vmc - half, vmc + half, step)
            if vm_bounds[0] - 1e-9 <= x <= vm_bounds[1] + 1e-9
        }
    )
    pairs = [(rf, vm) for rf in rfs for vm in vms]
    pairs.sort(key=lambda t: (t[0], t[1]))
    return pairs


def adopted_score_one_run(
    daily_pnls: list[float],
    trade_logs: list[dict],
    final_cash: float,
) -> tuple[float, dict]:
    ret_pct = (final_cash - INITIAL_CASH) / INITIAL_CASH * 100.0
    med = float(np.median(daily_pnls)) if daily_pnls else 0.0
    mdd = max_drawdown_yen(daily_pnls)
    cvl = cvar_daily_loss_yen(daily_pnls)
    conc = top1_symbol_concentration(trade_logs)
    return score_ratio_form(
        ret_pct,
        med,
        mdd,
        cvl,
        conc,
        **ADOPTED_RATIO_WEIGHTS,
    )


def load_state(path: Path) -> dict:
    if not path.is_file():
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def save_state(path: Path, data: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def apply_best_to_kabu_config(
    rank_fraction: float,
    volume_mult: float,
    config_path: Path,
) -> None:
    """探索ベストを kabu_config.json の rank_fraction / volume_mult に反映。"""
    if not config_path.is_file():
        raise FileNotFoundError(f"kabu_config が見つかりません: {config_path}")
    with open(config_path, encoding="utf-8") as f:
        cfg = json.load(f)
    prev_rf = cfg.get("rank_fraction")
    prev_vm = cfg.get("volume_mult")
    cfg["rank_fraction"] = round(float(rank_fraction), 2)
    cfg["volume_mult"] = round(float(volume_mult), 2)
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
        f.write("\n")
    print(
        f"kabu_config 更新: rank_fraction {prev_rf} -> {cfg['rank_fraction']}, "
        f"volume_mult {prev_vm} -> {cfg['volume_mult']}",
        flush=True,
    )


def resolve_mode(cli_mode: str, weekly_weekday: int) -> str:
    if cli_mode != "auto":
        return cli_mode
    tz = ZoneInfo("Asia/Tokyo")
    now = datetime.now(tz)
    return "weekly" if now.weekday() == weekly_weekday else "daily"


def main() -> None:
    ap = argparse.ArgumentParser(description="JP scheduled param search (weekly full / daily local)")
    ap.add_argument("--mode", choices=("weekly", "daily", "auto"), default="auto")
    ap.add_argument(
        "--weekly-weekday",
        type=int,
        default=0,
        help="autoモード時、0=月曜で週次フル検索（0-6 が Python weekday）",
    )
    ap.add_argument(
        "--yf-period",
        type=str,
        default="59d",
        help="yfinance period（15m/5mはYahoo側で約60日が上限のため59d推奨。長すぎると再試行が増え遅くなる）",
    )
    ap.add_argument("--warmup-skip", type=int, default=10, help="評価開始までスキップする先頭取引日数")
    ap.add_argument(
        "--eval-trading-days",
        type=int,
        default=DEFAULT_EVAL_TRADING_DAYS,
        help="バックテスト評価に使う直近営業日数（既定60・yfinance上限で実際は約48営業日）",
    )
    ap.add_argument("--rf-min", type=float, default=0.1)
    ap.add_argument("--rf-max", type=float, default=0.5)
    ap.add_argument("--vm-min", type=float, default=1.1)
    ap.add_argument("--vm-max", type=float, default=1.5)
    ap.add_argument("--grid-step", type=float, default=0.01)
    ap.add_argument("--daily-half-width", type=float, default=0.05, help="日次モードでの中心からの幅（各変数）")
    ap.add_argument("--state-file", type=str, default=DEFAULT_STATE_PATH)
    ap.add_argument("--output", type=str, default=DEFAULT_RESULT_PATH)
    ap.add_argument(
        "--initial-rf",
        type=float,
        default=DEFAULT_INITIAL_RF,
        help="状態ファイルなしで日次初回のみ使う rank_fraction の中心",
    )
    ap.add_argument(
        "--initial-vm",
        type=float,
        default=DEFAULT_INITIAL_VM,
        help="同上 volume_mult の中心",
    )
    ap.add_argument(
        "--apply-kabu-config",
        action="store_true",
        help="完了後にベスト値を kabu_config.json の rank_fraction / volume_mult へ書き込む",
    )
    ap.add_argument(
        "--kabu-config",
        type=str,
        default=DEFAULT_KABU_CONFIG_PATH,
        help="--apply-kabu-config 時の設定ファイルパス",
    )
    args = ap.parse_args()

    mode = resolve_mode(args.mode, args.weekly_weekday)
    rf_bounds = (float(args.rf_min), float(args.rf_max))
    vm_bounds = (float(args.vm_min), float(args.vm_max))
    step = float(args.grid_step)

    state_path = Path(args.state_file)
    state = load_state(state_path)

    centers_rf = float(state.get("best_rank_fraction", args.initial_rf))
    centers_vm = float(state.get("best_volume_mult", args.initial_vm))

    if mode == "weekly":
        combos = grid_weekly(rf_bounds, vm_bounds, step)
    else:
        combos = grid_daily_local(
            centers_rf,
            centers_vm,
            half=float(args.daily_half_width),
            step=step,
            rf_bounds=rf_bounds,
            vm_bounds=vm_bounds,
        )

    tz = ZoneInfo("Asia/Tokyo")
    run_started = datetime.now(tz).isoformat()

    t0 = time.time()
    idx_n = len(NIKKEI225)
    session = get_session_profile("JP")

    print(
        f"[{mode}] yf-period={args.yf_period} eval_days={args.eval_trading_days} combos={len(combos)} ...",
        flush=True,
    )
    data_15m, intra_data, test_dates = load_market_data(
        period=args.yf_period,
        warmup_skip=args.warmup_skip,
        evaluation_trading_days=args.eval_trading_days,
    )
    print(f"test_dates(len)={len(test_dates)} range={test_dates[:1]}..{test_dates[-1:]}", flush=True)

    runs_out = []
    best = None

    for i, (rf, vm) in enumerate(combos, 1):
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
            max_symbols=LIVE_MAX_SYMBOLS,
        )
        daily_pnls = [d["daily_pnl"] for d in daily_rows]
        score, bd = adopted_score_one_run(daily_pnls, logs, fc)
        top_n_eff = compute_rank_top_n(idx_n, rf, None)
        perf = {
            "return_pct": (fc - INITIAL_CASH) / INITIAL_CASH * 100.0,
            "final_cash": float(fc),
            "total_profit_yen": float(tp),
            "trades": len(logs),
            "n_days": len(daily_pnls),
        }
        row = {
            "rank_fraction": rf,
            "volume_mult": vm,
            "top_n_effective": top_n_eff,
            "adopted_score": score,
            "score_breakdown_ratio": bd,
            "performance": perf,
        }
        runs_out.append(row)
        if best is None or score > best["adopted_score"]:
            best = row
        if i % max(1, len(combos) // 50) == 0 or i == len(combos):
            print(
                f"  [{i}/{len(combos)}] rf={rf:.2f} vm={vm:.2f} score={score:.2f} ret={perf['return_pct']:+.2f}%",
                flush=True,
            )

    runs_sorted = sorted(runs_out, key=lambda x: x["adopted_score"], reverse=True)
    elapsed = round(time.time() - t0, 2)

    new_state = {
        "version": STATE_VERSION,
        "best_rank_fraction": best["rank_fraction"],
        "best_volume_mult": best["volume_mult"],
        "best_adopted_score": best["adopted_score"],
        "last_run_started": run_started,
        "last_run_mode": mode,
        "yf_period": args.yf_period,
        "warmup_skip": args.warmup_skip,
        "eval_trading_days": args.eval_trading_days,
        "adopted_weights": ADOPTED_RATIO_WEIGHTS,
        "prior_center_rf": centers_rf,
        "prior_center_vm": centers_vm,
    }

    result = {
        "meta": {
            "market": "JP_NIKKEI225",
            "mode_requested": args.mode,
            "mode_used": mode,
            "evaluation_trading_days": args.eval_trading_days,
            "yf_period": args.yf_period,
            "warmup_skip_days": args.warmup_skip,
            "combo_count": len(combos),
            "test_dates": len(test_dates),
            "elapsed_sec": elapsed,
            "adopted_formula": ADOPTED_RATIO_WEIGHTS,
        },
        "best": best,
        "ranked_all": runs_sorted,
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    save_state(state_path, new_state)

    if args.apply_kabu_config and best is not None:
        apply_best_to_kabu_config(
            best["rank_fraction"],
            best["volume_mult"],
            Path(args.kabu_config),
        )

    print(f"best: rf={best['rank_fraction']:.2f} vm={best['volume_mult']:.2f} score={best['adopted_score']:.2f}", flush=True)
    print(f"saved: {args.output}", flush=True)
    print(f"saved: {state_path}", flush=True)


if __name__ == "__main__":
    main()
