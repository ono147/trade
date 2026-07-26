from __future__ import annotations

"""
JP グリッド結果の再スコアリング。
既定係数はプロジェクト標準のスケール不変式に合わせる:
  w_return=250, w_median_pp=900, w_dd_pp=80, w_cvar_pp=200, w_conc=20
"""

import argparse
import csv
import json

INITIAL_CASH = 1_000_000

# 単一の参照先（スケジュール探索・再スコア共通）
ADOPTED_RATIO_WEIGHTS = {
    "w_return": 250.0,
    "w_median_pp": 900.0,
    "w_dd_pp": 80.0,
    "w_cvar_pp": 200.0,
    "w_conc": 20.0,
}


def score_ratio_form(
    return_pct: float,
    median_daily_pnl: float,
    mdd_yen: float,
    cvar_yen: float,
    conc: float,
    *,
    w_return: float,
    w_median_pp: float,
    w_dd_pp: float,
    w_cvar_pp: float,
    w_conc: float,
) -> tuple[float, dict[str, float]]:
    """
    スケール不変評価（線形売買規模・固定レバ時）:
      return_pct は既に％
      其余は INITIAL_CASH で割って「資本に対する割合」を取り ×100 で %-of-initial と揃える
    """
    cap = float(INITIAL_CASH)
    med_pp = median_daily_pnl / cap * 100.0
    dd_pp = mdd_yen / cap * 100.0
    cvar_pp = cvar_yen / cap * 100.0

    contrib_return = w_return * return_pct
    contrib_median = w_median_pp * med_pp
    contrib_mdd = -w_dd_pp * dd_pp
    contrib_cvar = -w_cvar_pp * cvar_pp
    contrib_conc = -w_conc * conc

    score = contrib_return + contrib_median + contrib_mdd + contrib_cvar + contrib_conc

    breakdown = {
        "median_daily_pct_of_initial": med_pp,
        "max_drawdown_pct_of_initial": dd_pp,
        "cvar5_daily_loss_pct_of_initial": cvar_pp,
        "top1_symbol_share": conc,
        "contrib_return": contrib_return,
        "contrib_median_pct": contrib_median,
        "contrib_drawdown_pct": contrib_mdd,
        "contrib_cvar_pct": contrib_cvar,
        "contrib_concentration": contrib_conc,
    }
    return float(score), breakdown


def score_yen_legacy(
    return_pct: float,
    median_daily_pnl: float,
    mdd: float,
    cvar: float,
    conc: float,
    *,
    w_return: float,
    w_median: float,
    lambda_mdd: float,
    lambda_cvar: float,
    lambda_conc: float,
) -> float:
    return (
        w_return * return_pct
        + w_median * median_daily_pnl
        - lambda_mdd * mdd
        - lambda_cvar * cvar
        - lambda_conc * conc * INITIAL_CASH
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Rescore JP sweep (profit-focused; default: scale-invariant)")
    ap.add_argument("--input", default="jp_objective_sweep_results.json")
    ap.add_argument("--output", default="jp_objective_profit_focused_results.json")
    ap.add_argument(
        "--legacy-yen-formula",
        action="store_true",
        help="Use old formula with raw yen terms (not scale-invariant)",
    )
    ap.add_argument(
        "--w-return",
        type=float,
        default=ADOPTED_RATIO_WEIGHTS["w_return"],
        help="[ratio] weight on return_pct",
    )
    ap.add_argument(
        "--w-median-pp",
        type=float,
        default=ADOPTED_RATIO_WEIGHTS["w_median_pp"],
        help="[ratio] weight on median/C*100",
    )
    ap.add_argument(
        "--w-dd-pp",
        type=float,
        default=ADOPTED_RATIO_WEIGHTS["w_dd_pp"],
        help="[ratio] weight on max_dd/C*100",
    )
    ap.add_argument(
        "--w-cvar-pp",
        type=float,
        default=ADOPTED_RATIO_WEIGHTS["w_cvar_pp"],
        help="[ratio] weight on cvar/C*100",
    )
    ap.add_argument(
        "--w-conc",
        type=float,
        default=ADOPTED_RATIO_WEIGHTS["w_conc"],
        help="[ratio] weight on top1 share (0-1)",
    )
    ap.add_argument("--require-positive-return", action="store_true")
    ap.add_argument(
        "--csv-out",
        default=None,
        help="[ratio mode] Write 33-pattern term breakdown CSV to this path",
    )
    ap.add_argument("--w-median", type=float, default=1.0, help="[legacy only] median yen weight")
    ap.add_argument("--lambda-mdd", type=float, default=0.0001, help="[legacy]")
    ap.add_argument("--lambda-cvar", type=float, default=0.15, help="[legacy]")
    ap.add_argument("--lambda-conc", type=float, default=0.05, help="[legacy]")
    args = ap.parse_args()

    with open(args.input, "r", encoding="utf-8") as f:
        src = json.load(f)

    rows = src.get("all_runs", [])
    rescored = []

    for row in rows:
        comp = row["components"]
        perf = row["performance"]
        return_pct = float(perf["return_pct"])
        median_daily_pnl = float(comp["median_daily_pnl"])
        mdd = float(comp["max_drawdown_yen"])
        cvar = float(comp["cvar5_daily_loss_yen"])
        conc = float(comp["top1_symbol_abs_pnl_share"])

        if args.legacy_yen_formula:
            score = score_yen_legacy(
                return_pct,
                median_daily_pnl,
                mdd,
                cvar,
                conc,
                w_return=2500.0,
                w_median=args.w_median,
                lambda_mdd=args.lambda_mdd,
                lambda_cvar=args.lambda_cvar,
                lambda_conc=args.lambda_conc,
            )
            rec = {**row, "profit_focused_score": float(score), "score_mode": "legacy_yen"}
        else:
            score, breakdown = score_ratio_form(
                return_pct,
                median_daily_pnl,
                mdd,
                cvar,
                conc,
                w_return=args.w_return,
                w_median_pp=args.w_median_pp,
                w_dd_pp=args.w_dd_pp,
                w_cvar_pp=args.w_cvar_pp,
                w_conc=args.w_conc,
            )
            rec = {
                **row,
                "profit_focused_score": float(score),
                "score_mode": "ratio_of_initial",
                "score_breakdown": breakdown,
            }
        rescored.append(rec)

    if args.require_positive_return:
        ranked = [r for r in rescored if float(r["performance"]["return_pct"]) > 0.0]
    else:
        ranked = list(rescored)
    ranked.sort(key=lambda x: x["profit_focused_score"], reverse=True)

    if args.legacy_yen_formula:
        formula = (
            "2500*return_pct + w_median*median_daily_pnl - lambda_mdd*max_dd_yen "
            "- lambda_cvar*cvar5_yen - lambda_conc*(top1_share*initial_cash)"
        )
        meta_extra = {
            "w_median": args.w_median,
            "lambda_mdd": args.lambda_mdd,
            "lambda_cvar": args.lambda_cvar,
            "lambda_conc": args.lambda_conc,
        }
    else:
        formula = (
            "w_return*return_pct + w_median_pp*(median/C*100) - w_dd_pp*(max_dd/C*100) "
            "- w_cvar_pp*(cvar5/C*100) - w_conc*top1_share"
        )
        meta_extra = {
            "w_return": args.w_return,
            "w_median_pp": args.w_median_pp,
            "w_dd_pp": args.w_dd_pp,
            "w_cvar_pp": args.w_cvar_pp,
            "w_conc": args.w_conc,
        }

    out = {
        "meta": {
            "source_file": args.input,
            "score_mode": "legacy_yen" if args.legacy_yen_formula else "ratio_of_initial",
            "objective_formula": formula,
            "initial_cash": INITIAL_CASH,
            "note": (
                "ratio_of_initial: all yen paths divided by initial_cash; "
                "linear strategy scaling keeps rankings stable when capital scales."
            ),
            **meta_extra,
            "require_positive_return": bool(args.require_positive_return),
            "n_all_runs": len(rescored),
            "n_ranked_runs": len(ranked),
        },
        "best_by_profit_focused_score": ranked[0] if ranked else None,
        "ranked_by_profit_focused_score": ranked,
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"saved: {args.output}")

    if args.csv_out and not args.legacy_yen_formula:
        rows_csv = []
        for row in rescored:
            bd = row.get("score_breakdown") or {}
            comp = row["components"]
            perf = row["performance"]
            rows_csv.append(
                (
                    float(row["rank_fraction"]),
                    float(row["volume_mult"]),
                    bd.get("median_daily_pct_of_initial", 0.0),
                    bd.get("max_drawdown_pct_of_initial", 0.0),
                    bd.get("cvar5_daily_loss_pct_of_initial", 0.0),
                    bd.get("top1_symbol_share", 0.0),
                    bd.get("contrib_return", 0.0),
                    bd.get("contrib_median_pct", 0.0),
                    bd.get("contrib_drawdown_pct", 0.0),
                    bd.get("contrib_cvar_pct", 0.0),
                    bd.get("contrib_concentration", 0.0),
                    float(row["profit_focused_score"]),
                    float(perf["return_pct"]),
                    int(perf["trades"]),
                )
            )
        rows_csv.sort(key=lambda x: (x[0], x[1]))
        with open(args.csv_out, "w", newline="", encoding="utf-8-sig") as cf:
            w = csv.writer(cf)
            w.writerow(
                [
                    "rank_fraction",
                    "volume_mult",
                    "median_daily_pct_of_initial",
                    "max_drawdown_pct_of_initial",
                    "cvar5_daily_loss_pct_of_initial",
                    "top1_share",
                    "contrib_w_return_x_return_pct",
                    "contrib_w_median_x_median_pp",
                    "contrib_neg_w_dd_x_dd_pp",
                    "contrib_neg_w_cvar_x_cvar_pp",
                    "contrib_neg_w_conc_x_share",
                    "total_score",
                    "return_pct",
                    "trades",
                ]
            )
            w.writerows(rows_csv)
        print(f"saved: {args.csv_out}")
    if ranked:
        best = ranked[0]
        print(
            "best:",
            f"rf={best['rank_fraction']:.2f}",
            f"vm={best['volume_mult']:.2f}",
            f"score={best['profit_focused_score']:.2f}",
            f"ret={best['performance']['return_pct']:+.2f}%",
            f"trades={best['performance']['trades']}",
        )


if __name__ == "__main__":
    main()
