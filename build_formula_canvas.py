import csv
import json
from pathlib import Path

from jp_profit_focused_rescore import ADOPTED_RATIO_WEIGHTS, INITIAL_CASH, score_ratio_form

W_RETURN = ADOPTED_RATIO_WEIGHTS["w_return"]
W_MEDIAN_PP = ADOPTED_RATIO_WEIGHTS["w_median_pp"]
W_DD_PP = ADOPTED_RATIO_WEIGHTS["w_dd_pp"]
W_CVAR_PP = ADOPTED_RATIO_WEIGHTS["w_cvar_pp"]
W_CONC = ADOPTED_RATIO_WEIGHTS["w_conc"]


def main() -> None:
    src = json.load(open("jp_objective_sweep_results.json", encoding="utf-8"))
    rows = src["all_runs"]

    out_rows = []
    csv_path = Path("jp_formula_validity_33patterns.csv")
    legacy_csv = Path("jp_formula_validity_33patterns_yen_legacy.csv")

    legacy_table = []
    ratio_table = []

    for r in rows:
        c = r["components"]
        p = r["performance"]
        return_pct = float(p["return_pct"])
        median = float(c["median_daily_pnl"])
        mdd = float(c["max_drawdown_yen"])
        cvar = float(c["cvar5_daily_loss_yen"])
        conc = float(c["top1_symbol_abs_pnl_share"])

        score, bd = score_ratio_form(
            return_pct,
            median,
            mdd,
            cvar,
            conc,
            w_return=W_RETURN,
            w_median_pp=W_MEDIAN_PP,
            w_dd_pp=W_DD_PP,
            w_cvar_pp=W_CVAR_PP,
            w_conc=W_CONC,
        )

        out_rows.append(
            {
                "rank_fraction": float(r["rank_fraction"]),
                "volume_mult": float(r["volume_mult"]),
                "pattern": "rf={:.2f}, vm={:.2f}".format(r["rank_fraction"], r["volume_mult"]),
                **bd,
                "total_score": score,
                "return_pct": return_pct,
                "trades": int(p["trades"]),
            }
        )

        # Yen legacy breakdown (for comparison)
        term_return = 2500.0 * return_pct
        term_median = 1.0 * median
        term_mdd = -0.0001 * mdd
        term_cvar = -0.15 * cvar
        term_conc = -0.05 * conc * INITIAL_CASH
        legacy_total = term_return + term_median + term_mdd + term_cvar + term_conc
        legacy_table.append(
            (
                float(r["rank_fraction"]),
                float(r["volume_mult"]),
                term_return,
                term_median,
                term_mdd,
                term_cvar,
                term_conc,
                legacy_total,
                return_pct,
                int(p["trades"]),
            )
        )

        ratio_table.append(
            (
                float(r["rank_fraction"]),
                float(r["volume_mult"]),
                bd["contrib_return"],
                bd["contrib_median_pct"],
                bd["contrib_drawdown_pct"],
                bd["contrib_cvar_pct"],
                bd["contrib_concentration"],
                score,
                return_pct,
                int(p["trades"]),
                bd["median_daily_pct_of_initial"],
                bd["max_drawdown_pct_of_initial"],
                bd["cvar5_daily_loss_pct_of_initial"],
                bd["top1_symbol_share"],
            )
        )

    out_rows.sort(key=lambda x: (x["rank_fraction"], x["volume_mult"]))

    with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
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
        for row in sorted(ratio_table, key=lambda x: (x[0], x[1])):
            (
                rf,
                vm,
                cr,
                cm,
                cd,
                cc,
                cn,
                total,
                rp,
                tr,
                med_pp,
                dd_pp,
                cvar_pp,
                sh,
            ) = row
            w.writerow(
                [
                    rf,
                    vm,
                    med_pp,
                    dd_pp,
                    cvar_pp,
                    sh,
                    cr,
                    cm,
                    cd,
                    cc,
                    cn,
                    total,
                    rp,
                    tr,
                ]
            )

    with legacy_csv.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "rank_fraction",
                "volume_mult",
                "legacy_T1_2500xreturn_pct",
                "legacy_T2_median_yen",
                "legacy_T3_neg0.0001xmax_dd_yen",
                "legacy_T4_neg0.15xcvar5_yen",
                "legacy_concentration_yen_scale",
                "legacy_total_score",
                "return_pct",
                "trades",
            ]
        )
        for row in sorted(legacy_table, key=lambda x: (x[0], x[1])):
            w.writerow(row)

    table_rows = []
    for x in out_rows:
        table_rows.append(
            [
                x["pattern"],
                "{:.2f}".format(x["contrib_return"]),
                "{:.2f}".format(x["contrib_median_pct"]),
                "{:.2f}".format(x["contrib_drawdown_pct"]),
                "{:.2f}".format(x["contrib_cvar_pct"]),
                "{:.2f}".format(x["contrib_concentration"]),
                "{:.2f}".format(x["total_score"]),
                "{:+.2f}%".format(x["return_pct"]),
                str(x["trades"]),
                "{:.4f}".format(x["median_daily_pct_of_initial"]),
                "{:.4f}".format(x["max_drawdown_pct_of_initial"]),
            ]
        )

    headers = [
        "pattern",
        "contrib: w_r*ret%",
        "contrib: w_m*median%/init",
        "contrib: -w_dd*DD%/init",
        "contrib: -w_cv*CVaR%/init",
        "contrib: -w_c*top1share",
        "total_score",
        "return_pct",
        "trades",
        "median%/init",
        "max_DD%/init",
    ]

    headers_js = json.dumps(headers, ensure_ascii=False)
    rows_js = json.dumps(table_rows, ensure_ascii=False)

    formula_note = (
        f"Scale-invariant: total = {W_RETURN}*return_pct + {W_MEDIAN_PP}*(median/C*100) "
        f"- {W_DD_PP}*(maxDD/C*100) - {W_CVAR_PP}*(CVaR5/C*100) - {W_CONC}*top1_share"
    )

    content = f"""import {{ Divider, H1, H2, Stack, Table, Text }} from 'cursor/canvas';

export default function JPFormulaValidityBreakdown() {{
  const headers = {headers_js};
  const rows = {rows_js};

  return (
    <Stack gap={{16}}>
      <H1>33 Patterns: Scale-Invariant Formula</H1>
      <Text>C = initial cash (yen). All yen moments divided by C; multiplied by 100 for %-of-initial.</Text>
      <Text tone="secondary" size="small">
        {formula_note}
      </Text>
      <Divider />
      <H2>All 33 patterns</H2>
      <Table headers={{headers}} rows={{rows}} />
    </Stack>
  );
}}
"""

    canvas_path = Path(
        r"C:\Users\phabc\.cursor\projects\c-Users-phabc-OneDrive-trade-main\canvases\jp-formula-validity-33-patterns.canvas.tsx"
    )
    canvas_path.write_text(content, encoding="utf-8")
    print(f"saved: {canvas_path}")
    print(f"saved: {csv_path}")
    print(f"saved: {legacy_csv}")


if __name__ == "__main__":
    main()
