import csv
import json
from pathlib import Path

from jp_profit_focused_rescore import score_ratio_form

PROFILES = {
    "adopted": {"w_return": 250.0, "w_median_pp": 900.0, "w_dd_pp": 80.0, "w_cvar_pp": 200.0, "w_conc": 20.0},
    "profit_max": {"w_return": 320.0, "w_median_pp": 500.0, "w_dd_pp": 70.0, "w_cvar_pp": 70.0, "w_conc": 20.0},
    "balanced_legacy": {"w_return": 250.0, "w_median_pp": 800.0, "w_dd_pp": 100.0, "w_cvar_pp": 100.0, "w_conc": 40.0},
    "defensive": {"w_return": 180.0, "w_median_pp": 900.0, "w_dd_pp": 170.0, "w_cvar_pp": 180.0, "w_conc": 80.0},
}

PROFILE_ORDER = ["adopted", "profit_max", "balanced_legacy", "defensive"]


def score_row(row: dict, weights: dict) -> float:
    comp = row["components"]
    perf = row["performance"]
    score, _ = score_ratio_form(
        float(perf["return_pct"]),
        float(comp["median_daily_pnl"]),
        float(comp["max_drawdown_yen"]),
        float(comp["cvar5_daily_loss_yen"]),
        float(comp["top1_symbol_abs_pnl_share"]),
        w_return=weights["w_return"],
        w_median_pp=weights["w_median_pp"],
        w_dd_pp=weights["w_dd_pp"],
        w_cvar_pp=weights["w_cvar_pp"],
        w_conc=weights["w_conc"],
    )
    return float(score)


def pattern_label(row: dict) -> str:
    return "rf={:.2f}, vm={:.2f}".format(float(row["rank_fraction"]), float(row["volume_mult"]))


def main() -> None:
    src = json.load(open("jp_objective_sweep_results.json", encoding="utf-8"))
    runs = src["all_runs"]

    profile_rankings: dict[str, list[dict]] = {}
    for name, w in PROFILES.items():
        scored = []
        for r in runs:
            s = score_row(r, w)
            scored.append(
                {
                    "pattern": pattern_label(r),
                    "rank_fraction": float(r["rank_fraction"]),
                    "volume_mult": float(r["volume_mult"]),
                    "score": s,
                    "return_pct": float(r["performance"]["return_pct"]),
                    "trades": int(r["performance"]["trades"]),
                }
            )
        scored.sort(key=lambda x: x["score"], reverse=True)
        for i, item in enumerate(scored, 1):
            item["rank"] = i
        profile_rankings[name] = scored

    all_patterns = sorted(
        {x["pattern"] for xs in profile_rankings.values() for x in xs},
        key=lambda s: (float(s.split(",")[0].split("=")[1]), float(s.split(",")[1].split("=")[1])),
    )
    rank_by_profile = {p: {x["pattern"]: x["rank"] for x in xs} for p, xs in profile_rankings.items()}
    score_by_profile = {p: {x["pattern"]: x["score"] for x in xs} for p, xs in profile_rankings.items()}
    base_info = {x["pattern"]: x for x in profile_rankings["adopted"]}

    csv_path = Path("jp_weight_profile_comparison.csv")
    with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
        wcsv = csv.writer(f)
        header = ["pattern", "return_pct", "trades"]
        header += [f"rank_{k}" for k in PROFILE_ORDER]
        header += [f"score_{k}" for k in PROFILE_ORDER]
        wcsv.writerow(header)
        for p in all_patterns:
            info = base_info[p]
            row_out = [
                p,
                info["return_pct"],
                info["trades"],
                *[rank_by_profile[k][p] for k in PROFILE_ORDER],
                *[score_by_profile[k][p] for k in PROFILE_ORDER],
            ]
            wcsv.writerow(row_out)

    top_rows = []
    for profile in PROFILE_ORDER:
        for item in profile_rankings[profile][:10]:
            top_rows.append(
                [
                    profile,
                    str(item["rank"]),
                    item["pattern"],
                    "{:+.2f}%".format(item["return_pct"]),
                    str(item["trades"]),
                    "{:.2f}".format(item["score"]),
                ]
            )

    full_headers = ["pattern", "return_pct", "trades"] + [
        x for pk in PROFILE_ORDER for x in (f"rank_{pk}", f"score_{pk}")
    ]
    full_rows = []
    for p in all_patterns:
        info = base_info[p]
        flat = []
        for pk in PROFILE_ORDER:
            flat.append(str(rank_by_profile[pk][p]))
            flat.append("{:.1f}".format(score_by_profile[pk][p]))
        full_rows.append(
            [
                p,
                "{:+.2f}%".format(info["return_pct"]),
                str(info["trades"]),
                *flat,
            ]
        )

    profile_notes = "; ".join(
        f'{k}=({PROFILES[k]["w_return"]},{PROFILES[k]["w_median_pp"]},{PROFILES[k]["w_dd_pp"]},'
        f'{PROFILES[k]["w_cvar_pp"]},{PROFILES[k]["w_conc"]})'
        for k in PROFILE_ORDER
    )

    canvas_content = f"""import {{ Divider, H1, H2, Stack, Table, Text }} from 'cursor/canvas';

export default function WeightProfileComparison() {{
  const topHeaders = ["profile", "rank", "pattern", "return_pct", "trades", "score"];
  const topRows = {json.dumps(top_rows, ensure_ascii=False)};
  const fullHeaders = {json.dumps(full_headers, ensure_ascii=False)};
  const fullRows = {json.dumps(full_rows, ensure_ascii=False)};

  return (
    <Stack gap={{16}}>
      <H1>33 Cases: Coefficient Profile Comparison</H1>
      <Text>Same 33 cases, re-ranked by weight profiles (scale-invariant formula).</Text>
      <Text tone="secondary" size="small">{profile_notes}</Text>
      <Divider />
      <H2>Top 10 per profile</H2>
      <Table headers={{topHeaders}} rows={{topRows}} />
      <Divider />
      <H2>All 33 patterns (rank & score)</H2>
      <Table headers={{fullHeaders}} rows={{fullRows}} />
    </Stack>
  );
}}
"""

    canvas_path = Path(
        r"C:\Users\phabc\.cursor\projects\c-Users-phabc-OneDrive-trade-main\canvases\jp-weight-profile-comparison.canvas.tsx"
    )
    canvas_path.write_text(canvas_content, encoding="utf-8")

    out_json = {
        "profiles": PROFILES,
        "rankings": profile_rankings,
    }
    with open("jp_weight_profile_comparison.json", "w", encoding="utf-8") as f:
        json.dump(out_json, f, ensure_ascii=False, indent=2)

    print("saved: jp_weight_profile_comparison.csv")
    print("saved: jp_weight_profile_comparison.json")
    print(f"saved: {canvas_path}")


if __name__ == "__main__":
    main()
