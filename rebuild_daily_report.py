"""既存 trades_*.jsonl を約定照会して summary を再生成する。"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict, deque
from pathlib import Path

from kabu_api import KabuAPI, KabuApiConfig

LOG_DIR = Path("kabu_logs")


def load_config(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_trades(date: str) -> list[dict]:
    path = LOG_DIR / f"trades_{date}.jsonl"
    if not path.exists():
        raise FileNotFoundError(path)
    trades: list[dict] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                trades.append(json.loads(line))
    return trades


def backfill_sell_order_links(trades: list[dict], orders: list[dict], date: str) -> None:
    """SELL に buy_order_id / order_id が無い旧ログを補完する。"""
    date_key = date.replace("-", "")

    open_buys: dict[str, deque] = defaultdict(deque)
    for t in trades:
        if t.get("action") == "BUY" and t.get("order_id"):
            open_buys[t["symbol"]].append(str(t["order_id"]))

    sell_orders_by_sym: dict[str, list[dict]] = defaultdict(list)
    for o in orders:
        if str(o.get("Side")) != "1":
            continue
        recv = str(o.get("RecvTime", ""))
        exp = str(o.get("ExpireDay", ""))
        if date_key not in recv and not str(exp).startswith(date_key):
            continue
        sym = f"{o.get('Symbol')}.T"
        sell_orders_by_sym[sym].append(o)

    for sym in sell_orders_by_sym:
        sell_orders_by_sym[sym].sort(key=lambda x: str(x.get("RecvTime", "")))

    used_sell_ids: set[str] = set()
    for t in trades:
        if t.get("action") != "SELL":
            continue
        sym = t["symbol"]
        if not t.get("buy_order_id") and open_buys[sym]:
            t["buy_order_id"] = open_buys[sym].popleft()
        if t.get("order_id"):
            continue
        qty = float(t.get("qty", 0) or 0)
        for o in sell_orders_by_sym.get(sym, []):
            oid = str(o.get("ID", ""))
            if oid in used_sell_ids:
                continue
            if float(o.get("OrderQty", 0) or 0) != qty:
                continue
            t["order_id"] = oid
            used_sell_ids.add(oid)
            break


def extract_exec_price_from_order(order: dict) -> tuple[float | None, float, float]:
    details = order.get("Details") or []
    exec_qty = 0.0
    exec_value = 0.0
    commission_total = 0.0
    for d in details:
        if int(d.get("RecType", -1)) != 8:
            continue
        try:
            px = float(d.get("Price", 0) or 0)
            qty = float(d.get("Qty", 0) or 0)
        except (TypeError, ValueError):
            continue
        if px <= 0 or qty <= 0:
            continue
        exec_qty += qty
        exec_value += px * qty
        commission_total += float(d.get("Commission", 0) or 0) + float(
            d.get("CommissionTax", 0) or 0
        )
    if exec_qty <= 0:
        return None, 0.0, commission_total
    return exec_value / exec_qty, exec_qty, commission_total


def enrich_trades_with_executions(trades: list[dict], api: KabuAPI) -> dict[str, float]:
    """実約定単価で entry/exit/pnl を更新。旧 pnl の合計も返す。"""
    old_pnl = sum(float(t.get("pnl", 0) or 0) for t in trades if t.get("action") == "SELL")

    needed: set[str] = set()
    for t in trades:
        if t.get("action") == "BUY" and t.get("order_id"):
            needed.add(str(t["order_id"]))
        if t.get("action") == "SELL":
            if t.get("order_id"):
                needed.add(str(t["order_id"]))
            if t.get("buy_order_id"):
                needed.add(str(t["buy_order_id"]))

    orders = api.get_orders(product="1")
    by_id = {str(o.get("ID", "")): o for o in orders}

    exec_map: dict[str, dict[str, float]] = {}
    for oid in needed:
        order = by_id.get(oid)
        if not order:
            continue
        px, qty, com = extract_exec_price_from_order(order)
        if px is None:
            continue
        exec_map[oid] = {"price": float(px), "qty": float(qty), "commission": float(com)}

    buy_comm: dict[str, float] = {}
    for t in trades:
        if t.get("action") != "BUY":
            continue
        oid = t.get("order_id")
        if not oid or str(oid) not in exec_map:
            continue
        t["price"] = exec_map[str(oid)]["price"]
        t["price_source"] = "execution"
        buy_comm[str(oid)] = exec_map[str(oid)]["commission"]

    for t in trades:
        if t.get("action") != "SELL":
            continue
        sell_oid = t.get("order_id")
        buy_oid = t.get("buy_order_id")
        if not sell_oid or not buy_oid:
            t["pnl_source"] = "theoretical_partial"
            continue
        sell_oid = str(sell_oid)
        buy_oid = str(buy_oid)
        if sell_oid not in exec_map or buy_oid not in exec_map:
            t["pnl_source"] = "theoretical_partial"
            continue

        entry_px = exec_map[buy_oid]["price"]
        exit_px = exec_map[sell_oid]["price"]
        qty = float(t.get("qty", 0) or 0)
        comm = buy_comm.get(buy_oid, 0.0) + exec_map[sell_oid]["commission"]
        t["entry_price"] = entry_px
        t["exit_price"] = exit_px
        t["pnl"] = (exit_px - entry_px) * qty - comm
        t["commission"] = comm
        t["pnl_source"] = "execution"

    new_pnl = sum(float(t.get("pnl", 0) or 0) for t in trades if t.get("action") == "SELL")
    return {"old_pnl": old_pnl, "new_pnl": new_pnl}


def save_outputs(date: str, trades: list[dict], available_cash: float | None) -> Path:
    buys = sum(1 for t in trades if t.get("action") == "BUY")
    sells = sum(1 for t in trades if t.get("action") == "SELL")
    total_pnl = sum(float(t.get("pnl", 0) or 0) for t in trades if t.get("action") == "SELL")

    summary = {
        "date": date,
        "buys": buys,
        "sells": sells,
        "total_pnl": total_pnl,
        "pnl_basis": "execution",
        "available_cash": available_cash,
        "trades": trades,
    }
    summary_path = LOG_DIR / f"summary_{date}.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2, default=str)

    trades_path = LOG_DIR / f"trades_{date}.jsonl"
    with open(trades_path, "w", encoding="utf-8") as f:
        for t in trades:
            f.write(json.dumps(t, ensure_ascii=False, default=str) + "\n")
    return summary_path


def print_report(date: str, trades: list[dict], pnl_delta: dict[str, float], cash: float | None) -> None:
    total = pnl_delta["new_pnl"]
    print("=" * 60)
    print(f"  日次レポート（約定照会ベース） {date}")
    print("=" * 60)
    print(f"  理論損益(旧): {pnl_delta['old_pnl']:+,.0f}円")
    print(f"  実約定損益(新): {total:+,.0f}円")
    print(f"  差分: {total - pnl_delta['old_pnl']:+,.0f}円")
    print("-" * 60)
    for t in trades:
        if t.get("action") != "SELL":
            continue
        sign = "+" if t.get("pnl", 0) >= 0 else ""
        src = t.get("pnl_source", "?")
        print(
            f"  {t['symbol']} {t['name']} {t['qty']}株 "
            f"{t['entry_price']:.1f}→{t['exit_price']:.1f} "
            f"{sign}{t['pnl']:.0f}円 ({t['reason']}) [{src}]"
        )
    buys = sum(1 for t in trades if t.get("action") == "BUY")
    sells = sum(1 for t in trades if t.get("action") == "SELL")
    sign = "+" if total >= 0 else ""
    print("-" * 60)
    print(f"  買い: {buys}回  売り: {sells}回  合計損益: {sign}{total:.0f}円")
    if cash is not None:
        print(f"  現在 買付可能額: ¥{cash:,.0f}")
    print("=" * 60)


def main() -> None:
    ap = argparse.ArgumentParser(description="約定照会で日次レポートを再生成")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--config", default="kabu_config.json")
    args = ap.parse_args()

    cfg = load_config(Path(args.config))
    base_url = (
        "http://localhost:18080/kabusapi"
        if cfg.get("is_production")
        else cfg.get("base_url", "http://localhost:18081/kabusapi")
    )
    api = KabuAPI(
        KabuApiConfig(
            api_password=cfg["api_password"],
            trade_password=cfg["trade_password"],
            base_url=base_url,
            account_type=cfg.get("account_type", 4),
            exchange=cfg.get("exchange", 9),
        )
    )
    api.authenticate()

    trades = load_trades(args.date)
    orders = api.get_orders(product="1")
    backfill_sell_order_links(trades, orders, args.date)
    pnl_delta = enrich_trades_with_executions(trades, api)

    cash = None
    try:
        cash = float(api.get_wallet_cash().get("StockAccountWallet", 0) or 0)
    except Exception:
        pass

    path = save_outputs(args.date, trades, cash)
    print_report(args.date, trades, pnl_delta, cash)
    print(f"saved: {path}")


if __name__ == "__main__":
    main()
