import argparse
import datetime as dt
import json
import os
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
import yfinance as yf

from nikkei225_list import NIKKEI225
from simulation_realistic import get_earnings_tickers, run_daily_selection


JST = dt.timezone(dt.timedelta(hours=9))


def now_jst() -> dt.datetime:
    return dt.datetime.now(JST)


def today_jst_str() -> str:
    return now_jst().strftime("%Y-%m-%d")


def normalize_tz_index(idx: pd.DatetimeIndex) -> pd.DatetimeIndex:
    if getattr(idx, "tz", None) is not None:
        idx = idx.tz_convert("Asia/Tokyo").tz_localize(None)
    return idx


class SbiClient:
    def __init__(self, base_url: str, version: str, user_id: str, password: str, trade_password: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.version = version
        self.user_id = user_id
        self.password = password
        self.trade_password = trade_password
        self.token: Optional[str] = None

    def _url(self, path: str) -> str:
        return f"{self.base_url}/{self.version}/{path.lstrip('/')}"

    def login(self) -> str:
        r = requests.post(
            self._url("token"),
            json={"UserId": self.user_id, "Password": self.password},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        token = data.get("Token")
        if not token:
            raise RuntimeError(f"Token not found in response: {data}")
        self.token = token
        return token

    def _headers(self) -> Dict[str, str]:
        if not self.token:
            raise RuntimeError("Not authenticated. Call login() first.")
        return {"X-API-KEY": self.token}

    def get_board(self, symbol: str, exchange: int = 1) -> dict:
        r = requests.get(self._url(f"board/{symbol}@{exchange}"), headers=self._headers(), timeout=10)
        r.raise_for_status()
        return r.json()

    def send_market_order(self, symbol: str, exchange: int, side: int, qty: int) -> dict:
        payload = {
            "Password": self.trade_password,
            "Symbol": symbol,
            "Exchange": exchange,
            "SecurityType": 1,
            "Side": side,  # 1=buy, 2=sell
            "CashMargin": 1,  # cash
            "DelivType": 2,
            "FundType": "11",
            "AccountType": 4,
            "Qty": qty,
            "FrontOrderType": 10,  # market
            "Price": 0,
            "ExpireDay": 0,
        }
        r = requests.post(self._url("sendorder"), headers=self._headers(), json=payload, timeout=10)
        r.raise_for_status()
        return r.json()


def select_target_stocks(target_date: str, top_n: int, enable_earnings_filter: bool) -> List[Tuple[str, float, str]]:
    symbols = [s[0] for s in NIKKEI225]
    raw = yf.download(
        " ".join(symbols),
        period="12d",
        interval="15m",
        group_by="ticker",
        auto_adjust=True,
        progress=False,
        threads=True,
    )

    earnings_today = set(get_earnings_tickers(target_date) if enable_earnings_filter else [])
    scores: List[Tuple[str, float, str]] = []
    for code, name in NIKKEI225:
        if code in earnings_today:
            continue
        try:
            df = raw[code].copy().dropna()
        except Exception:
            continue
        if df.empty:
            continue
        df.index = normalize_tz_index(df.index)
        d_prev = df[df.index.strftime("%Y-%m-%d") < target_date]
        if len(d_prev) < 130:
            continue
        last_close = d_prev["Close"].iloc[-1]
        if isinstance(last_close, pd.Series):
            last_close = last_close.iloc[0]
        if float(last_close) >= 9000:
            continue
        score = run_daily_selection(d_prev)
        scores.append((code, float(score), name))
    scores.sort(key=lambda x: x[1], reverse=True)
    return scores[:top_n]


@dataclass
class Position:
    qty: int
    entry_price: float
    entry_time: str


@dataclass
class SymbolState:
    bars: pd.DataFrame = field(default_factory=lambda: pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"]))
    current_bucket: Optional[dt.datetime] = None
    open_price: Optional[float] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None
    close_price: Optional[float] = None
    start_volume: Optional[float] = None
    last_total_volume: Optional[float] = None


def floor_to_5m(ts: dt.datetime) -> dt.datetime:
    minute = (ts.minute // 5) * 5
    return ts.replace(minute=minute, second=0, microsecond=0)


class LiveEngine:
    def __init__(
        self,
        client: SbiClient,
        target_stocks: List[Tuple[str, float, str]],
        initial_cash: float,
        mode: str,
        volume_mult: float,
        poll_sec: int,
        log_path: str,
        allow_live_orders: bool,
    ) -> None:
        self.client = client
        self.target_stocks = target_stocks
        self.symbols = [s[0] for s in target_stocks]
        self.names = {s[0]: s[2] for s in target_stocks}
        self.cash = float(initial_cash)
        self.mode = mode
        self.volume_mult = volume_mult
        self.poll_sec = poll_sec
        self.allow_live_orders = allow_live_orders
        self.positions: Dict[str, Position] = {}
        self.states: Dict[str, SymbolState] = {sym: SymbolState() for sym in self.symbols}
        self.logs: List[dict] = []
        self.log_path = log_path

    def _append_bar(self, sym: str, bucket: dt.datetime) -> None:
        st = self.states[sym]
        if st.open_price is None or st.close_price is None:
            return
        end_total = st.last_total_volume if st.last_total_volume is not None else 0.0
        start_total = st.start_volume if st.start_volume is not None else end_total
        bar_volume = max(0.0, float(end_total) - float(start_total))
        st.bars.loc[bucket] = {
            "Open": float(st.open_price),
            "High": float(st.high_price if st.high_price is not None else st.open_price),
            "Low": float(st.low_price if st.low_price is not None else st.open_price),
            "Close": float(st.close_price),
            "Volume": float(bar_volume),
        }
        st.bars = st.bars.sort_index().tail(300)

    def _update_symbol(self, sym: str, price: float, total_volume: float, now_local: dt.datetime) -> None:
        st = self.states[sym]
        bucket = floor_to_5m(now_local)
        if st.current_bucket is None:
            st.current_bucket = bucket
            st.open_price = st.high_price = st.low_price = st.close_price = price
            st.start_volume = total_volume
            st.last_total_volume = total_volume
            return
        if bucket != st.current_bucket:
            self._append_bar(sym, st.current_bucket)
            st.current_bucket = bucket
            st.open_price = st.high_price = st.low_price = st.close_price = price
            st.start_volume = total_volume
            st.last_total_volume = total_volume
            return
        st.close_price = price
        st.high_price = max(float(st.high_price), float(price)) if st.high_price is not None else price
        st.low_price = min(float(st.low_price), float(price)) if st.low_price is not None else price
        st.last_total_volume = total_volume

    def _indicators(self, sym: str) -> Optional[pd.DataFrame]:
        bars = self.states[sym].bars.copy()
        if len(bars) < 22:
            return None
        bars["ema5"] = bars["Close"].ewm(span=5, adjust=False).mean()
        bars["ema15"] = bars["Close"].ewm(span=15, adjust=False).mean()
        bars["vol_ma20"] = bars["Volume"].rolling(window=20, min_periods=5).mean()
        return bars

    def _place_buy(self, sym: str, price: float, now_local: dt.datetime) -> None:
        qty = int(self.cash // (price * 100)) * 100
        if qty < 100:
            return
        if self.mode == "live" and self.allow_live_orders:
            self.client.send_market_order(sym, exchange=1, side=1, qty=qty)
        self.cash -= qty * price
        self.positions[sym] = Position(qty=qty, entry_price=price, entry_time=now_local.strftime("%H:%M:%S"))

    def _place_sell(self, sym: str, price: float, reason: str, now_local: dt.datetime) -> None:
        pos = self.positions[sym]
        if self.mode == "live" and self.allow_live_orders:
            self.client.send_market_order(sym, exchange=1, side=2, qty=pos.qty)
        pnl = (price - pos.entry_price) * pos.qty
        self.cash += pos.qty * price
        self.logs.append(
            {
                "entry_time": pos.entry_time,
                "exit_time": now_local.strftime("%H:%M:%S"),
                "symbol": sym,
                "name": self.names.get(sym, sym),
                "qty": pos.qty,
                "entry_price": pos.entry_price,
                "exit_price": price,
                "pnl": pnl,
                "reason": reason,
            }
        )
        del self.positions[sym]

    def run(self) -> None:
        start = now_jst()
        target_date = start.strftime("%Y-%m-%d")
        hard_close = start.replace(hour=15, minute=15, second=0, microsecond=0)
        open_after = start.replace(hour=9, minute=30, second=0, microsecond=0)
        stop_open = start.replace(hour=14, minute=45, second=0, microsecond=0)

        print(f"[{self.mode}] start {target_date}, targets={len(self.target_stocks)}, cash={self.cash:,.0f}")
        print(f"[{self.mode}] symbols(top5): {', '.join([self.names[s] for s in self.symbols[:5]])}")

        while True:
            now_local = now_jst()
            if now_local >= hard_close:
                break
            for sym in self.symbols:
                try:
                    board = self.client.get_board(sym, exchange=1)
                    price = float(board.get("CurrentPrice") or 0.0)
                    total_volume = float(board.get("TradingVolume") or 0.0)
                    if price <= 0:
                        continue
                    self._update_symbol(sym, price, total_volume, now_local)
                except Exception:
                    continue

            # exits first
            for sym in list(self.positions.keys()):
                inds = self._indicators(sym)
                if inds is None or len(inds) < 2:
                    continue
                prev = inds.iloc[-2]
                curr = inds.iloc[-1]
                dead_cross = (prev["ema5"] >= prev["ema15"]) and (curr["ema5"] < curr["ema15"])
                last_price = float(curr["Close"])
                stop_loss = last_price <= self.positions[sym].entry_price * 0.995
                if dead_cross or stop_loss or now_local >= hard_close:
                    reason = "StopLoss" if stop_loss else ("TimeLimit" if now_local >= hard_close else "DeadCross")
                    self._place_sell(sym, last_price, reason, now_local)

            # entries
            if open_after <= now_local < stop_open:
                for sym in self.symbols:
                    if sym in self.positions:
                        continue
                    inds = self._indicators(sym)
                    if inds is None or len(inds) < 2:
                        continue
                    prev = inds.iloc[-2]
                    curr = inds.iloc[-1]
                    golden = (prev["ema5"] <= prev["ema15"]) and (curr["ema5"] > curr["ema15"])
                    vol_ma = float(curr["vol_ma20"]) if pd.notna(curr["vol_ma20"]) else 0.0
                    vol_now = float(curr["Volume"])
                    volume_surge = vol_ma > 0 and vol_now >= vol_ma * self.volume_mult
                    if golden and volume_surge:
                        self._place_buy(sym, float(curr["Close"]), now_local)

            time.sleep(self.poll_sec)

        # final liquidation
        for sym in list(self.positions.keys()):
            inds = self._indicators(sym)
            if inds is None or len(inds) == 0:
                continue
            self._place_sell(sym, float(inds.iloc[-1]["Close"]), "TimeLimit", now_jst())

        summary = {
            "type": "summary",
            "mode": self.mode,
            "date": target_date,
            "initial_cash": self.cash - sum([x["pnl"] for x in self.logs]),
            "final_cash": self.cash,
            "total_pnl": sum([x["pnl"] for x in self.logs]),
            "trade_count": len(self.logs),
            "volume_mult": self.volume_mult,
        }
        with open(self.log_path, "w", encoding="utf-8") as f:
            f.write(json.dumps(summary, ensure_ascii=False) + "\n")
            for rec in self.logs:
                row = {"type": "trade", **rec}
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        print(f"[{self.mode}] total pnl: {summary['total_pnl']:+,.0f} yen | final cash: {self.cash:,.0f}")
        print(f"[{self.mode}] wrote: {self.log_path}")


def main() -> None:
    p = argparse.ArgumentParser(description="SBI HyperSBI2 live/paper bot (top-80 strategy)")
    p.add_argument("--mode", choices=["paper", "live"], default="paper")
    p.add_argument("--initial-cash", type=float, default=1_000_000)
    p.add_argument("--top-n", type=int, default=80)
    p.add_argument("--volume-mult", type=float, default=1.2)
    p.add_argument("--poll-sec", type=int, default=20)
    p.add_argument("--target-date", type=str, default=today_jst_str())
    p.add_argument("--enable-earnings-filter", action="store_true")
    p.add_argument("--api-base", type=str, default="http://localhost:18080")
    p.add_argument("--api-version", type=str, default="v1")
    p.add_argument("--allow-live-orders", action="store_true", help="must be set in live mode to actually place orders")
    p.add_argument("--log-file", type=str, default="")
    args = p.parse_args()

    if not args.log_file:
        args.log_file = f"sbi_{args.mode}_logs_{args.target_date}.jsonl"

    sbi_user = os.getenv("SBI_USER_ID", "")
    sbi_pass = os.getenv("SBI_PASSWORD", "")
    trade_pass = os.getenv("TRADE_PASSWORD", "")
    if not sbi_user or not sbi_pass:
        raise RuntimeError("Set SBI_USER_ID and SBI_PASSWORD environment variables.")
    if args.mode == "live" and not trade_pass:
        raise RuntimeError("Set TRADE_PASSWORD for live mode.")
    if args.mode == "live" and not args.allow_live_orders:
        raise RuntimeError("Refusing live run without --allow-live-orders.")

    targets = select_target_stocks(
        target_date=args.target_date,
        top_n=args.top_n,
        enable_earnings_filter=args.enable_earnings_filter,
    )
    if not targets:
        raise RuntimeError("No target stocks found. Check date/data conditions.")

    client = SbiClient(
        base_url=args.api_base,
        version=args.api_version,
        user_id=sbi_user,
        password=sbi_pass,
        trade_password=trade_pass,
    )
    client.login()

    engine = LiveEngine(
        client=client,
        target_stocks=targets,
        initial_cash=args.initial_cash,
        mode=args.mode,
        volume_mult=args.volume_mult,
        poll_sec=args.poll_sec,
        log_path=args.log_file,
        allow_live_orders=args.allow_live_orders,
    )
    engine.run()


if __name__ == "__main__":
    main()

