"""
kabuステーションAPI を利用したライブトレーダー。
simulation_realistic.py と同じ EMA GC/DC + 出来高急増戦略を、
5分足確定（東証グリッド）でのみエントリー/決済するリアルタイムトレーダー。

使い方:
  python kabu_trader.py                     # 検証環境（デフォルト）
  python kabu_trader.py --production        # 本番環境
  python kabu_trader.py --production --signal-only  # 本番・発注なし（ログは kabu_logs/signal_live_日付.log）
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from kabu_api import (
    KabuAPI,
    KabuApiConfig,
    KabuApiError,
    ORDER_EXCHANGE,
    order_exchange_label,
)
from nikkei225_list import NIKKEI225
from simulation_realistic import (
    SESSION_PROFILES,
    compute_rank_top_n,
    download_with_retry,
    extract_symbol_frame,
    fetch_single_symbol,
    get_earnings_tickers,
    is_entry_blocked_by_session,
    is_time_limit_session,
    run_daily_selection,
)

JP_SESSION = SESSION_PROFILES["JP"]

JST = ZoneInfo("Asia/Tokyo")
LOG_DIR = Path("kabu_logs")
STATE_FILE = Path("kabu_trader_state.json")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("kabu_trader")


# ─── 時刻定数（JST 分表現） ───────────────────────────
MORNING_BLOCK_END = 9 * 60 + 30     # 9:30 以降エントリー可
CLOSE_BLOCK_START = 14 * 60 + 45    # 14:45 以降新規エントリー禁止
TIMELIMIT_START = 15 * 60 + 15      # 15:15 以降ポジション強制決済
MARKET_CLOSE = 15 * 60 + 30         # 15:30 に全処理終了
PREMARKET_PREP = 8 * 60 + 30        # 8:30 にプレマーケット準備開始

POLL_INTERVAL_ENTRY = 20             # 板更新サイクル間隔（秒）
BAR_UPDATE_MAX_SEC = 120             # 1回の全銘柄板更新の上限（長時間ブロック防止）
BAR_INTERVAL_MIN = 5                 # 5分足バー
BATCH_BOARD_SIZE = 45                # 1バッチの板取得上限（API登録上限50に余裕）
MAX_TOTAL_SYMBOLS = 135              # 最大監視銘柄数（3バッチ×45でローテーション）


@dataclass
class LivePosition:
    symbol: str
    name: str
    qty: int
    entry_price: float
    entry_time: datetime
    order_id: str = ""


@dataclass
class BarAccumulator:
    """リアルタイム板情報から5分足 OHLCV を蓄積"""
    open: float = 0.0
    high: float = 0.0
    low: float = float("inf")
    close: float = 0.0
    volume: float = 0.0
    bar_start: datetime | None = None


@dataclass
class FinalizedBar:
    """確定した5分足1本（シミュの1タイムスタンプに相当）"""
    ss: SymbolState
    close: float
    volume: float
    bar_start: datetime


@dataclass
class SymbolState:
    code: str
    name: str
    ema5: float = 0.0
    ema15: float = 0.0
    prev_ema5: float = 0.0
    prev_ema15: float = 0.0
    vol_ma20: float = 0.0
    vol_history: list = field(default_factory=list)
    bar_count: int = 0
    current_bar: BarAccumulator = field(default_factory=BarAccumulator)
    last_price: float = 0.0
    last_volume_cumulative: float = 0.0


class KabuTrader:
    def __init__(
        self,
        config_path: str = "kabu_config.json",
        dry_run: bool = False,
        production: bool = False,
        signal_only: bool = False,
    ):
        self.dry_run = dry_run
        self.signal_only = signal_only
        with open(config_path, encoding="utf-8") as f:
            raw = json.load(f)

        base_url = raw.get("base_url", "http://localhost:18081/kabusapi")
        if production:
            base_url = "http://localhost:18080/kabusapi"

        self.api_config = KabuApiConfig(
            api_password=raw["api_password"],
            trade_password=raw["trade_password"],
            base_url=base_url,
            account_type=raw.get("account_type", 4),
            exchange=raw.get("exchange", ORDER_EXCHANGE),
            # 信用口座ありでも現物買いのみ。預り区分 AA=信用代用（02=保護は信用口座ありだと 100031）
            fund_type=str(raw.get("fund_type", "AA")),
        )
        if self.api_config.exchange != ORDER_EXCHANGE:
            logger.warning(
                "kabu_config.json の exchange=%s は無視し、発注は %s で行います",
                self.api_config.exchange,
                order_exchange_label(ORDER_EXCHANGE),
            )
        self.api = KabuAPI(self.api_config)

        self.rank_fraction: float = raw.get("rank_fraction", 0.49)
        self.volume_mult: float = raw.get("volume_mult", 1.38)
        self.stop_loss_pct: float = raw.get("stop_loss_pct", 0.005)
        self.max_position_value_pct: float = float(raw.get("max_position_value_pct", 1.0))
        self.oneshot_max_yen: float = float(raw.get("oneshot_max_yen", 500_000))
        self.max_lot_value_yen: float = float(raw.get("max_lot_value_yen", 500_000))
        self.yf_period: str = raw.get("yf_period", "59d")

        self.positions: dict[str, LivePosition] = {}
        self.symbol_states: dict[str, SymbolState] = {}
        self.target_stocks: list[tuple[str, float, str]] = []
        self.trade_logs: list[dict] = []
        self.available_cash: float = 0.0
        self.top_n: int = 0

        self.is_production = production
        self._today_str: str = ""

        LOG_DIR.mkdir(exist_ok=True)

    # ─── ユーティリティ ─────────────────────────────────

    def _now(self) -> datetime:
        return datetime.now(JST)

    def _now_minutes(self) -> int:
        n = self._now()
        return n.hour * 60 + n.minute

    def _is_past_timelimit(self) -> bool:
        return self._now_minutes() >= TIMELIMIT_START

    def _is_market_closed(self) -> bool:
        return self._now_minutes() >= MARKET_CLOSE

    def _force_timelimit_exits(self, reason: str = "TimeLimit") -> None:
        """15:15以降または終了時に全ポジションを成行決済する。"""
        if not self.positions or self.signal_only:
            return
        logger.info("--- %s: 全ポジション決済 (%d件) ---", reason, len(self.positions))
        for sym in list(self.positions.keys()):
            pos = self.positions.get(sym)
            if not pos:
                continue
            self._update_single_board(sym)
            self._place_sell(pos, reason)
        if self.positions:
            logger.error(
                "決済未完了のポジション: %s",
                ", ".join(f"{s}({self.positions[s].qty}株)" for s in self.positions),
            )

    def _log_trade(self, trade: dict) -> None:
        self.trade_logs.append(trade)
        log_file = LOG_DIR / f"trades_{self._today_str}.jsonl"
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(trade, ensure_ascii=False, default=str) + "\n")

    def _save_state(self) -> None:
        state = {
            "date": self._today_str,
            "positions": {
                sym: {
                    "symbol": p.symbol,
                    "name": p.name,
                    "qty": p.qty,
                    "entry_price": p.entry_price,
                    "entry_time": p.entry_time.isoformat(),
                    "order_id": p.order_id,
                }
                for sym, p in self.positions.items()
            },
            "trade_count": len(self.trade_logs),
            "available_cash": self.available_cash,
        }
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)

    # ─── 認証 ──────────────────────────────────────────

    def _authenticate(self) -> None:
        if self.dry_run:
            logger.info("[DRY-RUN] 認証スキップ")
            return
        try:
            self.api.authenticate()
        except Exception as e:
            logger.error("認証失敗: %s", e)
            raise

    # ─── 余力取得 ────────────────────────────────────

    def _refresh_cash(self) -> float:
        if self.dry_run:
            return self.available_cash
        try:
            w = self.api.get_wallet_cash()
            cash = w.get("StockAccountWallet", 0) or 0
            self.available_cash = float(cash)
            logger.info("現物買付可能額: ¥%s", f"{self.available_cash:,.0f}")
        except KabuApiError as e:
            logger.warning("余力取得失敗: %s", e)
        return self.available_cash

    @staticmethod
    def _normalize_symbol(code: str) -> str:
        c = str(code).strip()
        if c.endswith(".T"):
            return c
        return f"{c}.T"

    def _order_budget_yen(self, wallet_cash: float) -> float:
        """1回の買いに使える上限金額（比率・ワンショット上限を反映）"""
        pct = max(0.0, min(1.0, self.max_position_value_pct))
        budget = wallet_cash * pct
        if self.oneshot_max_yen > 0:
            budget = min(budget, self.oneshot_max_yen)
        return budget

    def _compute_buy_qty(self, price: float, wallet_cash: float | None = None) -> int:
        if price <= 0:
            return 0
        cash = self.available_cash if wallet_cash is None else wallet_cash
        budget = self._order_budget_yen(cash)
        return int(budget // (price * 100)) * 100

    def _sync_positions_from_api(self) -> None:
        """口座の現物保有をAPIから読み込み、内部ポジションと同期"""
        if self.dry_run:
            return
        try:
            rows = self.api.get_positions(product="1")
        except KabuApiError as e:
            logger.warning("保有株取得失敗: %s", e)
            return

        if not rows:
            logger.info("口座保有株: 0件")
            return

        name_map = {sym: name for sym, name in NIKKEI225}
        aggregated: dict[str, dict] = {}
        for row in rows:
            sym = self._normalize_symbol(row.get("Symbol", ""))
            qty = int(float(row.get("LeavesQty", 0) or 0))
            if qty <= 0:
                continue
            px = float(row.get("Price", 0) or 0)
            if sym not in aggregated:
                aggregated[sym] = {"qty": 0, "cost": 0.0}
            aggregated[sym]["qty"] += qty
            aggregated[sym]["cost"] += qty * px

        logger.info("口座保有株: %d銘柄（APIレコード %d件）", len(aggregated), len(rows))
        for sym, data in aggregated.items():
            qty = data["qty"]
            avg_price = data["cost"] / qty if qty else 0.0
            name = name_map.get(sym, sym)
            if sym in self.positions:
                logger.warning("保有同期: %s は内部ポジションあり → APIで上書き", sym)
            self.positions[sym] = LivePosition(
                symbol=sym,
                name=name,
                qty=qty,
                entry_price=avg_price,
                entry_time=self._now(),
                order_id="SYNC",
            )
            logger.info("  保有: %s %s %d株 取得単価≈%.1f", sym, name, qty, avg_price)

    def _reset_symbol_registration(self) -> None:
        if self.dry_run:
            return
        try:
            resp = self.api.unregister_all()
            logger.info("API登録銘柄をリセット: %s", resp)
            time.sleep(2)
        except KabuApiError as e:
            logger.warning("銘柄登録リセット失敗: %s", e)

    def _unregister_between_batches(self) -> None:
        """バッチローテーション間の高速リセット（起動時より短い待機）"""
        if self.dry_run:
            return
        try:
            self.api.unregister_all()
            time.sleep(1)
        except KabuApiError as e:
            logger.warning("バッチ間リセット失敗: %s", e)

    def _gc_proximity(self, sym_code: str) -> float:
        """EMA5/EMA15の乖離率を返す。小さいほどGCに近い = 優先度高"""
        ss = self.symbol_states.get(sym_code)
        if not ss or ss.ema15 <= 0:
            return float("inf")
        return abs(ss.ema5 - ss.ema15) / ss.ema15

    # ─── プレマーケット：銘柄選定 ─────────────────────

    def _premarket_selection(self) -> None:
        logger.info("=== プレマーケット：銘柄選定開始 ===")
        self._today_str = self._now().strftime("%Y-%m-%d")

        idx_n = len(NIKKEI225)
        self.top_n = compute_rank_top_n(idx_n, self.rank_fraction, None)
        logger.info(
            "パラメータ: rank_fraction=%.2f, volume_mult=%.2f, top_n=%d",
            self.rank_fraction, self.volume_mult, self.top_n,
        )

        symbols = [s[0] for s in NIKKEI225]
        logger.info("15分足データ取得中 (yfinance period=%s)...", self.yf_period)
        data_15m_raw = download_with_retry(
            tickers=" ".join(symbols),
            period=self.yf_period,
            interval="15m",
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=True,
        )

        earnings_today = get_earnings_tickers(self._today_str)
        logger.info("本日の決算発表銘柄: %d 件", len(earnings_today))

        symbol_scores: list[tuple[str, float, str]] = []
        for sym_code, sym_name in NIKKEI225:
            if sym_code in earnings_today:
                continue
            d_df = extract_symbol_frame(data_15m_raw, sym_code)
            if d_df.empty:
                d_df = fetch_single_symbol(sym_code, "15m")
            if d_df.empty or not isinstance(d_df.index, pd.DatetimeIndex):
                continue
            if d_df.index.tz is not None:
                d_df.index = d_df.index.tz_convert("Asia/Tokyo").tz_localize(None)

            d_dates = d_df.index.strftime("%Y-%m-%d")
            d_df_prev = d_df[d_dates < self._today_str]
            if len(d_df_prev) < 130:
                continue

            last_close = d_df_prev["Close"].iloc[-1]
            if isinstance(last_close, pd.Series):
                last_close = last_close.iloc[0]
            if last_close * 100 > self.max_lot_value_yen:
                continue

            score = run_daily_selection(d_df_prev)
            symbol_scores.append((sym_code, score, sym_name))

        symbol_scores.sort(key=lambda x: x[1], reverse=True)
        full_list = symbol_scores[: self.top_n]
        total_limit = min(len(full_list), MAX_TOTAL_SYMBOLS)
        self.target_stocks = full_list[:total_limit]
        n_batches = math.ceil(len(self.target_stocks) / BATCH_BOARD_SIZE)
        logger.info(
            "対象銘柄選定完了: %d / %d 銘柄（EMAランク上位%d件、%dバッチでローテーション）",
            len(self.target_stocks), len(symbol_scores), self.top_n, n_batches,
        )
        if self.target_stocks:
            top5 = ", ".join(f"{s[2]}({s[1]:.4f})" for s in self.target_stocks[:5])
            logger.info("上位5: %s", top5)

    # ─── 5分足ヒストリカルデータでEMAウォームアップ ──────

    def _warmup_ema(self) -> None:
        logger.info("5分足ヒストリカルデータ取得（EMAウォームアップ）...")
        symbols = [s[0] for s in self.target_stocks]
        if not symbols:
            return

        intra_raw = download_with_retry(
            tickers=" ".join(symbols),
            period=self.yf_period,
            interval="5m",
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=True,
        )

        for sym_code, _score, sym_name in self.target_stocks:
            df = extract_symbol_frame(intra_raw, sym_code)
            if df.empty:
                df = fetch_single_symbol(sym_code, "5m")
            if df.empty or not isinstance(df.index, pd.DatetimeIndex):
                continue
            if df.index.tz is not None:
                df.index = df.index.tz_convert("Asia/Tokyo").tz_localize(None)

            close_s = df["Close"]
            if isinstance(close_s, pd.DataFrame):
                close_s = close_s.iloc[:, 0]
            vol_s = df["Volume"]
            if isinstance(vol_s, pd.DataFrame):
                vol_s = vol_s.iloc[:, 0]

            ema5 = close_s.ewm(span=5, adjust=False).mean()
            ema15 = close_s.ewm(span=15, adjust=False).mean()
            vol_ma20 = vol_s.rolling(window=20, min_periods=5).mean()

            ss = SymbolState(code=sym_code, name=sym_name)
            ss.ema5 = float(ema5.iloc[-1])
            ss.ema15 = float(ema15.iloc[-1])
            ss.prev_ema5 = float(ema5.iloc[-2]) if len(ema5) >= 2 else ss.ema5
            ss.prev_ema15 = float(ema15.iloc[-2]) if len(ema15) >= 2 else ss.ema15
            ss.vol_ma20 = float(vol_ma20.iloc[-1]) if pd.notna(vol_ma20.iloc[-1]) else 0.0
            recent_vols = vol_s.iloc[-20:].tolist()
            ss.vol_history = [float(v) for v in recent_vols if pd.notna(v)]
            ss.bar_count = len(close_s)
            ss.last_price = float(close_s.iloc[-1])

            self.symbol_states[sym_code] = ss

        self._warmup_board_volume()
        logger.info("EMAウォームアップ完了: %d 銘柄", len(self.symbol_states))

    def _warmup_board_volume(self) -> None:
        """起動時に板情報から累積出来高を取得（バッチローテーション対応）"""
        if self.dry_run:
            return
        all_syms = list(self.symbol_states.keys())
        batches = [all_syms[i:i + BATCH_BOARD_SIZE]
                   for i in range(0, len(all_syms), BATCH_BOARD_SIZE)]
        for batch_idx, batch in enumerate(batches):
            if batch_idx > 0:
                self._unregister_between_batches()
            for sym_code in batch:
                board = self._safe_get_board(sym_code)
                if board:
                    cum_vol = board.get("TradingVolume", 0) or 0
                    self.symbol_states[sym_code].last_volume_cumulative = float(cum_vol)
        if len(batches) > 1:
            self._unregister_between_batches()
        logger.info("板情報初期取得完了: %d バッチ", len(batches))

    # ─── 板情報取得 ─────────────────────────────────────

    def _safe_get_board(self, symbol_code: str) -> dict | None:
        if self.dry_run:
            return None
        code = symbol_code.replace(".T", "")
        try:
            return self.api.get_board(code, exchange=1)
        except KabuApiError as e:
            logger.warning("板情報取得失敗 %s: %s", symbol_code, e)
            return None

    # ─── 5分足バー更新（東証5分グリッド・simulation_realistic と同じ確定足ロジック）──

    def _five_min_slot(self, dt: datetime) -> datetime:
        m = (dt.minute // BAR_INTERVAL_MIN) * BAR_INTERVAL_MIN
        return dt.replace(minute=m, second=0, microsecond=0)

    def _update_bars(self) -> bool:
        """全対象銘柄の板情報をバッチローテーションで取得し、5分足バーを更新。
        GC近接度が高い（クロスに近い）銘柄を優先バッチに配置する。
        戻り値: True=完了, False=TimeLimit/タイムアウトで中断。"""
        if self._is_past_timelimit():
            return False

        deadline = time.monotonic() + BAR_UPDATE_MAX_SEC
        all_syms = list(self.symbol_states.keys())
        all_syms.sort(key=lambda s: self._gc_proximity(s))

        batches = [all_syms[i:i + BATCH_BOARD_SIZE]
                   for i in range(0, len(all_syms), BATCH_BOARD_SIZE)]

        total_ok = 0
        total_fail = 0
        pending: list[FinalizedBar] = []

        for batch_idx, batch in enumerate(batches):
            if self._is_past_timelimit():
                logger.info("TimeLimit時刻のためバー更新を中断（バッチ %d/%d）", batch_idx + 1, len(batches))
                return False
            if time.monotonic() >= deadline:
                logger.warning("バー更新が%d秒を超過したため中断", BAR_UPDATE_MAX_SEC)
                return False

            if batch_idx > 0:
                self._unregister_between_batches()

            now = self._now()
            for sym_code in batch:
                if self._is_past_timelimit() or time.monotonic() >= deadline:
                    logger.info("TimeLimit/タイムアウトのためバー更新を中断")
                    return False
                ss = self.symbol_states[sym_code]
                board = self._safe_get_board(sym_code)
                if not board:
                    total_fail += 1
                    continue

                price = board.get("CurrentPrice")
                cum_vol = board.get("TradingVolume", 0) or 0
                if price is None or price <= 0:
                    total_fail += 1
                    continue

                total_ok += 1
                cum_f = float(cum_vol)
                if cum_f < ss.last_volume_cumulative:
                    bar_vol = cum_f
                else:
                    bar_vol = max(0.0, cum_f - ss.last_volume_cumulative)

                self._accumulate_bar_tick(ss, float(price), bar_vol, now, pending)
                ss.last_price = float(price)
                ss.last_volume_cumulative = cum_f

        now = self._now()
        slot = self._five_min_slot(now)
        for ss in self.symbol_states.values():
            self._maybe_finalize_to_pending(ss, pending, now)
        n_finalized = len(pending)
        self._process_pending_bar_cycle(pending)

        logger.info(
            "バー更新: 板取得OK=%d 失敗=%d 確定バー=%d (バッチ=%d 総銘柄=%d)",
            total_ok, total_fail, n_finalized, len(batches), len(all_syms),
        )
        return True

    def _maybe_finalize_to_pending(
        self, ss: SymbolState, pending: list[FinalizedBar], now: datetime
    ) -> None:
        slot = self._five_min_slot(now)
        bar = ss.current_bar
        if bar.bar_start is None or bar.bar_start >= slot:
            return
        pending.append(
            FinalizedBar(
                ss=ss,
                close=bar.close,
                volume=bar.volume,
                bar_start=bar.bar_start,
            )
        )
        ss.current_bar = BarAccumulator()

    def _accumulate_bar_tick(
        self,
        ss: SymbolState,
        price: float,
        bar_vol: float,
        now: datetime,
        pending: list[FinalizedBar],
    ) -> None:
        self._maybe_finalize_to_pending(ss, pending, now)
        bar = ss.current_bar
        slot = self._five_min_slot(now)
        if bar.bar_start is None:
            bar.bar_start = slot
            bar.open = bar.high = bar.low = bar.close = price
            bar.volume = bar_vol
        else:
            bar.high = max(bar.high, price)
            bar.low = min(bar.low, price)
            bar.close = price
            bar.volume += bar_vol

    def _apply_ema_from_bar_close(
        self, ss: SymbolState, close: float, volume: float
    ) -> None:
        ss.prev_ema5 = ss.ema5
        ss.prev_ema15 = ss.ema15
        alpha5 = 2.0 / (5 + 1)
        alpha15 = 2.0 / (15 + 1)
        ss.ema5 = alpha5 * close + (1 - alpha5) * ss.ema5
        ss.ema15 = alpha15 * close + (1 - alpha15) * ss.ema15

        ss.vol_history.append(volume)
        if len(ss.vol_history) > 20:
            ss.vol_history = ss.vol_history[-20:]
        if len(ss.vol_history) >= 5:
            ss.vol_ma20 = float(np.mean(ss.vol_history))
        ss.bar_count += 1

    def _process_pending_bar_cycle(self, pending: list[FinalizedBar]) -> None:
        """確定足ごとに EMA更新→全銘柄決済→全銘柄エントリー（sim のタイムスタンプループと同順）。"""
        if not pending:
            return
        code_order = {s[0]: i for i, s in enumerate(self.target_stocks)}
        by_slot: dict[datetime, list[FinalizedBar]] = {}
        for fb in pending:
            by_slot.setdefault(fb.bar_start, []).append(fb)

        for bar_start in sorted(by_slot):
            batch = by_slot[bar_start]
            batch.sort(key=lambda fb: code_order.get(fb.ss.code, 9999))

            for fb in batch:
                self._apply_ema_from_bar_close(fb.ss, fb.close, fb.volume)
            for fb in batch:
                self._process_bar_exits(fb.ss, fb.close, bar_start)
            if is_time_limit_session(bar_start, JP_SESSION):
                continue
            for fb in batch:
                self._process_bar_entry(fb.ss, fb.close, fb.volume, bar_start)

    def _process_bar_exits(
        self, ss: SymbolState, bar_close: float, bar_start: datetime
    ) -> None:
        pos = self.positions.get(ss.code)
        if not pos:
            return
        if is_time_limit_session(bar_start, JP_SESSION):
            logger.info("TimeLimit(5分足確定): %s 終値=%.1f", ss.code, bar_close)
            self._place_sell(pos, "TimeLimit", exit_price=bar_close)
            return
        stop_price = pos.entry_price * (1.0 - self.stop_loss_pct)
        if bar_close <= stop_price:
            logger.info(
                "StopLoss(5分足確定): %s 終値=%.1f < 損切=%.1f (entry=%.1f)",
                ss.code, bar_close, stop_price, pos.entry_price,
            )
            self._place_sell(pos, "StopLoss", exit_price=bar_close)
            return
        if self._check_dead_cross(ss):
            logger.info(
                "DeadCross(5分足確定): %s (ema5=%.1f<ema15=%.1f)",
                ss.code, ss.ema5, ss.ema15,
            )
            self._place_sell(pos, "DeadCross", exit_price=bar_close)

    def _process_bar_entry(
        self,
        ss: SymbolState,
        bar_close: float,
        volume: float,
        bar_start: datetime,
    ) -> None:
        if ss.code in self.positions:
            return
        if is_entry_blocked_by_session(bar_start, JP_SESSION):
            return
        if not self._check_golden_cross(ss):
            return
        if ss.vol_ma20 <= 0 or volume < ss.vol_ma20 * self.volume_mult:
            return
        logger.info(
            "GC+出来高(5分足確定): %s %s 終値=%.1f ema5=%.1f>ema15=%.1f vol=%.0f/ma=%.0f",
            ss.code, ss.name, bar_close, ss.ema5, ss.ema15, volume, ss.vol_ma20,
        )
        self._place_buy(ss, entry_price=bar_close)

    # ─── シグナル判定 ──────────────────────────────────

    def _check_golden_cross(self, ss: SymbolState) -> bool:
        return ss.prev_ema5 <= ss.prev_ema15 and ss.ema5 > ss.ema15

    def _check_dead_cross(self, ss: SymbolState) -> bool:
        return ss.prev_ema5 >= ss.prev_ema15 and ss.ema5 < ss.ema15

    # ─── 発注ロジック ──────────────────────────────────

    def _place_buy(self, ss: SymbolState, *, entry_price: float | None = None) -> bool:
        price = entry_price if entry_price is not None and entry_price > 0 else ss.last_price
        if price <= 0:
            return False

        if not self.dry_run:
            self._refresh_cash()

        cash = self.available_cash
        budget = self._order_budget_yen(cash)
        qty = self._compute_buy_qty(price, cash)
        oneshot_note = (
            f"¥{self.oneshot_max_yen:,.0f}" if self.oneshot_max_yen > 0 else "なし"
        )
        logger.info(
            "注文資金: 買付可能=¥%s × %.0f%% → ¥%s（ワンショット上限=%s）→ %d株 @%.1f",
            f"{cash:,.0f}",
            self.max_position_value_pct * 100,
            f"{budget:,.0f}",
            oneshot_note,
            qty,
            price,
        )
        if qty < 100:
            logger.info(
                "資金不足: %s 価格=%.1f 使用可能予算=%.0f",
                ss.code, price, budget,
            )
            return False

        sym_bare = ss.code.replace(".T", "")
        now = self._now()

        if self.signal_only:
            logger.info(
                "[SIGNAL-ONLY] 買いシグナル: %s %s %d株 @%.1f (ema5=%.2f ema15=%.2f)",
                ss.code, ss.name, qty, price, ss.ema5, ss.ema15,
            )
            self._log_trade({
                "date": self._today_str, "time": now.isoformat(),
                "action": "BUY_SIGNAL", "symbol": ss.code, "name": ss.name,
                "qty": qty, "price": price, "ema5": ss.ema5, "ema15": ss.ema15,
            })
            return False

        if self.dry_run:
            order_id = f"DRY-{now.strftime('%H%M%S')}-{sym_bare}"
            logger.info(
                "[DRY-RUN] 買い注文: %s %s %d株 @約%.1f",
                ss.code, ss.name, qty, price,
            )
        else:
            try:
                resp = self.api.send_buy_order(sym_bare, qty, order_type=10, price=0)
                result = resp.get("Result", -1)
                order_id = resp.get("OrderId", "")
                if result != 0:
                    logger.warning("買い注文エラー: %s result=%s resp=%s", ss.code, result, resp)
                    return False
                logger.info(
                    "買い注文発注: %s %s %d株 order_id=%s",
                    ss.code, ss.name, qty, order_id,
                )
            except KabuApiError as e:
                logger.error("買い注文API失敗: %s %s", ss.code, e)
                return False

        self.positions[ss.code] = LivePosition(
            symbol=ss.code,
            name=ss.name,
            qty=qty,
            entry_price=price,
            entry_time=now,
            order_id=order_id,
        )
        if self.dry_run:
            self.available_cash = max(0.0, self.available_cash - qty * price)
        else:
            self._refresh_cash()

        self._log_trade({
            "date": self._today_str,
            "time": now.isoformat(),
            "action": "BUY",
            "symbol": ss.code,
            "name": ss.name,
            "qty": qty,
            "price": price,
            "order_id": order_id,
        })
        self._save_state()
        return True

    def _place_sell(
        self, pos: LivePosition, reason: str, *, exit_price: float | None = None
    ) -> bool:
        sym_bare = pos.symbol.replace(".T", "")
        now = self._now()
        ss = self.symbol_states.get(pos.symbol)
        if exit_price is None:
            exit_price = ss.last_price if ss else pos.entry_price

        if self.signal_only:
            logger.info(
                "[SIGNAL-ONLY] 売りシグナル(%s): %s %s %d株 @%.1f (entry=%.1f)",
                reason, pos.symbol, pos.name, pos.qty, exit_price, pos.entry_price,
            )
            self._log_trade({
                "date": self._today_str, "time": now.isoformat(),
                "action": "SELL_SIGNAL", "reason": reason,
                "symbol": pos.symbol, "name": pos.name,
                "qty": pos.qty, "entry_price": pos.entry_price,
                "exit_price": exit_price,
            })
            return False

        if self.dry_run:
            logger.info(
                "[DRY-RUN] 売り注文(%s): %s %s %d株 @約%.1f",
                reason, pos.symbol, pos.name, pos.qty, exit_price,
            )
        else:
            try:
                resp = self.api.send_sell_order(sym_bare, pos.qty, order_type=10, price=0)
                result = resp.get("Result", -1)
                if result != 0:
                    logger.warning("売り注文エラー: %s result=%s resp=%s", pos.symbol, result, resp)
                    return False
                order_id = resp.get("OrderId", "")
                logger.info(
                    "売り注文発注(%s): %s %s %d株 order_id=%s",
                    reason, pos.symbol, pos.name, pos.qty, order_id,
                )
            except KabuApiError as e:
                logger.error("売り注文API失敗: %s %s", pos.symbol, e)
                return False

        pnl = (exit_price - pos.entry_price) * pos.qty
        if self.dry_run:
            self.available_cash += pos.qty * exit_price
        else:
            self._refresh_cash()

        self._log_trade({
            "date": self._today_str,
            "time": now.isoformat(),
            "action": "SELL",
            "reason": reason,
            "symbol": pos.symbol,
            "name": pos.name,
            "qty": pos.qty,
            # 約定照会で実約定単価に差し替えるために必要
            "order_id": order_id,
            "buy_order_id": pos.order_id,
            "entry_price": pos.entry_price,
            "exit_price": exit_price,
            "pnl": pnl,
        })

        del self.positions[pos.symbol]
        self._save_state()

        sign = "+" if pnl >= 0 else ""
        logger.info(
            "決済完了(%s): %s %s 損益=%s%.0f円",
            reason, pos.symbol, pos.name, sign, pnl,
        )
        return True

    # ─── メインループ ──────────────────────────────────

    def _trading_loop(self) -> None:
        logger.info(
            "=== トレーディングループ開始 === "
            "エントリー/決済=5分足確定のみ（GC/DC/損切%.2f%%/出来高）simulation_realistic 準拠",
            self.stop_loss_pct * 100,
        )
        last_bar_update = time.monotonic()

        while True:
            # TimeLimit を最優先（MARKET_CLOSE より先）。長時間ブロック後の取りこぼしを防ぐ
            if self._is_past_timelimit():
                self._force_timelimit_exits("TimeLimit")
                if self._is_market_closed():
                    logger.info("市場閉場。ループ終了。")
                    break
                time.sleep(5)
                continue

            if self._is_market_closed():
                self._force_timelimit_exits("TimeLimit")
                logger.info("市場閉場。ループ終了。")
                break

            if time.monotonic() - last_bar_update >= POLL_INTERVAL_ENTRY:
                self._update_bars()
                last_bar_update = time.monotonic()

            time.sleep(5)

    def _update_single_board(self, sym_code: str) -> None:
        ss = self.symbol_states.get(sym_code)
        if not ss:
            return
        board = self._safe_get_board(sym_code)
        if board:
            price = board.get("CurrentPrice")
            if price and price > 0:
                ss.last_price = float(price)

    # ─── デイリーサマリー ────────────────────────────────

    def _print_daily_summary(self) -> None:
        # 日次サマリー出力時に約定照会し、実約定単価ベースで pnl を更新する
        if self.is_production and not self.dry_run:
            try:
                self._enrich_trades_with_executions()
            except Exception:
                logger.warning("約定照会による損益更新に失敗。理論損益のまま出力します。\n%s", traceback.format_exc())

        logger.info("=" * 60)
        logger.info("  デイリーサマリー  %s", self._today_str)
        logger.info("=" * 60)
        total_pnl = 0.0
        for t in self.trade_logs:
            if t.get("action") == "SELL":
                pnl = t.get("pnl", 0)
                total_pnl += pnl
                sign = "+" if pnl >= 0 else ""
                logger.info(
                    "  %s %s %d株 %.1f→%.1f %s%.0f円 (%s)",
                    t["symbol"], t["name"], t["qty"],
                    t["entry_price"], t["exit_price"],
                    sign, pnl, t["reason"],
                )
        buys = sum(1 for t in self.trade_logs if t.get("action") == "BUY")
        sells = sum(1 for t in self.trade_logs if t.get("action") == "SELL")
        sign = "+" if total_pnl >= 0 else ""
        logger.info("-" * 60)
        logger.info("  買い: %d回  売り: %d回  合計損益: %s%.0f円", buys, sells, sign, total_pnl)
        logger.info("  残余力: ¥%s", f"{self.available_cash:,.0f}")
        logger.info("=" * 60)

        summary = {
            "date": self._today_str,
            "buys": buys,
            "sells": sells,
            "total_pnl": total_pnl,
            "available_cash": self.available_cash,
            "trades": self.trade_logs,
        }
        summary_file = LOG_DIR / f"summary_{self._today_str}.json"
        with open(summary_file, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2, default=str)

        # trades_*.jsonl も「更新後の trade_logs」で上書きして整合させる
        try:
            trades_file = LOG_DIR / f"trades_{self._today_str}.jsonl"
            with open(trades_file, "w", encoding="utf-8") as f:
                for trade in self.trade_logs:
                    f.write(json.dumps(trade, ensure_ascii=False, default=str) + "\n")
        except Exception:
            logger.warning("trades_*.jsonl の上書きに失敗しました: %s", traceback.format_exc())

    def _extract_exec_price_from_order(self, order: dict) -> tuple[float | None, float, float]:
        """
        order['Details'] から実約定平均単価を抽出する。
        戻り値: (exec_price, exec_qty, commission_total)
        """
        details = order.get("Details") or []
        exec_qty = 0.0
        exec_value = 0.0
        commission_total = 0.0

        for d in details:
            # RecType=8 が約定/トランザクション断片（例: sample ではここに Price が入っていた）
            if int(d.get("RecType", -1)) != 8:
                continue
            state = d.get("State", None)
            # state が 3=約定済みのケースが多いが、念のため Price/Qty があるものを優先
            try:
                px = float(d.get("Price", 0) or 0)
                qty = float(d.get("Qty", 0) or 0)
            except Exception:
                continue
            if px <= 0 or qty <= 0:
                continue

            exec_qty += qty
            exec_value += px * qty
            try:
                commission_total += float(d.get("Commission", 0) or 0) + float(d.get("CommissionTax", 0) or 0)
            except Exception:
                pass

        if exec_qty <= 0:
            return None, 0.0, commission_total
        return exec_value / exec_qty, exec_qty, commission_total

    def _enrich_trades_with_executions(self) -> None:
        """
        trade_logs 内の BUY/SELL（order_id/buy_order_id）を使い、
        api.get_orders() から実約定単価を引いて entry/exit/pnl を更新する。
        """
        # 実約定照会に必要な order_id 集合だけに絞る
        needed_order_ids: set[str] = set()
        for t in self.trade_logs:
            if t.get("action") == "BUY" and t.get("order_id"):
                needed_order_ids.add(str(t["order_id"]))
            if t.get("action") == "SELL":
                if t.get("order_id"):
                    needed_order_ids.add(str(t["order_id"]))
                if t.get("buy_order_id"):
                    needed_order_ids.add(str(t["buy_order_id"]))

        if not needed_order_ids:
            return

        orders = self.api.get_orders(product="1")
        by_id = {str(o.get("ID", "")): o for o in orders}

        exec_map: dict[str, dict[str, float]] = {}
        for oid in needed_order_ids:
            order = by_id.get(oid)
            if not order:
                continue
            px, qty, com = self._extract_exec_price_from_order(order)
            if px is None:
                continue
            exec_map[oid] = {"price": float(px), "qty": float(qty), "commission": float(com)}

        if not exec_map:
            return

        # BUY側を先に更新（SELLの entry_price 用）
        buy_order_price: dict[str, float] = {}
        buy_order_comm: dict[str, float] = {}
        for t in self.trade_logs:
            if t.get("action") != "BUY":
                continue
            oid = t.get("order_id")
            if not oid:
                continue
            oid = str(oid)
            if oid not in exec_map:
                continue
            t["price"] = exec_map[oid]["price"]
            buy_order_price[oid] = exec_map[oid]["price"]
            buy_order_comm[oid] = exec_map[oid]["commission"]

        # SELL を更新（entry/exit を実約定に差し替えて pnl を再計算）
        for t in self.trade_logs:
            if t.get("action") != "SELL":
                continue

            sell_oid = t.get("order_id")
            buy_oid = t.get("buy_order_id")
            if not sell_oid or not buy_oid:
                continue

            sell_oid = str(sell_oid)
            buy_oid = str(buy_oid)
            if sell_oid not in exec_map or buy_oid not in exec_map:
                continue

            entry_exec_px = exec_map[buy_oid]["price"]
            exit_exec_px = exec_map[sell_oid]["price"]
            qty = float(t.get("qty", 0) or 0)
            if qty <= 0:
                continue

            buy_comm = buy_order_comm.get(buy_oid, 0.0)
            sell_comm = exec_map[sell_oid].get("commission", 0.0)
            pnl = (exit_exec_px - entry_exec_px) * qty - (buy_comm + sell_comm)

            t["entry_price"] = float(entry_exec_px)
            t["exit_price"] = float(exit_exec_px)
            t["pnl"] = float(pnl)

        # 次の _print_daily_summary で total_pnl を再集計するので、
        # trade_logs の中身だけ整合していればOK

    # ─── エントリポイント ──────────────────────────────

    def run(self) -> None:
        mode_label = "[SIGNAL-ONLY] " if self.signal_only else ("[DRY-RUN] " if self.dry_run else "")
        env_label = "本番" if self.is_production else "検証"
        logger.info(
            "%s=== kabu_trader 起動 (%s環境) ===",
            mode_label, env_label,
        )
        logger.info("API URL: %s", self.api_config.base_url)
        logger.info("発注市場: %s（固定）", order_exchange_label(ORDER_EXCHANGE))
        logger.info(
            "現物買い預り区分 FundType=%s（CashMargin=1・信用新規なし）",
            self.api_config.fund_type,
        )

        # 1. 認証
        self._authenticate()

        # 1.5. 銘柄登録リセット
        self._reset_symbol_registration()

        # 2. 余力・保有確認
        self._refresh_cash()
        self._sync_positions_from_api()
        logger.info(
            "注文サイズ設定: max_position_value_pct=%.0f%% oneshot_max_yen=%s max_lot_value_yen=¥%s",
            self.max_position_value_pct * 100,
            f"¥{self.oneshot_max_yen:,.0f}" if self.oneshot_max_yen > 0 else "なし",
            f"{self.max_lot_value_yen:,.0f}",
        )
        if (self.dry_run or self.signal_only) and self.available_cash == 0:
            self.available_cash = 1_000_000
            logger.info("[仮想資金] ¥%s（シグナル判定用）", f"{self.available_cash:,.0f}")

        # 3. プレマーケット銘柄選定
        now_min = self._now_minutes()
        if now_min < MARKET_CLOSE:
            self._premarket_selection()
            self._warmup_ema()
        else:
            logger.warning("市場時間外です。翌営業日まで待機してください。")
            return

        # 4. 市場オープン待ち
        while self._now_minutes() < MORNING_BLOCK_END:
            remaining = MORNING_BLOCK_END - self._now_minutes()
            logger.info("寄り後ブロック期間中。エントリー可能まで約%d分...", remaining)
            time.sleep(min(60, remaining * 60))

        # 5. トレーディングループ
        try:
            self._trading_loop()
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt: 安全に停止...")
        except Exception:
            logger.error("予期しないエラー:\n%s", traceback.format_exc())
        finally:
            if self.positions:
                logger.warning(
                    "未決済ポジション %d 件 — 成行決済を試行します",
                    len(self.positions),
                )
                self._force_timelimit_exits("Shutdown")
            if self.positions:
                for sym, pos in self.positions.items():
                    logger.warning(
                        "  手動決済が必要: %s %s %d株 @%.1f",
                        sym, pos.name, pos.qty, pos.entry_price,
                    )

        # 6. デイリーサマリー
        self._print_daily_summary()


def _setup_utf8_file_log(
    explicit_path: str | None = None,
    *,
    default_name: str,
) -> Path:
    """UTF-8 で全INFOログをファイルへ複製（日本語が文字化けしない）"""
    LOG_DIR.mkdir(exist_ok=True)
    if explicit_path:
        log_path = Path(explicit_path)
    else:
        log_path = LOG_DIR / default_name
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(
        logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logging.getLogger().addHandler(fh)
    return log_path.resolve()


def main() -> None:
    parser = argparse.ArgumentParser(description="kabuステーションAPI ライブトレーダー")
    parser.add_argument(
        "--config", default="kabu_config.json",
        help="設定ファイルパス（デフォルト: kabu_config.json）",
    )
    parser.add_argument(
        "--production", action="store_true",
        help="本番環境 (localhost:18080) で実行",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="API呼出しなしでロジック確認（仮想資金100万円）",
    )
    parser.add_argument(
        "--signal-only", action="store_true",
        help="本番APIで板情報を取得しシグナル検出するが、発注は一切行わない",
    )
    parser.add_argument(
        "--signal-log-file",
        default=None,
        metavar="PATH",
        help="--signal-only 時のUTF-8ログファイル（省略時は kabu_logs/signal_live_日付.log）",
    )
    args = parser.parse_args()

    root_log = logging.getLogger("kabu_trader")
    if args.signal_only:
        p = _setup_utf8_file_log(
            args.signal_log_file,
            default_name=f"signal_live_{datetime.now(JST).strftime('%Y-%m-%d')}.log",
        )
        root_log.info("シグナル検出ログ（UTF-8）: %s", p)
    elif args.production and not args.dry_run:
        p = _setup_utf8_file_log(
            None,
            default_name=f"live_trading_{datetime.now(JST).strftime('%Y-%m-%d')}.log",
        )
        root_log.warning("=" * 60)
        root_log.warning("  実売買モード: 成行注文が発注されます（--production）")
        root_log.warning("  ログ（UTF-8）: %s", p)
        root_log.warning("=" * 60)

    trader = KabuTrader(
        config_path=args.config,
        dry_run=args.dry_run,
        production=args.production,
        signal_only=args.signal_only,
    )
    trader.run()


if __name__ == "__main__":
    main()
