"""
kabuステーションAPI クライアント
三菱UFJ eスマート証券 REST API (localhost) のラッパー。
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any

import requests

logger = logging.getLogger(__name__)

_RATE_LIMIT_ORDER = 4
_RATE_LIMIT_INFO = 3
_RETRY_429_MAX = 3
_RETRY_429_WAIT = 1.0

# 現物・信用の新規発注で指定可能な市場コード（東証=1 は新規不可）
EXCHANGE_SOR = 9
EXCHANGE_TOUSHOU_PLUS = 27
ORDER_EXCHANGE = EXCHANGE_SOR  # 売買発注は常に SOR


def order_exchange_label(exchange: int = ORDER_EXCHANGE) -> str:
    labels = {EXCHANGE_SOR: "SOR(9)", EXCHANGE_TOUSHOU_PLUS: "東証+(27)", 1: "東証(1)"}
    return labels.get(exchange, str(exchange))


@dataclass
class KabuApiConfig:
    api_password: str
    trade_password: str
    base_url: str = "http://localhost:18081/kabusapi"
    account_type: int = 4  # 特定口座
    exchange: int = EXCHANGE_SOR  # 参照用（発注は ORDER_EXCHANGE を使用）
    # 現物買いの預り区分: "02"=保護 / "AA"=信用代用（信用口座あり時は AA が必要。CashMargin=1 のまま現物）
    fund_type: str = "AA"
    timeout_sec: float = 10.0


class KabuApiError(Exception):
    def __init__(self, status_code: int, body: Any):
        self.status_code = status_code
        self.body = body
        super().__init__(f"HTTP {status_code}: {body}")


class KabuAPI:
    """kabuステーション REST API クライアント"""

    def __init__(self, config: KabuApiConfig):
        self.cfg = config
        self._token: str | None = None
        self._session = requests.Session()
        self._session.headers["Content-Type"] = "application/json"
        self._last_order_ts: float = 0.0
        self._last_info_ts: float = 0.0

    @property
    def token(self) -> str:
        if self._token is None:
            raise RuntimeError("トークン未取得。authenticate() を先に呼んでください。")
        return self._token

    def authenticate(self) -> str:
        url = f"{self.cfg.base_url}/token"
        body = {"APIPassword": self.cfg.api_password}
        resp = self._session.post(url, data=json.dumps(body), timeout=self.cfg.timeout_sec)
        if resp.status_code != 200:
            raise KabuApiError(resp.status_code, resp.text)
        data = resp.json()
        self._token = data["Token"]
        self._session.headers["X-API-KEY"] = self._token
        logger.info("認証成功 token=%s...", self._token[:8])
        return self._token

    def _throttle_order(self) -> None:
        elapsed = time.monotonic() - self._last_order_ts
        interval = 1.0 / _RATE_LIMIT_ORDER
        if elapsed < interval:
            time.sleep(interval - elapsed)
        self._last_order_ts = time.monotonic()

    def _throttle_info(self) -> None:
        elapsed = time.monotonic() - self._last_info_ts
        interval = 1.0 / _RATE_LIMIT_INFO
        if elapsed < interval:
            time.sleep(interval - elapsed)
        self._last_info_ts = time.monotonic()

    def _get(self, path: str, params: dict | None = None) -> Any:
        url = f"{self.cfg.base_url}{path}"
        for attempt in range(1, _RETRY_429_MAX + 1):
            self._throttle_info()
            resp = self._session.get(url, params=params, timeout=self.cfg.timeout_sec)
            if resp.status_code == 429 and attempt < _RETRY_429_MAX:
                time.sleep(_RETRY_429_WAIT * attempt)
                continue
            if resp.status_code != 200:
                raise KabuApiError(resp.status_code, resp.text)
            return resp.json()

    def _post(self, path: str, body: dict, throttle: str = "info") -> Any:
        if throttle == "order":
            self._throttle_order()
        else:
            self._throttle_info()
        url = f"{self.cfg.base_url}{path}"
        resp = self._session.post(url, data=json.dumps(body), timeout=self.cfg.timeout_sec)
        if resp.status_code != 200:
            raise KabuApiError(resp.status_code, resp.text)
        return resp.json()

    def _put(self, path: str, body: dict, throttle: str = "info") -> Any:
        if throttle == "order":
            self._throttle_order()
        else:
            self._throttle_info()
        url = f"{self.cfg.base_url}{path}"
        resp = self._session.put(url, data=json.dumps(body), timeout=self.cfg.timeout_sec)
        if resp.status_code != 200:
            raise KabuApiError(resp.status_code, resp.text)
        return resp.json()

    # ─── 残高・余力 ─────────────────────────────────
    def get_wallet_cash(self) -> dict:
        return self._get("/wallet/cash")

    def get_positions(self, product: str = "1") -> list[dict]:
        """product: 0=すべて, 1=現物, 2=信用"""
        return self._get("/positions", params={"product": product, "addinfo": "true"})

    # ─── 板情報 ─────────────────────────────────────
    def get_board(self, symbol_code: str, exchange: int = 1) -> dict:
        """
        symbol_code: 銘柄コード (例: '6857')
        exchange: 板情報取得時の市場コード (1=東証)
        戻り値: BoardSuccess スキーマ
        """
        sym = f"{symbol_code}@{exchange}"
        return self._get(f"/board/{sym}")

    # ─── 注文 ───────────────────────────────────────
    def send_buy_order(
        self,
        symbol: str,
        qty: int,
        *,
        order_type: int = 10,
        price: float = 0,
    ) -> dict:
        """
        現物買い注文（成行デフォルト）。
        order_type: 10=成行, 20=指値
        CashMargin=1（現物）。信用新規は行わない。
        FundType は cfg.fund_type（信用口座あり時は通常 "AA"）。
        """
        fund_type = self.cfg.fund_type or "AA"
        body = {
            "Password": self.cfg.trade_password,
            "Symbol": symbol,
            "Exchange": ORDER_EXCHANGE,
            "SecurityType": 1,
            "Side": "2",
            "CashMargin": 1,
            "DelivType": 2,
            "FundType": fund_type,
            "AccountType": self.cfg.account_type,
            "Qty": qty,
            "FrontOrderType": order_type,
            "Price": price,
            "ExpireDay": 0,
        }
        logger.info(
            "買い発注リクエスト: %s %d株 Exchange=%s FundType=%s (現物)",
            symbol, qty, order_exchange_label(ORDER_EXCHANGE), fund_type,
        )
        return self._post("/sendorder", body, throttle="order")

    def send_sell_order(
        self,
        symbol: str,
        qty: int,
        *,
        order_type: int = 10,
        price: float = 0,
    ) -> dict:
        """現物売り注文（成行デフォルト）。"""
        body = {
            "Password": self.cfg.trade_password,
            "Symbol": symbol,
            "Exchange": ORDER_EXCHANGE,
            "SecurityType": 1,
            "Side": "1",
            "CashMargin": 1,
            "DelivType": 0,
            "FundType": "  ",
            "AccountType": self.cfg.account_type,
            "Qty": qty,
            "FrontOrderType": order_type,
            "Price": price,
            "ExpireDay": 0,
        }
        logger.info(
            "売り発注リクエスト: %s %d株 Exchange=%s",
            symbol, qty, order_exchange_label(ORDER_EXCHANGE),
        )
        return self._post("/sendorder", body, throttle="order")

    def cancel_order(self, order_id: str) -> dict:
        body = {
            "OrderId": order_id,
            "Password": self.cfg.trade_password,
        }
        return self._put("/cancelorder", body, throttle="order")

    # ─── 注文照会 ────────────────────────────────────
    def get_orders(self, product: str = "1", state: str | None = None) -> list[dict]:
        params: dict[str, str] = {"product": product}
        if state is not None:
            params["state"] = state
        return self._get("/orders", params=params)

    # ─── 銘柄情報 ────────────────────────────────────
    def get_symbol_info(self, symbol_code: str, exchange: int = 1) -> dict:
        sym = f"{symbol_code}@{exchange}"
        return self._get(f"/symbol/{sym}")

    # ─── 銘柄登録（PUSH用） ─────────────────────────
    def register_symbols(self, symbols: list[dict]) -> dict:
        """
        symbols: [{"Symbol": "6857", "Exchange": 1}, ...]  (最大50銘柄)
        """
        body = {"Symbols": symbols}
        return self._put("/register", body)

    def unregister_all(self) -> dict:
        return self._put("/unregister/all", {})

    # ─── API制限情報 ──────────────────────────────────
    def get_api_soft_limit(self) -> dict:
        return self._get("/apisoftlimit")
