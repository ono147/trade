import yfinance as yf
import pandas as pd
import numpy as np
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
from scipy.stats import linregress

warnings.filterwarnings('ignore')

from nikkei225_list import NIKKEI225
import os
import json
import requests
from bs4 import BeautifulSoup
import re
import time
import argparse
from dataclasses import dataclass
from zoneinfo import ZoneInfo

EARNINGS_CACHE_FILE = 'earnings_cache.json'

# 指数構成銘柄数に対する監視上位比率（旧80/225≈35.6%ではなく、デフォルトは約30%）
DEFAULT_RANK_FRACTION = 0.30
# 100株の想定購入金額上限（超える銘柄は選定除外。50万円=株価5000円超）
DEFAULT_MAX_LOT_VALUE_YEN = 500_000


@dataclass(frozen=True)
class IntradaySessionProfile:
    """
    イントラデータの index は各市場のローカル時刻（tz除去後）を想定。
    - 寄り付き直後・引け直前は新規エントリー禁止
    - time_limit 以降は強制決済（TimeLimit）
    """

    key: str
    label: str
    # 寄り付き直後のエントリー禁止 [morning_block_start, morning_block_end) 分
    morning_block_start_min: int
    morning_block_end_min: int
    # この分以降は引け前の新規エントリー禁止（当日セッション終了まで）
    close_block_start_min: int
    # この分以降は強制決済（TimeLimit）。post-market バーも想定し hour 条件を併用可
    time_limit_start_min: int
    time_limit_after_hour: int | None  # 例: JP は 15:15 以降 + hour>15 のバーでも決済


def _tod_minutes(dt) -> int:
    return int(dt.hour) * 60 + int(dt.minute)


def is_entry_blocked_by_session(dt, profile: IntradaySessionProfile) -> bool:
    m = _tod_minutes(dt)
    if profile.morning_block_start_min <= m < profile.morning_block_end_min:
        return True
    if m >= profile.close_block_start_min:
        return True
    return False


def is_time_limit_session(dt, profile: IntradaySessionProfile) -> bool:
    m = _tod_minutes(dt)
    if profile.time_limit_after_hour is not None and dt.hour > profile.time_limit_after_hour:
        return True
    if m >= profile.time_limit_start_min:
        return True
    return False


# ローカル時計・定常セッション（yfinance 5分足のタイムスタンプに合わせた目安）
SESSION_PROFILES: dict[str, IntradaySessionProfile] = {
    # 東証: 前場寄り後30分・後場終了15分前〜・TimeLimit は従来どおり 15:15〜 / 15時台以降
    "JP": IntradaySessionProfile(
        key="JP",
        label="Tokyo (TSE)",
        morning_block_start_min=9 * 60,
        morning_block_end_min=9 * 60 + 30,
        close_block_start_min=14 * 60 + 45,
        time_limit_start_min=15 * 60 + 15,
        time_limit_after_hour=15,
    ),
    # NYSE 通常: 寄り後30分・終了15分前〜、16:00 以降は強制決済
    "US": IntradaySessionProfile(
        key="US",
        label="US (NYSE regular, ET)",
        morning_block_start_min=9 * 60 + 30,
        morning_block_end_min=10 * 60,
        close_block_start_min=15 * 60 + 45,
        time_limit_start_min=16 * 60,
        time_limit_after_hour=None,
    ),
    # LSE 通常: 8:00–16:30 相当（現物）。寄り後30分・終了15分前〜、16:25 以降 TimeLimit
    "UK": IntradaySessionProfile(
        key="UK",
        label="London (LSE)",
        morning_block_start_min=8 * 60,
        morning_block_end_min=8 * 60 + 30,
        close_block_start_min=16 * 60 + 15,
        time_limit_start_min=16 * 60 + 25,
        time_limit_after_hour=None,
    ),
    # Xetra 通常: 9:00–17:30 CET
    "DE": IntradaySessionProfile(
        key="DE",
        label="Frankfurt (Xetra)",
        morning_block_start_min=9 * 60,
        morning_block_end_min=9 * 60 + 30,
        close_block_start_min=17 * 60 + 15,
        time_limit_start_min=17 * 60 + 25,
        time_limit_after_hour=None,
    ),
    # Euronext Paris 現物: 9:00–17:30 CET
    "FR": IntradaySessionProfile(
        key="FR",
        label="Paris (Euronext)",
        morning_block_start_min=9 * 60,
        morning_block_end_min=9 * 60 + 30,
        close_block_start_min=17 * 60 + 15,
        time_limit_start_min=17 * 60 + 25,
        time_limit_after_hour=None,
    ),
}


def get_session_profile(market_key: str) -> IntradaySessionProfile:
    k = (market_key or "JP").strip().upper()
    if k not in SESSION_PROFILES:
        raise ValueError(f"unknown session market key: {market_key!r}, expected one of {list(SESSION_PROFILES)}")
    return SESSION_PROFILES[k]


def compute_rank_top_n(index_constituent_count: int, rank_fraction: float, rank_top_n_override: int | None = None) -> int:
    if rank_top_n_override is not None:
        return max(1, int(rank_top_n_override))
    n = int(round(float(index_constituent_count) * float(rank_fraction)))
    return max(1, min(n, int(index_constituent_count)))


def download_with_retry(max_retries: int = 3, retry_wait_sec: float = 1.5, **download_kwargs) -> pd.DataFrame:
    """yfinance download を失敗時に最大 max_retries 回まで再試行する。"""
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            data = yf.download(**download_kwargs)
            if data is not None and not data.empty:
                return data
            last_error = RuntimeError("empty data")
        except Exception as e:
            last_error = e

        if attempt < max_retries:
            wait_sec = retry_wait_sec * attempt
            print(
                f"取得失敗のため再試行します ({attempt}/{max_retries}): "
                f"{download_kwargs.get('interval', 'n/a')} wait={wait_sec:.1f}s"
            )
            time.sleep(wait_sec)

    print(f"最終的に取得失敗: interval={download_kwargs.get('interval', 'n/a')} err={last_error}")
    return pd.DataFrame()


def extract_symbol_frame(raw_data: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """一括取得データから銘柄DataFrameを安全に取り出す。"""
    if raw_data is None or raw_data.empty:
        return pd.DataFrame()
    try:
        df = raw_data[symbol].copy().dropna()
        return df if not df.empty else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def fetch_single_symbol(symbol: str, interval: str) -> pd.DataFrame:
    """銘柄単体で再取得し、失敗時は空DataFrameを返す。"""
    single = download_with_retry(
        tickers=symbol,
        period="60d",
        interval=interval,
        auto_adjust=True,
        progress=False,
        threads=False,
    )
    if single is None or single.empty:
        return pd.DataFrame()
    return single.dropna()

def get_earnings_tickers(target_date_str: str) -> list:
    """外部サイトから当日決算発表を行う銘柄リストを自動取得しキャッシュする"""
    cache = {}
    if os.path.exists(EARNINGS_CACHE_FILE):
        with open(EARNINGS_CACHE_FILE, 'r', encoding='utf-8') as f:
            try: cache = json.load(f)
            except: pass
            
    if target_date_str in cache:
        return cache[target_date_str]
        
    date_formatted = target_date_str.replace('-', '')
    url = f"https://kabutan.jp/warning/?mode=2_1&dt={date_formatted}"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        links = soup.find_all('a', href=re.compile(r'/stock/\?code=\d{4}'))
        codes = []
        for a in links:
            match = re.search(r'code=(\d{4})', a['href'])
            if match:
                code = match.group(1) + ".T"
                if code not in codes:
                    codes.append(code)
                    
        cache[target_date_str] = codes
        with open(EARNINGS_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, indent=4)
            
        time.sleep(1) # サーバー負荷軽減
        return codes
    except Exception as e:
        print(f"決算情報の取得に失敗 ({target_date_str}): {e}")
        return []

def run_daily_selection(df: pd.DataFrame) -> float:
    """
    15分足データを用いて、直近のEMA5/EMA15のGC→DC仮想トレード成績から銘柄スコアを返す。
    - 直近最大10日分（約260本）の15分足終値で、GCで買い→DCで売りの仮想トレードを複数回実行。
    - 大きなだましGC（GC→DCで損失率が-1.0%を下回る）が1回でもあれば「失格」として -inf を返す。
    - GCが一度も発生しない場合は 0.0 を返す。
    - 失格でなければ、仮想トレードの損益率（小さな損益も含む）の合計をスコアとして返す。
    """
    # 15分足は1日約26本。最低5日分(約130本)は必要
    if len(df) < 130: return 0.0 
    
    # 直近のデータをスライス（最大260本：約10日分）
    df = df.iloc[-260:].copy()
    
    close_series = df['Close'].iloc[:, 0] if isinstance(df['Close'], pd.DataFrame) else df['Close']
    volume_series = df['Volume'].iloc[:, 0] if isinstance(df['Volume'], pd.DataFrame) else df['Volume']
    
    # 出来高フィルター: 直近1日(約26本)の出来高が、過去5日(約130本)の1日平均の0.5倍未満なら除外
    recent_vol = float(volume_series.iloc[-26:].sum())
    avg_vol_5d = float(volume_series.iloc[-156:-26].sum() / 5) if len(volume_series) >= 156 else float(volume_series.iloc[:-26].sum() / (max(1, len(volume_series[:-26]) / 26)))
    if avg_vol_5d == 0: return 0.0
    vol_ratio = recent_vol / avg_vol_5d
    if vol_ratio < 0.5:
        return 0.0
    
    # EMA計算
    ema5 = close_series.ewm(span=5, adjust=False).mean()
    ema15 = close_series.ewm(span=15, adjust=False).mean()
    
    in_position = False
    entry_price = 0.0
    total_profit_pct = 0.0
    gc_count = 0
    
    for i in range(1, len(close_series)):
        prev_ema5 = ema5.iloc[i-1]
        prev_ema15 = ema15.iloc[i-1]
        curr_ema5 = ema5.iloc[i]
        curr_ema15 = ema15.iloc[i]
        c = close_series.iloc[i]
        
        is_gc = (prev_ema5 <= prev_ema15) and (curr_ema5 > curr_ema15)
        is_dc = (prev_ema5 >= prev_ema15) and (curr_ema5 < curr_ema15)
        
        if not in_position and is_gc:
            in_position = True
            entry_price = float(c)
            gc_count += 1
            
        elif in_position and is_dc:
            in_position = False
            exit_price = float(c)
            trade_return = (exit_price - entry_price) / entry_price
            
            # 大きなだましGC（-1.0%超の損失）があれば、ランキングに乗らないよう失格扱い
            if trade_return < -0.010:
                return -np.inf
                
            total_profit_pct += trade_return
            
    # GCが1度も発生しなかった場合はスコア0
    if gc_count == 0:
        return 0.0
        
    # 現在ポジションを持ったまま（未決済のGCがある状態）の場合、現在価格での含み損益も評価
    if in_position:
        current_price = float(close_series.iloc[-1])
        unrealized_return = (current_price - entry_price) / entry_price
        
        # 含み損が-1.0%を下回る進行形のGCがあれば、だましのリスクが高いので失格
        if unrealized_return < -0.010:
            return -np.inf
            
        total_profit_pct += unrealized_return
        
    return float(total_profit_pct)

def run_all_virtual_trades(
    intra_data: dict,
    target_stocks: list,
    current_cash: float,
    target_date: str,
    volume_surge_mult: float = 1.2,
    *,
    session: IntradaySessionProfile | None = None,
    stop_loss_pct: float = 0.005,
):
    profile = session if session is not None else SESSION_PROFILES["JP"]
    intra_ready = {}
    
    for sym_code, _, _ in target_stocks:
        i_df = intra_data.get(sym_code, pd.DataFrame())
        if i_df.empty: continue
        
        # DataFrame.index は datetime 型なので、文字列での loc が可能かチェック
        dates_in_df = np.unique(i_df.index.strftime('%Y-%m-%d'))
        if target_date not in dates_in_df:
            continue
            
        close_series = i_df['Close'].iloc[:, 0] if isinstance(i_df['Close'], pd.DataFrame) else i_df['Close']
        volume_series = i_df['Volume'].iloc[:, 0] if isinstance(i_df['Volume'], pd.DataFrame) else i_df['Volume']
        i_df['ema5'] = close_series.ewm(span=5, adjust=False).mean()
        i_df['ema15'] = close_series.ewm(span=15, adjust=False).mean()
        # 20本移動平均出来高（出来高急増の基準）
        i_df['vol_ma20'] = volume_series.rolling(window=20, min_periods=5).mean()
        
        target_df = i_df.loc[target_date].copy()
        if isinstance(target_df, pd.Series): target_df = target_df.to_frame().T
        if len(target_df) < 10: continue  # 1分足は最低10本以上あればEMA計算可能
        intra_ready[sym_code] = target_df
        
    if not intra_ready:
        return 0.0, []
        
    all_timestamps = set()
    for df in intra_ready.values():
        all_timestamps.update(df.index)
    all_timestamps = sorted(list(all_timestamps))
    
    daily_profit = 0.0
    available_cash = current_cash
    positions = {}
    trade_logs = []
    
    for dt in all_timestamps:
        is_time_limit = is_time_limit_session(dt, profile)

        # 決済処理
        for sym_code, _, _ in target_stocks:
            if sym_code not in positions or sym_code not in intra_ready: continue
            df = intra_ready[sym_code]
            if dt not in df.index: continue
            
            idx = df.index.get_loc(dt)
            if idx == 0: continue
            
            c = df['Close'].iloc[idx]
            if isinstance(c, pd.Series): c = c.iloc[0]
            
            prev_ema5 = df['ema5'].iloc[idx-1]
            prev_ema15 = df['ema15'].iloc[idx-1]
            curr_ema5 = df['ema5'].iloc[idx]
            curr_ema15 = df['ema15'].iloc[idx]
            
            is_dead_cross = (prev_ema5 >= prev_ema15) and (curr_ema5 < curr_ema15)
            
            entry_p = positions[sym_code]['entry_price']
            is_stop_loss = c <= entry_p * (1.0 - stop_loss_pct)
            
            if is_dead_cross or is_time_limit or is_stop_loss:
                qty = positions[sym_code]['size']
                entry_dt = positions[sym_code]['entry_dt']
                pnl = (c - entry_p) * qty
                daily_profit += pnl
                available_cash += (qty * c)
                
                reason = 'StopLoss' if is_stop_loss else ('TimeLimit' if is_time_limit else 'DeadCross')
                
                trade_logs.append({
                    'entry_time': entry_dt.strftime('%H:%M:%S'),
                    'exit_time': dt.strftime('%H:%M:%S'),
                    'symbol': sym_code,
                    'qty': qty,
                    'entry_price': entry_p,
                    'exit_price': c,
                    'pnl': pnl,
                    'reason': reason
                })
                
                del positions[sym_code]
                
        # エントリー処理
        for sym_code, _, _ in target_stocks:
            if sym_code in positions or sym_code not in intra_ready: continue
            if is_time_limit: continue

            if is_entry_blocked_by_session(dt, profile):
                continue

            df = intra_ready[sym_code]
            if dt not in df.index: continue
            
            idx = df.index.get_loc(dt)
            if idx == 0: continue
            
            c = df['Close'].iloc[idx]
            if isinstance(c, pd.Series): c = c.iloc[0]
            
            prev_ema5 = df['ema5'].iloc[idx-1]
            prev_ema15 = df['ema15'].iloc[idx-1]
            curr_ema5 = df['ema5'].iloc[idx]
            curr_ema15 = df['ema15'].iloc[idx]
            
            is_golden_cross = (prev_ema5 <= prev_ema15) and (curr_ema5 > curr_ema15)
            
            # 出来高急増チェック: 直近バーの出来高が20本平均の volume_surge_mult 倍以上
            curr_vol = df['Volume'].iloc[idx]
            if isinstance(curr_vol, pd.Series): curr_vol = curr_vol.iloc[0]
            vol_ma = df['vol_ma20'].iloc[idx]
            if isinstance(vol_ma, pd.Series): vol_ma = vol_ma.iloc[0]
            is_volume_surge = pd.notna(vol_ma) and vol_ma > 0 and (
                float(curr_vol) >= float(vol_ma) * volume_surge_mult
            )
            
            if is_golden_cross and is_volume_surge:
                qty = int(available_cash // (c * 100)) * 100
                if qty >= 100:
                    positions[sym_code] = {'size': qty, 'entry_price': c, 'entry_dt': dt}
                    available_cash -= (qty * c)
                    
    return daily_profit, trade_logs


def load_market_data(
    period: str = "60d",
    warmup_skip: int = 10,
    evaluation_trading_days: int | None = None,
):
    """
    period: yfinance の取得期間（例: 60d, 90d）
    warmup_skip: 先頭のいくつの取引日をバックテスト評価から除外するか（銘柄選定の履歴バー用）
    evaluation_trading_days: 指定時、評価対象の取引日を末尾からこの日数に切り詰める
    """
    data_15m, intra_data = {}, {}
    symbols = [s[0] for s in NIKKEI225]
    print("データを一括取得中 (15分足)...")
    data_15m_raw = download_with_retry(
        tickers=" ".join(symbols),
        period=period,
        interval="15m",
        group_by="ticker",
        auto_adjust=True,
        progress=False,
        threads=True,
    )
    print("データを一括取得中 (5分足)...")
    intra_data_raw = download_with_retry(
        tickers=" ".join(symbols),
        period=period,
        interval="5m",
        group_by="ticker",
        auto_adjust=True,
        progress=False,
        threads=True,
    )

    recovered_15m = 0
    recovered_5m = 0
    failed_15m = []
    failed_5m = []

    for sym in symbols:
        d15 = extract_symbol_frame(data_15m_raw, sym)
        if d15.empty:
            d15 = fetch_single_symbol(sym, "15m")
            if not d15.empty:
                recovered_15m += 1
            else:
                failed_15m.append(sym)
        if not d15.empty and d15.index.tz is not None:
            d15.index = d15.index.tz_convert('Asia/Tokyo').tz_localize(None)
        data_15m[sym] = d15

        d5 = extract_symbol_frame(intra_data_raw, sym)
        if d5.empty:
            d5 = fetch_single_symbol(sym, "5m")
            if not d5.empty:
                recovered_5m += 1
            else:
                failed_5m.append(sym)
        if not d5.empty and d5.index.tz is not None:
            d5.index = d5.index.tz_convert('Asia/Tokyo').tz_localize(None)
        intra_data[sym] = d5

    print(f"個別再取得: 15分足 {recovered_15m} 銘柄回復 / 5分足 {recovered_5m} 銘柄回復")
    print(f"最終取得失敗: 15分足 {len(failed_15m)} 銘柄 / 5分足 {len(failed_5m)} 銘柄")

    all_dates = set()
    for df in data_15m.values():
        if not df.empty:
            all_dates.update(df.index.strftime('%Y-%m-%d'))
    common_dates = sorted(list(all_dates))
    skip = max(0, int(warmup_skip))
    test_dates = common_dates[skip:] if len(common_dates) > skip else []
    if evaluation_trading_days is not None and test_dates:
        n = max(1, int(evaluation_trading_days))
        test_dates = test_dates[-n:]
    return data_15m, intra_data, test_dates


def run_full_backtest(
    data_15m: dict,
    intra_data: dict,
    test_dates: list,
    volume_surge_mult: float,
    print_daily: bool = True,
    *,
    session: IntradaySessionProfile | None = None,
    rank_fraction: float | None = None,
    index_constituent_count: int | None = None,
    rank_top_n: int | None = None,
    return_daily_breakdown: bool = False,
    max_symbols: int | None = None,
    stop_loss_pct: float = 0.005,
):
    INITIAL_CASH = 1_000_000
    current_cash = INITIAL_CASH
    total_profit = 0.0
    all_trade_logs = []
    daily_breakdown: list[dict] = []
    idx_n = int(index_constituent_count) if index_constituent_count is not None else len(NIKKEI225)
    rf = DEFAULT_RANK_FRACTION if rank_fraction is None else float(rank_fraction)
    top_n = compute_rank_top_n(idx_n, rf, rank_top_n)
    if max_symbols is not None:
        top_n = min(top_n, max_symbols)

    if print_daily:
        prof = session if session is not None else SESSION_PROFILES["JP"]
        print(
            f"\n[Step 2] 実運用シミュレーションを開始... session={prof.key} ({prof.label}) | "
            f"上位{top_n}銘柄（指数{idx_n}銘柄 × 比率{rf:.2%}）"
        )
    for target_date in test_dates:
        earnings_today = get_earnings_tickers(target_date)
        symbol_scores = []
        for sym_code, sym_name in NIKKEI225:
            if sym_code in earnings_today:
                continue

            d_df = data_15m.get(sym_code, pd.DataFrame())
            if d_df.empty: continue

            d_df_prev = d_df[d_df.index.strftime('%Y-%m-%d') < target_date]
            if len(d_df_prev) < 130: continue

            last_close = d_df_prev['Close'].iloc[-1]
            if isinstance(last_close, pd.Series): last_close = last_close.iloc[0]
            if last_close * 100 > DEFAULT_MAX_LOT_VALUE_YEN:
                continue

            score = run_daily_selection(d_df_prev)
            symbol_scores.append((sym_code, score, sym_name))

        symbol_scores.sort(key=lambda x: x[1], reverse=True)
        target_stocks = symbol_scores[: min(top_n, len(symbol_scores))]

        daily_profit, daily_logs = run_all_virtual_trades(
            intra_data, target_stocks, current_cash, target_date, volume_surge_mult,
            session=session, stop_loss_pct=stop_loss_pct,
        )
        current_cash += daily_profit
        total_profit += daily_profit

        symbol_map = {s[0]: s[2] for s in target_stocks}
        for log in daily_logs:
            log['date'] = target_date
            log['name'] = symbol_map.get(log['symbol'], log['symbol'])
            all_trade_logs.append(log)

        if print_daily:
            t_names = ", ".join([s[2] for s in target_stocks])
            print(f"[{target_date}] 損益: {daily_profit:+7,.0f} 円 | 資金: {current_cash:>10,.0f} 円 | 対象: {t_names}")

        if return_daily_breakdown:
            daily_breakdown.append({"date": target_date, "daily_pnl": float(daily_profit), "cash_after": float(current_cash)})

    if return_daily_breakdown:
        return total_profit, current_cash, all_trade_logs, daily_breakdown
    return total_profit, current_cash, all_trade_logs


def main():
    parser = argparse.ArgumentParser(description="NIKKEI225 リアルシミュレーション")
    parser.add_argument(
        "--volume-mult",
        type=float,
        default=1.2,
        help="出来高が20本平均の何倍以上でエントリーするか（既定 1.2）",
    )
    parser.add_argument(
        "--sweep-vol",
        action="store_true",
        help="1.1, 1.15, 1.2, 1.25 の4通りで連続検証（データ取得は1回のみ）",
    )
    parser.add_argument(
        "--session",
        type=str,
        default="JP",
        help="取引セッション: JP / US / UK / DE / FR（イントラのローカル時刻に合わせた寄り引け・TimeLimit）",
    )
    parser.add_argument(
        "--rank-fraction",
        type=float,
        default=None,
        help=f"指数構成銘柄数に対する監視上位の割合（既定 {DEFAULT_RANK_FRACTION:.2f}）",
    )
    parser.add_argument(
        "--rank-top-n",
        type=int,
        default=None,
        help="監視銘柄数を直接指定（指定時は rank-fraction より優先）",
    )
    parser.add_argument(
        "--index-size",
        type=int,
        default=None,
        help="指数の構成銘柄数（既定: 日経225=225）。上位銘柄数 = round(index_size * rank_fraction)",
    )
    parser.add_argument(
        "--yf-period",
        type=str,
        default="59d",
        help="yfinance period（15m/5m は約60日上限のため 59d 推奨）",
    )
    parser.add_argument(
        "--warmup-skip",
        type=int,
        default=10,
        help="評価開始までスキップする先頭取引日数（銘柄選定の履歴用）",
    )
    parser.add_argument(
        "--evaluation-trading-days",
        type=int,
        default=None,
        help="末尾からこの営業日数だけ評価（未指定なら warmup 以降すべて）",
    )
    parser.add_argument(
        "--single-date",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help="この取引日1日だけをバックテスト（--today と併用不可で --today が優先）",
    )
    parser.add_argument(
        "--today",
        action="store_true",
        help="Asia/Tokyo のカレンダー日付を「今日」として1日だけ評価",
    )
    parser.add_argument(
        "--max-symbols",
        type=int,
        default=None,
        help="監視銘柄数の上限（ライブの kabu API 上限に合わせる場合は 135 など）",
    )
    args = parser.parse_args()

    INITIAL_CASH = 1_000_000
    sess = get_session_profile(args.session)
    idx_n = int(args.index_size) if args.index_size is not None else len(NIKKEI225)
    rf = DEFAULT_RANK_FRACTION if args.rank_fraction is None else float(args.rank_fraction)
    top_n = compute_rank_top_n(idx_n, rf, args.rank_top_n)
    print("=" * 70)
    print(
        f"単元株制約・値嵩株除外・だましGC排除(15分損失-1.0%まで)・上位{top_n}銘柄監視 リアルシミュレーション"
    )
    print("=" * 70)

    data_15m, intra_data, test_dates = load_market_data(
        period=args.yf_period,
        warmup_skip=args.warmup_skip,
        evaluation_trading_days=args.evaluation_trading_days,
    )

    single = None
    if args.today:
        single = datetime.datetime.now(ZoneInfo("Asia/Tokyo")).strftime("%Y-%m-%d")
    elif args.single_date:
        single = args.single_date.strip()
    if single:
        if single not in test_dates:
            tail = test_dates[-10:] if len(test_dates) >= 10 else test_dates
            raise SystemExit(
                f"指定日 {single} は評価可能な取引日に含まれていません（休場・未取得・warmup 除外など）。\n"
                f"直近の評価可能日の例: {tail}"
            )
        test_dates = [single]
        print(f"\n[単一日評価] {single} のみ\n")

    bt_kwargs = dict(
        session=sess,
        rank_fraction=rf,
        index_constituent_count=idx_n,
        rank_top_n=args.rank_top_n,
        max_symbols=args.max_symbols,
    )

    if args.sweep_vol:
        sweep_mults = [1.1, 1.15, 1.2, 1.25]
        print(f"\n[出来高倍率スイープ] {sweep_mults} （データ取得1回・日次ログ省略）\n")
        rows = []
        for m in sweep_mults:
            tp, fc, logs = run_full_backtest(data_15m, intra_data, test_dates, m, print_daily=False, **bt_kwargs)
            n_trades = len(logs)
            ret_pct = (fc - INITIAL_CASH) / INITIAL_CASH * 100.0
            rows.append((m, tp, fc, n_trades, ret_pct))
            print(f"  出来高×{m:.2f} 完了 | 合計損益 {tp:+,.0f} 円 | 最終資金 {fc:,.0f} 円 | 約定数 {n_trades}")

        print("\n" + "=" * 70)
        print(" 出来高条件 比較サマリー（同一データ・同一期間）")
        print("=" * 70)
        print(f"  {'出来高倍率':>10} | {'合計損益(円)':>14} | {'最終資金(円)':>16} | {'約定数':>8} | {'リターン%':>10}")
        print("  " + "-" * 66)
        for m, tp, fc, n_trades, ret_pct in rows:
            print(f"  {m:>10.2f} | {tp:>+14,.0f} | {fc:>16,.0f} | {n_trades:>8} | {ret_pct:>+9.2f}%")
        print("=" * 70)
        return

    vm = args.volume_mult
    print(f"\n出来高条件: 20本平均の {vm} 倍以上でエントリー\n")

    total_profit, current_cash, all_trade_logs = run_full_backtest(
        data_15m, intra_data, test_dates, vm, print_daily=True, **bt_kwargs
    )

    print("\n======================================================================")
    print(" 詳細な売買履歴")
    print("======================================================================")
    for log in all_trade_logs:
        pnl = log['pnl']
        sign = "+" if pnl > 0 else ""
        print(f"{log['date']} {log['entry_time']} -> {log['exit_time']} | "
              f"{log['name'][:8]:<8} | {log['qty']:>5}株 | "
              f"買 {log['entry_price']:>6.1f} -> 売 {log['exit_price']:>6.1f} | "
              f"損益: {sign}{pnl:>7,.0f} 円 ({log['reason']})")

    print("\n======================================================================")
    print(f"  合計損益: {total_profit:+10,.0f} 円")
    print(f"  最終資金: {current_cash:,.0f} 円")
    print("======================================================================")


if __name__ == '__main__':
    main()
