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

EARNINGS_CACHE_FILE = 'earnings_cache.json'


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
):
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
        is_time_limit = (dt.hour == 15 and dt.minute >= 15) or (dt.hour > 15)
        
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
            is_stop_loss = c <= entry_p * 0.995
            
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
            
            # 朝イチ(9:00〜9:30)と引け間際(14:45以降)の新規エントリーは見送る
            if dt.hour == 9 and dt.minute < 30: continue
            if (dt.hour == 14 and dt.minute >= 45) or dt.hour >= 15: continue
            
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


def load_market_data():
    data_15m, intra_data = {}, {}
    symbols = [s[0] for s in NIKKEI225]
    print("データを一括取得中 (15分足)...")
    data_15m_raw = download_with_retry(
        tickers=" ".join(symbols),
        period="60d",
        interval="15m",
        group_by="ticker",
        auto_adjust=True,
        progress=False,
        threads=True,
    )
    print("データを一括取得中 (5分足)...")
    intra_data_raw = download_with_retry(
        tickers=" ".join(symbols),
        period="60d",
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
    test_dates = common_dates[10:] if len(common_dates) > 10 else []
    return data_15m, intra_data, test_dates


def run_full_backtest(data_15m: dict, intra_data: dict, test_dates: list, volume_surge_mult: float, print_daily: bool = True):
    INITIAL_CASH = 1_000_000
    current_cash = INITIAL_CASH
    total_profit = 0.0
    all_trade_logs = []

    if print_daily:
        print("\n[Step 2] 実運用シミュレーションを開始...")
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
            if last_close >= 9000:
                continue

            score = run_daily_selection(d_df_prev)
            symbol_scores.append((sym_code, score, sym_name))

        symbol_scores.sort(key=lambda x: x[1], reverse=True)
        target_stocks = symbol_scores[:80]

        daily_profit, daily_logs = run_all_virtual_trades(
            intra_data, target_stocks, current_cash, target_date, volume_surge_mult
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
    args = parser.parse_args()

    INITIAL_CASH = 1_000_000
    print("=" * 70)
    print("💰 単元株制約・値嵩株除外・だましGC排除(15分損失-1.0%まで)・80銘柄監視 リアルシミュレーション")
    print("=" * 70)

    data_15m, intra_data, test_dates = load_market_data()

    if args.sweep_vol:
        sweep_mults = [1.1, 1.15, 1.2, 1.25]
        print(f"\n[出来高倍率スイープ] {sweep_mults} （データ取得1回・日次ログ省略）\n")
        rows = []
        for m in sweep_mults:
            tp, fc, logs = run_full_backtest(data_15m, intra_data, test_dates, m, print_daily=False)
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
        data_15m, intra_data, test_dates, vm, print_daily=True
    )

    print("\n======================================================================")
    print(" 📜 詳細な売買履歴")
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
