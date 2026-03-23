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

EARNINGS_CACHE_FILE = 'earnings_cache.json'

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
    15分足データを用いて、直近で「GC後のだまし（損失を出すGC）」が起きていないかを評価する。
    - 過去10日分（約260本）の15分足の終値を用いて、EMA5とEMA15のGCからDCまでの仮想トレードを実行。
    - 1度でも損失（決済価格 < 買値）を出しただましGCがあれば、スコアを0.0にする。
    - すべての仮想トレードが利益で終わった銘柄のみ、その利益率の合計をスコアとして返す。
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
    dc_count = 0
    
    for i in range(1, len(close_series)):
        prev_ema5 = ema5.iloc[i-1]
        prev_ema15 = ema15.iloc[i-1]
        curr_ema5 = ema5.iloc[i]
        curr_ema15 = ema15.iloc[i]
        c = close_series.iloc[i]
        
        is_gc = (prev_ema5 <= prev_ema15) and (curr_ema5 > curr_ema15)
        is_dc = (prev_ema5 >= prev_ema15) and (curr_ema5 < curr_ema15)
        
        if not in_position and is_dc:
            in_position = True
            entry_price = float(c)
            dc_count += 1
            
        elif in_position and is_gc:
            in_position = False
            exit_price = float(c)
            trade_return = (entry_price - exit_price) / entry_price
            
            # 大きく損失を出した「だましGC」（-1.0%より大きい下落率）があれば即座に失格とする
            # 許容度を緩和(-0.5% -> -1.0%)して監視対象を増やし、取引回数を増やす
            if trade_return < -0.010:
                return 0.0
                
            total_profit_pct += trade_return
            
    # GCが1度も発生しなかった場合はスコア0
    if dc_count == 0:
        return 0.0
        
    # 現在ポジションを持ったまま（未決済のGCがある状態）の場合、現在価格での含み損益も評価
    if in_position:
        current_price = float(close_series.iloc[-1])
        unrealized_return = (entry_price - current_price) / entry_price
        
        # 含み損が-1.0%を下回る進行形のGCがあれば、だましのリスクが高いので失格
        if unrealized_return < -0.010:
            return 0.0
            
        total_profit_pct += unrealized_return
        
    return float(total_profit_pct)

def run_all_virtual_trades(intra_data: dict, target_stocks: list, current_cash: float, target_date: str):
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
            
            is_golden_cross = (prev_ema5 <= prev_ema15) and (curr_ema5 > curr_ema15)
            
            entry_p = positions[sym_code]['entry_price']
            is_stop_loss = c >= entry_p * 1.005
            
            if is_golden_cross or is_time_limit or is_stop_loss:
                qty = positions[sym_code]['size']
                entry_dt = positions[sym_code]['entry_dt']
                pnl = (entry_p - c) * qty
                daily_profit += pnl
                available_cash += (qty * entry_p) + pnl
                
                reason = 'StopLoss' if is_stop_loss else ('TimeLimit' if is_time_limit else 'GoldenCross')
                
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
            
            is_dead_cross = (prev_ema5 >= prev_ema15) and (curr_ema5 < curr_ema15)
            
            # 出来高急増チェック: 直近バーの出来高が20本平均の1.2倍以上
            curr_vol = df['Volume'].iloc[idx]
            if isinstance(curr_vol, pd.Series): curr_vol = curr_vol.iloc[0]
            vol_ma = df['vol_ma20'].iloc[idx]
            if isinstance(vol_ma, pd.Series): vol_ma = vol_ma.iloc[0]
            is_volume_surge = pd.notna(vol_ma) and vol_ma > 0 and (float(curr_vol) >= float(vol_ma) * 1.2)
            
            if is_dead_cross and is_volume_surge:
                qty = int(available_cash // (c * 100)) * 100
                if qty >= 100:
                    positions[sym_code] = {'size': qty, 'entry_price': c, 'entry_dt': dt}
                    available_cash -= (qty * c)
                    
    return daily_profit, trade_logs


def main():
    INITIAL_CASH = 1_000_000
    print("=" * 70)
    print("📉 ショート専用戦略：ダマシDC排除(15分損失-1.0%迄)・出来高1.2倍・80銘柄監視")
    print("=" * 70)

    data_15m, intra_data = {}, {}
    symbols = [s[0] for s in NIKKEI225]
    print("データを一括取得中 (15分足)...")
    data_15m_raw = yf.download(" ".join(symbols), period="60d", interval="15m", group_by="ticker", auto_adjust=True, progress=False, threads=True)
    print("データを一括取得中 (5分足)...")
    intra_data_raw = yf.download(" ".join(symbols), period="60d", interval="5m", group_by="ticker", auto_adjust=True, progress=False, threads=True)
    
    for sym in symbols:
        try:
            d15 = data_15m_raw[sym].copy().dropna()
            if d15.index.tz is not None: d15.index = d15.index.tz_convert('Asia/Tokyo').tz_localize(None)
            data_15m[sym] = d15
        except: data_15m[sym] = pd.DataFrame()
        
        try:
            d5 = intra_data_raw[sym].copy().dropna()
            if d5.index.tz is not None: d5.index = d5.index.tz_convert('Asia/Tokyo').tz_localize(None)
            intra_data[sym] = d5
        except: intra_data[sym] = pd.DataFrame()

    # 共通のテスト対象日を特定
    all_dates = set()
    for df in data_15m.values():
        if not df.empty:
            all_dates.update(df.index.strftime('%Y-%m-%d'))
    common_dates = sorted(list(all_dates))
            
    test_dates = common_dates[10:] if len(common_dates) > 10 else [] # 最初の10日は判定ウォームアップ用
    
    current_cash = INITIAL_CASH
    total_profit = 0
    results = []
    all_trade_logs = []
    
    print("\n[Step 2] 実運用シミュレーションを開始...")
    for target_date in test_dates:
        # 当日以前のデータで戦略適用 (当日の終値は見ないように、target_date未満でスライス)
        earnings_today = get_earnings_tickers(target_date)
        symbol_scores = []
        for sym_code, sym_name in NIKKEI225:
            # 決算日フィルター: 当日（target_date）がその銘柄の決算予定日ならスキップ（アプローチ2）
            if sym_code in earnings_today:
                continue
                
            d_df = data_15m.get(sym_code, pd.DataFrame())
            if d_df.empty: continue
            
            # 日付文字列での比較でtarget_date未満のデータを取得
            d_df_prev = d_df[d_df.index.strftime('%Y-%m-%d') < target_date]
            if len(d_df_prev) < 130: continue
            
            last_close = d_df_prev['Close'].iloc[-1]
            if isinstance(last_close, pd.Series): last_close = last_close.iloc[0]
            if last_close >= 9000:
                continue
                
            score = run_daily_selection(d_df_prev)
            symbol_scores.append((sym_code, score, sym_name))
            
        symbol_scores.sort(key=lambda x: x[1], reverse=True) # 降順（上昇率が高い順）
        target_stocks = symbol_scores  # 上位80銘柄を監視（45から緩和）
        
        daily_profit, daily_logs = run_all_virtual_trades(intra_data, target_stocks, current_cash, target_date)
        current_cash += daily_profit
        total_profit += daily_profit
        
        symbol_map = {s[0]: s[2] for s in target_stocks}
        for log in daily_logs:
            log['date'] = target_date
            log['name'] = symbol_map.get(log['symbol'], log['symbol'])
            all_trade_logs.append(log)
        
        t_names = ", ".join([s[2] for s in target_stocks])
        print(f"[{target_date}] 損益: {daily_profit:+7,.0f} 円 | 資金: {current_cash:>10,.0f} 円 | 対象: {t_names}")

    print("\n======================================================================")
    print(" 📜 詳細な売買履歴")
    print("======================================================================")
    for log in all_trade_logs:
        pnl = log['pnl']
        sign = "+" if pnl > 0 else ""
        print(f"{log['date']} {log['entry_time']} -> {log['exit_time']} | "
              f"{log['name'][:8]:<8} | {log['qty']:>5}株 | "
              f"売 {log['entry_price']:>6.1f} -> 買戻 {log['exit_price']:>6.1f} | "
              f"損益: {sign}{pnl:>7,.0f} 円 ({log['reason']})")
        
    print("\n======================================================================")
    print(f"  合計損益: {total_profit:+10,.0f} 円")
    print(f"  最終資金: {current_cash:,.0f} 円")
    print("======================================================================")

if __name__ == '__main__':
    main()
