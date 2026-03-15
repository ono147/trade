import yfinance as yf
import pandas as pd
import numpy as np
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings

warnings.filterwarnings('ignore')

from nikkei225_list import NIKKEI225

def run_daily_selection(df: pd.DataFrame) -> float:
    if len(df) < 25: return 0.0
    df = df.copy()
    close_series = df['Close'].iloc[:, 0] if isinstance(df['Close'], pd.DataFrame) else df['Close']
    df['sma5'] = close_series.rolling(window=5).mean()
    df['sma20'] = close_series.rolling(window=20).mean()
    df.dropna(inplace=True)
    if df.empty: return 0.0
    
    df['signal'] = np.where(df['sma5'] > df['sma20'], 1, -1)
    df['position'] = df['signal'].shift(1)
    df['returns'] = close_series.pct_change()
    df['strategy_returns'] = df['position'] * df['returns']
    total_return = np.exp(np.log1p(df['strategy_returns'].dropna()).sum()) - 1
    return total_return

def run_all_virtual_trades(intra_data: dict, target_stocks: list, current_cash: float, target_date: str):
    intra_ready = {}
    
    for sym_code, _, _ in target_stocks:
        i_df = intra_data.get(sym_code, pd.DataFrame())
        if i_df.empty: continue
        
        # DataFrame.index は datetime 型なので、文字列での loc が可能かチェック
        dates_in_df = np.unique(i_df.index.strftime('%Y-%m-%d'))
        if target_date not in dates_in_df:
            continue
            
        target_df = i_df.loc[target_date].copy()
        if isinstance(target_df, pd.Series): target_df = target_df.to_frame().T
        if len(target_df) < 20: continue
        
        close_series = target_df['Close'].iloc[:, 0] if isinstance(target_df['Close'], pd.DataFrame) else target_df['Close']
        target_df['ema5'] = close_series.ewm(span=5, adjust=False).mean()
        target_df['ema20'] = close_series.ewm(span=20, adjust=False).mean()
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
            prev_ema20 = df['ema20'].iloc[idx-1]
            curr_ema5 = df['ema5'].iloc[idx]
            curr_ema20 = df['ema20'].iloc[idx]
            
            is_dead_cross = (prev_ema5 >= prev_ema20) and (curr_ema5 < curr_ema20)
            
            if is_dead_cross or is_time_limit:
                qty = positions[sym_code]['size']
                entry_p = positions[sym_code]['entry_price']
                entry_dt = positions[sym_code]['entry_dt']
                pnl = (c - entry_p) * qty
                daily_profit += pnl
                available_cash += (qty * c)
                
                trade_logs.append({
                    'entry_time': entry_dt.strftime('%H:%M:%S'),
                    'exit_time': dt.strftime('%H:%M:%S'),
                    'symbol': sym_code,
                    'qty': qty,
                    'entry_price': entry_p,
                    'exit_price': c,
                    'pnl': pnl,
                    'reason': 'TimeLimit' if is_time_limit else 'DeadCross'
                })
                
                del positions[sym_code]
                
        # エントリー処理
        for sym_code, _, _ in target_stocks:
            if sym_code in positions or sym_code not in intra_ready: continue
            if is_time_limit: continue
            
            df = intra_ready[sym_code]
            if dt not in df.index: continue
            
            idx = df.index.get_loc(dt)
            if idx == 0: continue
            
            c = df['Close'].iloc[idx]
            if isinstance(c, pd.Series): c = c.iloc[0]
            
            prev_ema5 = df['ema5'].iloc[idx-1]
            prev_ema20 = df['ema20'].iloc[idx-1]
            curr_ema5 = df['ema5'].iloc[idx]
            curr_ema20 = df['ema20'].iloc[idx]
            
            is_golden_cross = (prev_ema5 <= prev_ema20) and (curr_ema5 > curr_ema20)
            
            if is_golden_cross:
                qty = int(available_cash // (c * 100)) * 100
                if qty >= 100:
                    positions[sym_code] = {'size': qty, 'entry_price': c, 'entry_dt': dt}
                    available_cash -= (qty * c)
                    
    return daily_profit, trade_logs

def get_data(symbol, period, interval):
    try:
        raw = yf.download(symbol, period=period, interval=interval, auto_adjust=True, progress=False)
        if isinstance(raw.columns, pd.MultiIndex): raw.columns = raw.columns.droplevel(1)
        raw.dropna(inplace=True)
        return symbol, raw
    except: return symbol, pd.DataFrame()

def main():
    INITIAL_CASH = 1_000_000
    print("=" * 70)
    print("💰 単元株制約・値嵩株除外・全力集中投資 リアルシミュレーション")
    print("=" * 70)

    daily_data, intra_data = {}, {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        f_daily = {ex.submit(get_data, sym, "60d", "1d"): sym for sym, _ in NIKKEI225}
        f_intra = {ex.submit(get_data, sym, "60d", "5m"): sym for sym, _ in NIKKEI225}
        for f in as_completed(f_daily): daily_data[f_daily[f]], d = f.result(); daily_data[f_daily[f]] = d
        for f in as_completed(f_intra): intra_data[f_intra[f]], d = f.result(); intra_data[f_intra[f]] = d

    # 共通のテスト対象日を特定
    common_index = None
    for df in daily_data.values():
        if common_index is None or len(df) > len(common_index):
            common_index = df.index
            
    test_dates = [d.strftime('%Y-%m-%d') for d in common_index[20:]]  # 最初の20日は学習用
    
    current_cash = INITIAL_CASH
    total_profit = 0
    results = []
    all_trade_logs = []
    
    print("\n[Step 2] 実運用シミュレーションを開始...")
    for target_date in test_dates:
        # 当日以前のデータで戦略適用 (当日の終値は見ないように、target_date未満でスライス)
        symbol_scores = []
        for sym_code, sym_name in NIKKEI225:
            d_df = daily_data.get(sym_code, pd.DataFrame())
            if d_df.empty: continue
            
            d_df_prev = d_df[d_df.index < target_date]
            if len(d_df_prev) < 20: continue
            
            last_close = d_df_prev['Close'].iloc[-1]
            if isinstance(last_close, pd.Series): last_close = last_close.iloc[0]
            if last_close >= 9000:
                continue
                
            score = run_daily_selection(d_df_prev.iloc[-25:]) # 最新1ヶ月程度を評価
            symbol_scores.append((sym_code, score, sym_name))
            
        symbol_scores.sort(key=lambda x: x[1])
        target_stocks = symbol_scores[:5]
        
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
              f"買 {log['entry_price']:>6.1f} -> 売 {log['exit_price']:>6.1f} | "
              f"損益: {sign}{pnl:>7,.0f} 円 ({log['reason']})")
        
    print("\n======================================================================")
    print(f"  合計損益: {total_profit:+10,.0f} 円")
    print(f"  最終資金: {current_cash:,.0f} 円")
    print("======================================================================")

if __name__ == '__main__':
    main()
