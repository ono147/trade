import yfinance as yf
import pandas as pd
import numpy as np
import os
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings

warnings.filterwarnings('ignore')

LOG_FILE = "forward_test_log.csv"
from nikkei225_list import NIKKEI225

def run_daily_selection(df, target_date):
    df = df.loc[:target_date].copy()
    if len(df) < 25: return 0.0
    close = df['Close']
    if isinstance(close, pd.DataFrame): close = close.iloc[:, 0]
    df['sma5'] = close.rolling(window=5).mean()
    df['sma20'] = close.rolling(window=20).mean()
    df.dropna(inplace=True)
    if df.empty: return 0.0
    df['signal'] = np.where(df['sma5'] > df['sma20'], 1, -1)
    df['position'] = df['signal'].shift(1)
    df['returns'] = close.pct_change()
    df['strategy_returns'] = df['position'] * df['returns']
    total_return = np.exp(np.log1p(df['strategy_returns'].dropna()).sum()) - 1
    return total_return

def run_intraday_virtual_trade(df, target_date, invest_cash):
    if target_date not in df.index.strftime('%Y-%m-%d'): return 0.0
    target_df = df.loc[target_date].copy()
    if len(target_df) < 20: return 0.0
    close = target_df['Close']
    if isinstance(close, pd.DataFrame): close = close.iloc[:, 0]
    
    target_df['ema5'] = close.ewm(span=5, adjust=False).mean()
    target_df['ema20'] = close.ewm(span=20, adjust=False).mean()
    
    profit = 0.0
    position_size = 0
    entry_price = 0.0
    
    for i in range(1, len(target_df)):
        dt = target_df.index[i]
        c = close.iloc[i]
        prev_ema5 = target_df['ema5'].iloc[i-1]
        prev_ema20 = target_df['ema20'].iloc[i-1]
        curr_ema5 = target_df['ema5'].iloc[i]
        curr_ema20 = target_df['ema20'].iloc[i]
        
        is_golden_cross = (prev_ema5 <= prev_ema20) and (curr_ema5 > curr_ema20)
        is_dead_cross   = (prev_ema5 >= prev_ema20) and (curr_ema5 < curr_ema20)
        is_time_limit = (dt.hour == 15 and dt.minute >= 15) or (dt.hour > 15)
        
        if position_size == 0 and not is_time_limit:
            if is_golden_cross:
                position_size = invest_cash / c
                entry_price = c
        elif position_size > 0:
            if is_dead_cross or is_time_limit:
                profit += (c - entry_price) * position_size
                position_size = 0
        if is_time_limit: break
    return profit

def get_data(symbol, period, interval):
    try:
        raw = yf.download(symbol, period=period, interval=interval, auto_adjust=True, progress=False)
        if isinstance(raw.columns, pd.MultiIndex): raw.columns = raw.columns.droplevel(1)
        raw.dropna(inplace=True)
        return symbol, raw
    except: return symbol, pd.DataFrame()

print("Fetching data...")
daily_data, intra_data = {}, {}
with ThreadPoolExecutor(max_workers=10) as ex:
    f_daily = {ex.submit(get_data, sym, "2mo", "1d"): sym for sym, _ in NIKKEI225}
    f_intra = {ex.submit(get_data, sym, "1mo", "5m"): sym for sym, _ in NIKKEI225}
    for f in as_completed(f_daily): daily_data[f_daily[f]], d = f.result(); daily_data[f_daily[f]] = d
    for f in as_completed(f_intra): intra_data[f_intra[f]], d = f.result(); intra_data[f_intra[f]] = d

log_df = pd.read_csv(LOG_FILE)
current_cash = float(log_df.iloc[-1]['Total_Cash'])

dates_to_run = ['2026-03-09', '2026-03-10', '2026-03-11', '2026-03-12']

for target_date in dates_to_run:
    if target_date in log_df['Date'].values: continue
    
    sample_df = list(daily_data.values())[0] if daily_data else pd.DataFrame()
    if target_date not in sample_df.index.strftime('%Y-%m-%d'):
        continue

    symbol_scores = []
    for sym_code, sym_name in NIKKEI225:
        d_df = daily_data.get(sym_code, pd.DataFrame())
        if d_df.empty: continue
        d_df_prev = d_df[d_df.index < target_date]
        if d_df_prev.empty: continue
        score = run_daily_selection(d_df_prev, d_df_prev.index[-1].strftime('%Y-%m-%d'))
        symbol_scores.append((sym_code, score, sym_name))
        
    symbol_scores.sort(key=lambda x: x[1])
    target_stocks = symbol_scores[:5]
    
    daily_profit = 0.0
    invest_per_stock = current_cash / len(target_stocks)
    for sym_code, _, _ in target_stocks:
        i_df = intra_data.get(sym_code, pd.DataFrame())
        if i_df.empty: continue
        pnl = run_intraday_virtual_trade(i_df, target_date, invest_per_stock)
        daily_profit += pnl
        
    current_cash += daily_profit
    
    new_log = pd.DataFrame([{
        'Date': target_date,
        'Target_Stocks': ','.join([s[2] for s in target_stocks]),
        'Daily_Profit': round(daily_profit, 0),
        'Total_Cash': round(current_cash, 0)
    }])
    new_log.to_csv(LOG_FILE, mode='a', header=False, index=False)
    log_df = pd.concat([log_df, new_log], ignore_index=True)
    print(f"[{target_date}] 損益: {daily_profit:,.0f}円 | 資金: {current_cash:,.0f}円")
