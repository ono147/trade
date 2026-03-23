"""
SBI証券 実運用想定 フォワードテスト（バーチャル運用）Bot
=======================================================
[概要]
最適化によるカーブフィッティングを排除するため、過去のバックテストではなく
「本日から未来に向かって、毎日仮想の資金で自動売買を行う」ためのスクリプト。

[仕様]
- 実行頻度: 毎日1回（引け後など）にこのスクリプトを実行する想定
- ロジック: ハイブリッド・デイトレード戦略
  1. 日足データ（直近1ヶ月間）で「最も負けていた上位5銘柄」を抽出
  2. その5銘柄に対して、"本日"の5分足データを用いてデイトレ売買を仮想実行
     - エントリー: 5分足 EMA(5) > EMA(20) のゴールデンクロス
     - 利確/損切: EMA(5) < EMA(20) のデッドクロス
     - 強制決済: 15:15 (手数料無料想定)
- 記録: 毎日の損益と抽出された銘柄を `forward_test_log.csv` に追記
"""
import yfinance as yf
import pandas as pd
import numpy as np
import os
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings

warnings.filterwarnings('ignore')

LOG_FILE = "forward_test_log.csv"
INITIAL_VIRTUAL_CASH = 1_000_000

# 日経225採用銘柄のリストをインポート
from nikkei225_list import NIKKEI225# --- 簡易バックテストエンジン (Backtraderを使わず軽量なPandas計算で本日の結果を出す) ---

def get_data(symbol, period, interval):
    try:
        raw = yf.download(symbol, period=period, interval=interval, auto_adjust=True, progress=False)
        if isinstance(raw.columns, pd.MultiIndex): raw.columns = raw.columns.droplevel(1)
        raw.dropna(inplace=True)
        return symbol, raw
    except: return symbol, pd.DataFrame()

def run_daily_selection(df: pd.DataFrame) -> float:
    """ 過去1ヶ月のSMAクロスによる仮想損益を計算 """
    if len(df) < 25: return 0.0
    df = df.copy()
    close_series = df['Close'].iloc[:, 0] if isinstance(df['Close'], pd.DataFrame) else df['Close']
    df['sma5'] = close_series.rolling(window=5).mean()
    df['sma20'] = close_series.rolling(window=20).mean()
    df.dropna(inplace=True)
    
    # +1でゴールデンクロス、-1でデッドクロス
    df['signal'] = np.where(df['sma5'] > df['sma20'], 1, -1)
    df['position'] = df['signal'].shift(1) # 翌日始値でエントリー想定
    
    # 簡易的なリターン計算
    df['returns'] = close_series.pct_change()
    df['strategy_returns'] = df['position'] * df['returns']
    
    # 累積値ではなく、資金100万円を複利しないで回した額 (大まかな優劣が分かれば良い)
    # ここでは単純化して累積リターンを損益スコアとする
    total_return = np.exp(np.log1p(df['strategy_returns']).sum()) - 1
    return total_return

def run_all_virtual_trades(intra_data: dict, target_stocks: list, current_cash: float):
    # 複数銘柄の5分足を時系列で追い、共通の資金枠で全力投資（100株単位）を行う
    intra_ready = {}
    
    # 本日の日付を特定（取得したデータの最新日）
    latest_dates = []
    for sym_code, _, _ in target_stocks:
        if not intra_data.get(sym_code, pd.DataFrame()).empty:
            latest_dates.append(intra_data[sym_code].index[-1].strftime('%Y-%m-%d'))
            
    if not latest_dates:
        return 0.0, datetime.datetime.now().strftime('%Y-%m-%d')
        
    today_date = max(latest_dates)
    
    for sym_code, _, _ in target_stocks:
        i_df = intra_data.get(sym_code, pd.DataFrame())
        if i_df.empty: continue
        try:
            target_df = i_df.loc[today_date].copy()
            if isinstance(target_df, pd.Series): target_df = target_df.to_frame().T
        except KeyError:
            continue
            
        if len(target_df) < 20: continue
        
        close_series = target_df['Close'].iloc[:, 0] if isinstance(target_df['Close'], pd.DataFrame) else target_df['Close']
        target_df['ema5'] = close_series.ewm(span=5, adjust=False).mean()
        target_df['ema20'] = close_series.ewm(span=20, adjust=False).mean()
        intra_ready[sym_code] = target_df
        
    if not intra_ready:
        return 0.0, today_date
        
    all_timestamps = set()
    for df in intra_ready.values():
        all_timestamps.update(df.index)
    all_timestamps = sorted(list(all_timestamps))
    
    daily_profit = 0.0
    available_cash = current_cash
    positions = {}
    
    for dt in all_timestamps:
        is_time_limit = (dt.hour == 15 and dt.minute >= 15) or (dt.hour > 15)
        
        # まず決済を処理（同タイミングでの売り買いは売り優先で資金を空ける）
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
                daily_profit += (c - entry_p) * qty
                available_cash += (qty * c)
                del positions[sym_code]
                
        # 次にエントリーを処理
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
                qty = int(available_cash // (c * 100)) * 100 # 100株単位で買えるだけ買う
                if qty >= 100:
                    positions[sym_code] = {'size': qty, 'entry_price': c}
                    available_cash -= (qty * c)
                    
    return daily_profit, today_date

def main():
    print("=" * 70)
    print("🤖 バーチャル自動売買(フォワードテスト) BOT 起動")
    print("=" * 70)
    
    # 履歴の読み込み（現在の仮想資金を取得するため）
    current_cash = INITIAL_VIRTUAL_CASH
    if os.path.exists(LOG_FILE):
        log_df = pd.read_csv(LOG_FILE)
        if not log_df.empty:
            current_cash = float(log_df.iloc[-1]['Total_Cash'])
            print(f"✅ 過去のログを読み込みました (開始資金: {current_cash:,.0f}円)")
    else:
        # 初回起動時、ヘッダーを書き込む
        pd.DataFrame(columns=['Date', 'Target_Stocks', 'Daily_Profit', 'Total_Cash']).to_csv(LOG_FILE, index=False)
        print(f"🆕 新規フォワードテストを開始します (開始資金: 1,000,000円)")

    print("\n[引け後処理] 本日までの相場データを取得中...")
    daily_data = {}
    intra_data = {}
    
    with ThreadPoolExecutor(max_workers=10) as ex:
        f_daily = {ex.submit(get_data, sym, "1mo", "1d"): sym for sym, _ in NIKKEI225}
        # 5分足は本日分が必要だが、EMAの暖機のために過去5日分を取る
        f_intra = {ex.submit(get_data, sym, "5d", "5m"): sym for sym, _ in NIKKEI225}
        
        for f in as_completed(f_daily): daily_data[f_daily[f]], d = f.result(); daily_data[f_daily[f]] = d
        for f in as_completed(f_intra): intra_data[f_intra[f]], d = f.result(); intra_data[f_intra[f]] = d

    # 1. 銘柄選定 (日足で過去1ヶ月の負け組を抽出)
    symbol_scores = []
    for sym_code, sym_name in NIKKEI225:
        d_df = daily_data.get(sym_code, pd.DataFrame())
        if d_df.empty: continue
        
        # 値嵩株（9,000円以上 = 100株で90万以上）は資金枠的に厳しいので対象外とする
        last_close = d_df['Close'].iloc[-1]
        if isinstance(last_close, pd.Series): last_close = last_close.iloc[0]
        if last_close >= 9000:
            continue
            
        score = run_daily_selection(d_df)
        symbol_scores.append((sym_code, score, sym_name))
        
    symbol_scores.sort(key=lambda x: x[1])
    target_stocks = symbol_scores[:5] # ワースト5銘柄を取得
    
    target_names = [s[2] for s in target_stocks]
    print(f"\n📊 今日の対象銘柄(過去1ヶ月の負け組): {', '.join(target_names)}")
    
    # 2. 本日のバーチャル運用 (5分足デイトレード)
    # 動的資金管理・100株単位での全力投資へ移行
    daily_profit, today_date = run_all_virtual_trades(intra_data, target_stocks, current_cash)

    # 3. ログの保存
    
    # 既に本日のログが存在するかチェック（重複実行防止）
    if os.path.exists(LOG_FILE):
        log_df = pd.read_csv(LOG_FILE)
        if today_date in log_df['Date'].values:
            print(f"\n⚠️ 既に本日({today_date})の運用ログは記録されています。")
            return

    current_cash += daily_profit
    
    print(f"\n📈 本日({today_date})の仮想損益: {daily_profit:+,.0f}円")
    print(f"💰 最新の仮想総資金: {current_cash:,.0f}円")
    
    new_log = pd.DataFrame([{
        'Date': today_date,
        'Target_Stocks': ','.join(target_names),
        'Daily_Profit': round(daily_profit, 0),
        'Total_Cash': round(current_cash, 0)
    }])
    
    new_log.to_csv(LOG_FILE, mode='a', header=False, index=False)
    print(f"✅ `{LOG_FILE}` に本日の結果を記録しました。明日もプログラムを実行してください！")

if __name__ == "__main__":
    main()
