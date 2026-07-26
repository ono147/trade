"""
ハイブリッド・デイトレード戦略: 100万円での損益シミュレーション
=======================================================
[条件]
- 当初資金: 1,000,000円
- 銘柄選定: 過去約1ヶ月間(20日)で最も負けていた銘柄を対象とする
- 運用方法: 5分足を使った日帰りデイトレ（14:45強制決済）
- 対象期間: 直近60営業日（yfinanceから取得可能な最大期間）
- 資金管理: 1トレードにつき資金の30%を投入（複数銘柄に分散）

[目的]
リアルな資金「100万円」で、実際にSBI証券等でこのシステムを60日間回した場合の
「利益額」や「勝率」「月単位の利回り」を現実的にシミュレーションする。
"""
import backtrader as bt
import yfinance as yf
import pandas as pd
import io, sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings

warnings.filterwarnings('ignore')

# 日経225採用銘柄のリストをインポート
from nikkei225_list import NIKKEI225# ---------- 1. 日足用（選定用）の戦略 ----------
class DailySmaStrategy(bt.Strategy):
    params = (('fast', 5), ('slow', 20), ('position_pct', 0.20))
    def __init__(self):
        self.sma_fast = bt.indicators.SMA(period=self.p.fast)
        self.sma_slow = bt.indicators.SMA(period=self.p.slow)
        self.crossover = bt.indicators.CrossOver(self.sma_fast, self.sma_slow)
    def next(self):
        if not self.position:
            if self.crossover > 0:
                size = int((self.broker.get_cash() * self.p.position_pct) / self.data.close[0])
                if size >= 1: self.buy(size=size)
        elif self.crossover < 0:
            self.close()

# ---------- 2. 5分足用（運用用・デイトレード）の戦略 ----------
class IntradayEmaStrategy(bt.Strategy):
    params = (("fast", 5), ("slow", 20), ("position_pct", 0.30)) # 資金の30%を投入
    def __init__(self):
        self.ema_fast = bt.indicators.EMA(period=self.p.fast)
        self.ema_slow = bt.indicators.EMA(period=self.p.slow)
        self.crossover = bt.indicators.CrossOver(self.ema_fast, self.ema_slow)
        self.trade_count = 0
        self.win_count = 0
        
    def next(self):
        dt = self.data.datetime.datetime(0)
        # 15:15以降は強制決済 (2024年の取引時間30分延長に対応)
        if (dt.hour == 15 and dt.minute >= 15) or dt.hour > 15:
            if self.position:
                self.close()
            return

        if not self.position:
            # 買いエントリー (ゴールデンクロス)
            if self.crossover > 0:
                size = int((self.broker.get_cash() * self.p.position_pct) / self.data.close[0])
                # 株価の単元株（日本株は通常100株単位）を考慮せず、計算上端数も買えるものとする
                if size >= 1: self.buy(size=size)
        elif self.crossover < 0:
            # 利確 / 損切り (デッドクロス)
            self.close()
            
    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.trade_count += 1
        if trade.pnl > 0:
            self.win_count += 1

def run_bt(df: pd.DataFrame, initial_cash: float, strategy_class, timeframe=bt.TimeFrame.Days, compression=1) -> dict:
    if df.empty or len(df) < 30: return {"profit": 0, "error": "データ不足"}
    old_stdout, sys.stdout = sys.stdout, io.StringIO()
    try:
        cerebro = bt.Cerebro(stdstats=False)
        cerebro.addstrategy(strategy_class)
        data_feed = bt.feeds.PandasData(dataname=df, timeframe=timeframe, compression=compression)
        cerebro.adddata(data_feed)
        cerebro.broker.setcash(initial_cash)
        # 最新のSBI証券(ゼロ革命)を反映し、売買手数料は完全無料(0%)とする
        cerebro.broker.setcommission(commission=0.0) 
        
        results = cerebro.run()
        strat = results[0]
        final_value = cerebro.broker.getvalue()
        profit = final_value - initial_cash
        
        # デイトレ戦略の場合のみ勝率を取得
        tc = getattr(strat, "trade_count", 0)
        wc = getattr(strat, "win_count", 0)
        
        return {"profit": profit, "final_value": final_value, "trade_count": tc, "win_count": wc}
    except Exception as e: return {"profit": 0, "error": str(e)}
    finally: sys.stdout = old_stdout

def get_data(symbol, period, interval):
    try:
        raw = yf.download(symbol, period=period, interval=interval, auto_adjust=True, progress=False)
        if isinstance(raw.columns, pd.MultiIndex): raw.columns = raw.columns.droplevel(1)
        raw.dropna(inplace=True)
        return symbol, raw
    except: return symbol, pd.DataFrame()

if __name__ == '__main__':
    INITIAL_CASH = 1_000_000
    
    print("=" * 70)
    print("💰 100万円・ハイブリッド戦略 リアルシミュレーション")
    print(f"   対象: 日経平均225銘柄")
    print("   設定: 手数料考慮(0.05%), 1回の投資枠30万円")
    print("=" * 70)

    print("\n[Step 1] 最新の過去60日分のデータを取得中...")
    daily_data = {}
    intra_data = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        f_daily = {ex.submit(get_data, sym, "60d", "1d"): sym for sym, _ in NIKKEI225}
        f_intra = {ex.submit(get_data, sym, "60d", "5m"): sym for sym, _ in NIKKEI225}
        for f in as_completed(f_daily): daily_data[f_daily[f]], d = f.result(); daily_data[f_daily[f]] = d
        for f in as_completed(f_intra): intra_data[f_intra[f]], d = f.result(); intra_data[f_intra[f]] = d

    common_index_d = None
    for sym, df in daily_data.items():
        if common_index_d is None or len(df) > len(common_index_d):
            common_index_d = df.index
    total_days = len(common_index_d)

    print("\n[Step 2] 実運用シミュレーションを開始...")
    
    # 今回は1日単位での結果を出力するため、運用期間(te_days)を「1日」にする
    tr_days = 20 # 1ヶ月(20日)学習
    te_days = 1  # 1日ごとに運用・出力
    
    roll_count = (total_days - tr_days - te_days) // te_days
    
    current_portfolio = INITIAL_CASH
    total_trades = 0
    total_wins = 0
    
    profits_history = []
    
    # 実際のお金（100万円）が増減していく様子をシミュレーション
    start_idx = 0
    for round_num in range(1, roll_count + 1):
        end_train_idx = start_idx + tr_days
        end_test_idx  = end_train_idx + te_days
        
        if end_test_idx >= total_days: break
        
        train_start_date = common_index_d[start_idx]
        train_end_date   = common_index_d[end_train_idx]
        test_start_date  = common_index_d[end_train_idx]
        test_end_date    = common_index_d[end_test_idx]
        
        train_warmup_date = common_index_d[max(0, start_idx - 25)]

        # 1. 学習フェーズ (日足でスコア計算)
        symbol_scores = [] # (symbol, pnl)
        for sym in NIKKEI225:
            sym_code = sym[0]
            d_df = daily_data.get(sym_code, pd.DataFrame())
            if d_df.empty: continue
            
            train_df_daily = d_df.loc[train_warmup_date:train_end_date]
            train_res = run_bt(train_df_daily, 1000000, DailySmaStrategy, bt.TimeFrame.Days, 1)
            symbol_scores.append((sym_code, train_res.get("profit", 0), sym[1]))
            
        # 損益がワースト（最も負けていた）5銘柄をピックアップ
        symbol_scores.sort(key=lambda x: x[1])
        target_symbols = symbol_scores[:5]
        period_profit = 0
        daily_trades = 0
        daily_wins = 0
        
        # 計算前にヘッダーだけ出す
        # print(f"[{test_start_date.strftime('%Y-%m-%d')} の運用結果]") # Moved below
        # print(f"  対象: {', '.join([s[2] for s in target_symbols])}") # Moved below
        # 抽出された銘柄に資金を分散して運用
        invest_per_stock = current_portfolio / len(target_symbols)
        # period_profit = 0 # Moved above
        
        for sym_code, _, _ in target_symbols:
            i_df = intra_data.get(sym_code, pd.DataFrame())
            if i_df.empty: continue
            
            # test_start_date より前の適当な日付（ウォームアップ用）
            try:
                wu_idx = max(0, d_df.index.get_loc(test_start_date) - 2)
                intra_warmup_date = d_df.index[wu_idx]
            except:
                intra_warmup_date = test_start_date

            test_df_intra = i_df.loc[str(intra_warmup_date)[:10]:str(test_end_date)[:10]]
            # 該当銘柄に分散した資金でバックテスト
            test_res = run_bt(test_df_intra, invest_per_stock, IntradayEmaStrategy, bt.TimeFrame.Minutes, 5)
            
            period_profit += test_res.get("profit", 0)
            daily_trades += test_res.get("trade_count", 0)
            daily_wins += test_res.get("win_count", 0)
            
        # --- 全銘柄の合計損益が出たところで1日分として処理 ---
        current_portfolio += period_profit
        profits_history.append(period_profit)
        total_trades += daily_trades
        total_wins += daily_wins
        
        # 1日の計算が終わった後に損益を出力
        print(f"[{test_start_date.strftime('%Y-%m-%d')} の運用結果]")
        print(f"  対象: {', '.join([s[2] for s in target_symbols])}")
        print(f"  損益: {period_profit:+7,.0f} 円 | 資金: {current_portfolio:,.0f} 円\n")
        
        start_idx += te_days

    # 最終結果
    print("\n" + "=" * 70)
    print("🎯 100万円運用 最終結果（約2ヶ月間・60営業日）")
    print("=" * 70)
    print(f"初期資金　: 1,000,000 円")
    print(f"最終資金　: {current_portfolio:,.0f} 円")
    net_profit = current_portfolio - INITIAL_CASH
    print(f"純利益　　: {net_profit:+,.0f} 円")
    print(f"月間利回り: {(net_profit / INITIAL_CASH * 100) / (total_days/20):.1f} % / 月")
    
    win_rate = (total_wins / total_trades * 100) if total_trades > 0 else 0
    print(f"勝率　　　: {win_rate:.1f} % ({total_wins}勝 / {total_trades - total_wins}敗)")
    print("※実際の手数料(0.05%)による摩擦コストを差し引いた後の金額です。")
    print("=" * 70)
