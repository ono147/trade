"""
アプローチ3: 中期選定 × 完全デイトレード戦略 ローリング検証
=======================================================
[目的]
決算またぎ等の突発的なギャップダウンリスクを排除するため、
「中期で負けている銘柄（反発エネルギーが高い）」を抽出しつつ、
「実際の売買はその日のうちに必ず決済する（完全デイトレ）」戦略が
どれくらいの利益・優位性を持つのかを検証する。

[手法: Two-Timeframe Rolling Walk-Forward]
1. 銘柄選定 (日足データ): 
   - 過去「1ヶ月(20営業日)」または「半年(120営業日)」の日足でバックテストを行い、
     その期間に【負け越していた銘柄（非選定組）】を抽出する。
2. 運用 (5分足データ):
   - 抽出された銘柄に対して、次の運用期間において【デイトレード専用ルール】で売買する。
   - エントリー: 朝方などの特定条件（ここではシンプルなEMAクロス）
   - エグジット: **14:45に強制決済** (オーバーナイトリスク・決算リスクゼロ)
"""
import backtrader as bt
import yfinance as yf
import pandas as pd
import io, sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')

NIKKEI20 = [
    ("7203.T","トヨタ自動車"),("8306.T","三菱UFJ"),("9984.T","ソフトバンクG"),
    ("6861.T","キーエンス"),("8035.T","東エレク"),("6758.T","ソニーG"),
    ("9432.T","NTT"),("6902.T","デンソー"),("8001.T","伊藤忠"),("9983.T","ファストリ"),
    ("8316.T","三井住友FG"),("8031.T","三井物産"),("4063.T","信越化学"),("6501.T","日立製作所"),
    ("7974.T","任天堂"),("6981.T","村田製作所"),("6857.T","アドバンテスト"),
    ("8053.T","住友商事"),("8002.T","丸紅"),("8411.T","みずほFG")
]

# ---------- 1. 日足用（選定用）の戦略 ----------
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
    params = (("fast", 5), ("slow", 20), ("position_pct", 0.20))
    def __init__(self):
        self.ema_fast = bt.indicators.EMA(period=self.p.fast)
        self.ema_slow = bt.indicators.EMA(period=self.p.slow)
        self.crossover = bt.indicators.CrossOver(self.ema_fast, self.ema_slow)
        
    def next(self):
        dt = self.data.datetime.datetime(0)
        # 14:45以降は強制決済し、新規エントリーしない
        if dt.hour >= 14 and dt.minute >= 45:
            if self.position:
                self.close()
            return

        if not self.position:
            if self.crossover > 0:
                size = int((self.broker.get_cash() * self.p.position_pct) / self.data.close[0])
                if size >= 1: self.buy(size=size)
        elif self.crossover < 0:
            self.close()

# ---------- 実行ヘルパー ----------
def run_bt(df: pd.DataFrame, initial_cash: float, strategy_class, timeframe=bt.TimeFrame.Days, compression=1) -> dict:
    if df.empty or len(df) < 30: return {"profit": 0, "error": "データ不足"}
    old_stdout, sys.stdout = sys.stdout, io.StringIO()
    try:
        cerebro = bt.Cerebro(stdstats=False)
        cerebro.addstrategy(strategy_class)
        # timeframeとcompressionを設定
        data_feed = bt.feeds.PandasData(dataname=df, timeframe=timeframe, compression=compression)
        cerebro.adddata(data_feed)
        cerebro.broker.setcash(initial_cash)
        cerebro.run()
        profit = cerebro.broker.getvalue() - initial_cash
        return {"profit": profit}
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
    # 今回は5分足が取れる最大期間「過去60日」の中でローリング検証を行う
    # 運用は「直近1日〜数日」のデイトレードをシミュレート
    
    print("=" * 70)
    print("⏳ 【決算リスクゼロ】中期選定 × 日帰デイトレ戦略 のローリング検証")
    print(f"   対象: {len(NIKKEI20)}銘柄 (計算量削減のため代表20銘柄)")
    print("=" * 70)

    # 日足（選定用）と5分足（運用用）の両方を取得
    print("\n[Step 1] 日足(60日分)と5分足(60日分)のデータを並列取得中...")
    daily_data = {}
    intra_data = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        f_daily = {ex.submit(get_data, sym, "60d", "1d"): sym for sym, _ in NIKKEI20}
        f_intra = {ex.submit(get_data, sym, "60d", "5m"): sym for sym, _ in NIKKEI20}
        
        for f in as_completed(f_daily): daily_data[f_daily[f]], d = f.result(); daily_data[f_daily[f]] = d
        for f in as_completed(f_intra): intra_data[f_intra[f]], d = f.result(); intra_data[f_intra[f]] = d

    print("取得完了")

    # 日足ベースでのローリングパターン（学習日数 → 運用日数）
    # 日足1日 ≒ 5分足60本
    TIMEFRAMES = [
        {"name": "2週間(10日)学習 → 次の一週間(5日)デイトレ", "train_d": 10, "test_d": 5},
        {"name": "1ヶ月(20日)学習 → 次の一週間(5日)デイトレ", "train_d": 20, "test_d": 5},
    ]

    print("\n[Step 2] ロバスト・デイトレのローリング検証を実行中...")

    results_summary = []
    
    # 共通の日付インデックス（日足）を取得
    common_index_d = None
    for sym, df in daily_data.items():
        if common_index_d is None or len(df) > len(common_index_d):
            common_index_d = df.index
    total_days = len(common_index_d)

    for tf in TIMEFRAMES:
        tr_days = tf["train_d"]
        te_days = tf["test_d"]
        name = tf["name"]
        
        roll_count = (total_days - tr_days - te_days) // te_days
        if roll_count <= 0: continue
            
        print(f"\n▶ テスト中: {name} (計{roll_count}回ループ)")
        
        total_sel_pnl = 0
        total_unsel_pnl = 0
        
        # 配列のインデックスではなく、日付(Timestamp)を境界にしてスライスする
        start_idx = 0
        for round_num in range(1, roll_count + 1):
            
            end_train_idx = start_idx + tr_days
            end_test_idx  = end_train_idx + te_days
            
            if end_test_idx >= total_days: break
            
            train_start_date = common_index_d[start_idx]
            train_end_date   = common_index_d[end_train_idx]
            test_start_date  = common_index_d[end_train_idx]
            test_end_date    = common_index_d[end_test_idx]
            
            # 日足にもウォームアップ(SMA20用)が必要なので長めに取る
            train_warmup_date = common_index_d[max(0, start_idx - 25)]
            
            round_sel_pnl_sum = 0
            round_unsel_pnl_sum = 0
            sel_count = 0
            unsel_count = 0

            for sym in NIKKEI20:
                sym_code = sym[0]
                d_df = daily_data.get(sym_code, pd.DataFrame())
                i_df = intra_data.get(sym_code, pd.DataFrame())
                
                if d_df.empty or i_df.empty: continue
                
                # 1. 学習 (日足での選定)
                train_df_daily = d_df.loc[train_warmup_date:train_end_date]
                train_res = run_bt(train_df_daily, INITIAL_CASH, DailySmaStrategy, bt.TimeFrame.Days, 1)
                pnl_train = train_res.get("profit", 0)
                
                # 2. 運用 (5分足でのデイトレ)
                # デイトレのEMAウォームアップのため、直近1〜2日分の5分足を含めてスライス
                try:
                    # test_start_date より前の適当な日付（ウォームアップ用）
                    wu_idx = max(0, d_df.index.get_loc(test_start_date) - 2)
                    intra_warmup_date = d_df.index[wu_idx]
                except:
                    intra_warmup_date = test_start_date # フォールバック

                test_df_intra = i_df.loc[str(intra_warmup_date)[:10]:str(test_end_date)[:10]]
                test_res = run_bt(test_df_intra, INITIAL_CASH, IntradayEmaStrategy, bt.TimeFrame.Minutes, 5)
                pnl_test = test_res.get("profit", 0)

                # 集計
                if pnl_train > 0:
                    round_sel_pnl_sum += pnl_test
                    sel_count += 1
                else:
                    round_unsel_pnl_sum += pnl_test
                    unsel_count += 1

            avg_sel_pnl = round_sel_pnl_sum / sel_count if sel_count > 0 else 0
            avg_unsel_pnl = round_unsel_pnl_sum / unsel_count if unsel_count > 0 else 0
            
            total_sel_pnl += avg_sel_pnl
            total_unsel_pnl += avg_unsel_pnl
            
            print(f"  Round {round_num:3d}/{roll_count}: 選定={avg_sel_pnl:+7,.0f} | 負け組デイトレ={avg_unsel_pnl:+7,.0f}", end="\r")
            start_idx += te_days
            
        results_summary.append({
            "時間軸": name,
            "検証回数": roll_count,
            "選定組のデイトレ": int(total_sel_pnl),
            "負け組のデイトレ(逆張り)": int(total_unsel_pnl),
            "差額(非選定 - 選定)": int(total_unsel_pnl - total_sel_pnl)
        })

    print("\n\n" + "=" * 80)
    print("🏆 最終結果: 中期選定 × 決算リスクゼロの完全デイトレ手法")
    print("=" * 80)
    df_res = pd.DataFrame(results_summary)
    pd.options.display.float_format = '{:,.0f}'.format
    print(df_res.to_string(index=False))
