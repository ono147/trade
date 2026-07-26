"""
日経225 5分足 超短期ローリング検証
=======================================================
[目的]
デイトレードの基本である「5分足」において、短いスパンでの「順張り(モメンタム)」
と「逆張り(ミーン・リバージョン)」のどちらに優位性があるかを検証する。

[手法: Rolling Walk-Forward Analysis]
- データ: 過去60日間の5分足データ (yfinanceの取得限界最大期間)
  ※ 1日あたり約60本 × 60日 ＝ 約3600本
- ウィンドウサイズ (様々なパターンを検証):
  1. 1日(60本)学習 → 1日(60本)運用
  2. 3日(180本)学習 → 1日(60本)運用
  3. 1週間(300本)学習 → 1日(60本)運用
  4. 2週間(600本)学習 → 1週間(300本)運用

[戦略] シンプルなEMAクロス順張り (SMAより反応が早い)
- 買い: EMA(5) > EMA(20)
- 決済/売り: EMA(5) < EMA(20)
※ 日をまたがないデイトレードを想定
"""
import backtrader as bt
import yfinance as yf
import pandas as pd
import io, sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# 処理速度を考慮し、代表的な20銘柄に絞る (5分足はデータ量が多い＆計算回数が多い)
NIKKEI20 = [
    ("7203.T","トヨタ自動車"),("8306.T","三菱UFJ"),("9984.T","ソフトバンクG"),
    ("6861.T","キーエンス"),("8035.T","東エレク"),("6758.T","ソニーG"),
    ("9432.T","NTT"),("6902.T","デンソー"),("8001.T","伊藤忠"),("9983.T","ファストリ"),
    ("8316.T","三井住友FG"),("8031.T","三井物産"),("4063.T","信越化学"),("6501.T","日立製作所"),
    ("7974.T","任天堂"),("6981.T","村田製作所"),("6857.T","アドバンテスト"),
    ("8053.T","住友商事"),("8002.T","丸紅"),("8411.T","みずほFG")
]

class EmaStrategy(bt.Strategy):
    params = (("fast", 5), ("slow", 20), ("position_pct", 0.20))
    def __init__(self):
        self.ema_fast = bt.indicators.EMA(period=self.p.fast)
        self.ema_slow = bt.indicators.EMA(period=self.p.slow)
        self.crossover = bt.indicators.CrossOver(self.ema_fast, self.ema_slow)
        
    def next(self):
        # 毎日14:45になれば強制決済する (デイトレード)
        current_time = self.data.datetime.time()
        if current_time.hour == 14 and current_time.minute >= 45:
            if self.position:
                self.close()
            return

        if not self.position:
            if self.crossover > 0:
                size = int((self.broker.get_cash() * self.p.position_pct) / self.data.close[0])
                if size >= 1: self.buy(size=size)
        elif self.crossover < 0:
            self.close()

def run_bt(df: pd.DataFrame, initial_cash: float) -> dict:
    if df.empty or len(df) < 30: return {"profit": 0, "error": "データ不足"}
    old_stdout, sys.stdout = sys.stdout, io.StringIO()
    try:
        cerebro = bt.Cerebro()
        cerebro.addstrategy(EmaStrategy)
        cerebro.adddata(bt.feeds.PandasData(dataname=df, timeframe=bt.TimeFrame.Minutes, compression=5))
        cerebro.broker.setcash(initial_cash)
        # スリッページ・手数料を考慮するとマイナスが大きすぎるため、一旦ゼロで優位性を純粋に見る
        cerebro.run()
        profit = cerebro.broker.getvalue() - initial_cash
        return {"profit": profit}
    except Exception as e: return {"profit": 0, "error": str(e)}
    finally: sys.stdout = old_stdout

def get_data(symbol):
    try:
        # yfinanceの制限により、5分足は過去60日分までしか取得できない
        raw = yf.download(symbol, period="60d", interval="5m", auto_adjust=True, progress=False)
        if isinstance(raw.columns, pd.MultiIndex): raw.columns = raw.columns.droplevel(1)
        raw.dropna(inplace=True)
        return symbol, raw
    except: return symbol, pd.DataFrame()

if __name__ == '__main__':
    INITIAL_CASH = 1_000_000
    
    # 5分足は1日約60本と仮定
    BARS_PER_DAY = 60
    TIMEFRAMES = [
        {"name": "1日(60本)学習 → 1日(60本)運用", "train": BARS_PER_DAY, "test": BARS_PER_DAY},
        {"name": "3日(180本)学習 → 1日(60本)運用", "train": BARS_PER_DAY * 3, "test": BARS_PER_DAY},
        {"name": "1週間(300本)学習 → 1日(60本)運用", "train": BARS_PER_DAY * 5, "test": BARS_PER_DAY},
        {"name": "2週間(600本)学習 → 1週間(300本)運用", "train": BARS_PER_DAY * 10, "test": BARS_PER_DAY * 5},
    ]

    print("=" * 70)
    print("⏳ 超短期5分足 ローリング・ウォークフォワード検証")
    print(f"   対象: {len(NIKKEI20)}銘柄 (計算量削減のため代表20銘柄)")
    print("   期間: 過去60日間 (5分足データが取得できる最大期間)")
    print("=" * 70)

    print("\n[Step 1] 過去60日分の5分足データを取得中...")
    all_data = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(get_data, sym) for sym, _ in NIKKEI20]
        for f in as_completed(futures):
            sym, df = f.result()
            if len(df) > 500: all_data[sym] = df
    print(f"取得完了: {len(all_data)}銘柄")

    if not all_data:
        print("データ取得に失敗しました。")
        sys.exit()

    common_index = None
    for sym, df in all_data.items():
        if common_index is None or len(df) > len(common_index):
            common_index = df.index
    total_bars = len(common_index)
    print(f"最大データ長: {total_bars}本")

    print("\n[Step 2] 全パターンの5分足ローリング検証を実行中...")
    
    results_summary = []

    for tf in TIMEFRAMES:
        tr_bars = tf["train"]
        te_bars = tf["test"]
        name = tf["name"]
        
        roll_count = (total_bars - tr_bars - te_bars) // te_bars
        if roll_count <= 0:
            continue
            
        print(f"\n▶ テスト中: {name} (計{roll_count}回ループ)")
        
        total_sel_pnl = 0
        total_unsel_pnl = 0
        start_idx = 0
        
        for round_num in range(1, roll_count + 1):
            end_train_idx = start_idx + tr_bars
            end_test_idx  = end_train_idx + te_bars
            
            # EMA(20) 暖機運転として直近50本を追加
            warmup = 50
            start_train_warmup = max(0, start_idx - warmup)
            
            round_sel_pnl_sum = 0
            round_unsel_pnl_sum = 0
            sel_count = 0
            unsel_count = 0

            for sym, df in all_data.items():
                train_df = df.iloc[start_train_warmup:end_train_idx]
                start_test_warmup = max(0, end_train_idx - warmup)
                test_df  = df.iloc[start_test_warmup:end_test_idx]

                train_res = run_bt(train_df, INITIAL_CASH)
                test_res  = run_bt(test_df, INITIAL_CASH)

                pnl_train = train_res.get("profit", 0)
                pnl_test  = test_res.get("profit", 0)

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
            
            start_idx += te_bars
            
            # 1日学習→1日運用のパターンのみ詳細な日付(ラウンド)ごとの結果を記録・表示
            if name == "1日(60本)学習 → 1日(60本)運用":
                print(f"  運用日 {round_num:2d}/{roll_count}: 順張り={avg_sel_pnl:+7,.0f}円 | 逆張り={avg_unsel_pnl:+7,.0f}円")
            else:
                print(f"  Round {round_num:3d}/{roll_count}: 選定={avg_sel_pnl:+7,.0f} | 非選定={avg_unsel_pnl:+7,.0f}", end="\r")
            
        # パターン終了
        results_summary.append({
            "時間軸": name,
            "検証回数": roll_count,
            "選定組(順張り)": int(total_sel_pnl),
            "非選定組(逆張り)": int(total_unsel_pnl),
            "差額(非選定 - 選定)": int(total_unsel_pnl - total_sel_pnl)
        })

    print("\n\n" + "=" * 80)
    print("🏆 5分足 最終結果: 時間軸ごとの「非選定組(負け組)」の優位性")
    print("=" * 80)
    df_res = pd.DataFrame(results_summary)
    pd.options.display.float_format = '{:,.0f}'.format
    print(df_res.to_string(index=False))
