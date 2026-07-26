"""
日経225 ローリング・ウォークフォワード検証
=======================================================
[目的]
「選定期間で負けていた銘柄が、検証期間で勝つ（ミーン・リバージョン）」
という現象が、単なる「カーブフィッティング（たまたま）」ではなく、
「日本株の普遍的な性質（エッジ）」であることを統計的に証明する。

[手法: Rolling Walk-Forward Analysis]
- データ: 過去5年間の日足データ (約1250取引日)
- ウィンドウサイズ:
  - 学習期間 (Train): 直近60営業日 (約3ヶ月)
  - 検証期間 (Test) : その後の20営業日 (約1ヶ月)
- ロール幅: 20営業日ごとにウィンドウを後ろへズラす
  → 合計 約50回強の「学習→検証」サイクルを繰り返す

[戦略] 最もシンプルな順張り（SMAクロス）
※戦略が優れているかではなく、「非選定組が勝つ」性質を証明するのが目的
- 買い: 5日線 > 20日線 (GDクロス)
- 決済/売り: 5日線 < 20日線 (DDクロス)
"""
import backtrader as bt
import yfinance as yf
import pandas as pd
import io, sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ============================================================
# 日経225 銘柄リスト（時間短縮のためランダムに抽出・または計算の軽い上位銘柄）
# 全銘柄(216)×50回のテストは計算量が膨大になるので、今回は代表的な50銘柄で検証。
# ============================================================
NIKKEI50 = [
    ("7203.T","トヨタ自動車"),("8306.T","三菱UFJ"),("9984.T","ソフトバンクG"),
    ("6861.T","キーエンス"),("8035.T","東エレク"),("6758.T","ソニーG"),
    ("9432.T","NTT"),("6902.T","デンソー"),("8001.T","伊藤忠"),("9983.T","ファストリ"),
    ("8316.T","三井住友FG"),("8031.T","三井物産"),("4063.T","信越化学"),("6501.T","日立製作所"),
    ("7974.T","任天堂"),("6981.T","村田製作所"),("6857.T","アドバンテスト"),
    ("8053.T","住友商事"),("8002.T","丸紅"),("8411.T","みずほFG"),
    ("4568.T","第一三共"),("6920.T","レーザーテック"),("3382.T","セブン&アイ"),
    ("6367.T","ダイキン"),("7741.T","HOYA"),("4502.T","武田薬品"),("7267.T","ホンダ"),
    ("6594.T","ニデック"),("8058.T","三菱商事"),("8766.T","東京海上HD"),
    ("4519.T","中外製薬"),("6301.T","小松製作所"),("6971.T","京セラ"),("6954.T","ファナック"),
    ("5108.T","ブリヂストン"),("9434.T","ソフトバンク"),("8591.T","オリックス"),
    ("4901.T","富士フイルム"),("1605.T","INPEX"),("2914.T","JT"),
    ("6702.T","富士通"),("4661.T","オリエンタルランド"),("9020.T","JR東日本"),
    ("8801.T","三井不動産"),("9433.T","KDDI"),("8802.T","三菱地所"),
    ("6146.T","ディスコ"),("4578.T","大塚HD"),("6503.T","三菱電機"),("2502.T","アサヒ")
]

class SmaStrategy(bt.Strategy):
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

def run_bt(df: pd.DataFrame, initial_cash: float) -> dict:
    if df.empty or len(df) < 25:
        return {"profit": 0, "error": "データ不足"}
    old_stdout, sys.stdout = sys.stdout, io.StringIO()
    try:
        cerebro = bt.Cerebro()
        cerebro.addstrategy(SmaStrategy)
        cerebro.adddata(bt.feeds.PandasData(dataname=df))
        cerebro.broker.setcash(initial_cash)
        cerebro.run()
        profit = cerebro.broker.getvalue() - initial_cash
        return {"profit": profit}
    except Exception as e:
        return {"profit": 0, "error": str(e)}
    finally:
        sys.stdout = old_stdout

def get_data(symbol):
    try:
        raw = yf.download(symbol, period="5y", interval="1d", auto_adjust=True, progress=False)
        if isinstance(raw.columns, pd.MultiIndex): raw.columns = raw.columns.droplevel(1)
        raw.dropna(inplace=True)
        return symbol, raw
    except:
        return symbol, pd.DataFrame()

if __name__ == '__main__':
    INITIAL_CASH = 1_000_000

    # 様々な時間軸（学習日数, 検証日数）の組み合わせをテスト
    # 営業日換算の目安: 1w=5日, 1m=20日, 3m=60日, 6m=120日
    TIMEFRAMES = [
        {"name": "1週間学習 → 1週間運用", "train": 5, "test": 5},
        {"name": "1ヶ月学習 → 1週間運用", "train": 20, "test": 5},
        {"name": "1ヶ月学習 → 1ヶ月運用", "train": 20, "test": 20},
        {"name": "3ヶ月学習 → 1ヶ月運用", "train": 60, "test": 20},
        {"name": "半年学習  → 1ヶ月運用", "train": 120, "test": 20},
        {"name": "半年学習  → 3ヶ月運用", "train": 120, "test": 60},
    ]

    print("=" * 70)
    print("⏳ ローリング・ウォークフォワード検証（最適時間軸の探索）")
    print(f"   対象: {len(NIKKEI50)}銘柄")
    print("   目的: どのスパン(1ヶ月, 半年など)が最もミーン・リバージョンが効くか？")
    print("=" * 70)

    print("\n[Step 1] 過去5年分のデータを取得中...")
    all_data = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(get_data, sym) for sym, _ in NIKKEI50]
        for f in as_completed(futures):
            sym, df = f.result()
            if len(df) > 1000: all_data[sym] = df
    print(f"取得完了: {len(all_data)}銘柄")

    common_index = None
    for sym, df in all_data.items():
        if common_index is None or len(df) > len(common_index):
            common_index = df.index
    total_bars = len(common_index)

    print("\n[Step 2] 全パターンのローリング検証を実行中...")
    
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
            
            warmup = 20
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
            
            start_idx += te_bars # ずらす
            
        # 1パターン終了
        results_summary.append({
            "時間軸": name,
            "検証回数": roll_count,
            "選定組(順張り)": int(total_sel_pnl),
            "非選定組(逆張り)": int(total_unsel_pnl),
            "差額(非選定 - 選定)": int(total_unsel_pnl - total_sel_pnl)
        })

    print("\n\n" + "=" * 80)
    print("🏆 最終結果: 時間軸ごとの「非選定組(負け組)」の優位性")
    print("=" * 80)
    df_res = pd.DataFrame(results_summary)
    # pandasの表示オプションでカンマ区切りにする
    pd.options.display.float_format = '{:,.0f}'.format
    
    # DataFrameをそのままprintすると見やすい
    print(df_res.to_string(index=False))
    
    best_tf = df_res.loc[df_res['非選定組(逆張り)'].idxmax()]
    print("\n" + "-" * 80)
    print(f"💡 最も利益が出た時間軸: 【{best_tf['時間軸']}】")
    print(f"   (非選定組の5年間トータル利益: {best_tf['非選定組(逆張り)']:,}円)")
    print("-" * 80)
