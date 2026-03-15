"""
ウォークフォワードバックテスト
================================
[Step 1] 全銘柄に対して「学習期間（最初の40取引日）」でバックテスト
  → プラスになった銘柄を選定
[Step 2] 選定した銘柄のみ「検証期間（残り20取引日）」で売買シミュレーション
  → 結果を評価・CSV保存
"""
import backtrader as bt
import yfinance as yf
import pandas as pd
from datetime import timedelta

# --- 売買戦略 (EMAクロス + RSIフィルター) ---
class EmaCrossRsiStrategy(bt.Strategy):
    params = (
        ('ema_fast', 5),
        ('ema_slow', 20),
        ('rsi_period', 14),
        ('rsi_upper', 55),
        ('rsi_lower', 45),
        ('position_pct', 0.25),
    )

    def __init__(self):
        self.dataclose = self.datas[0].close
        self.order = None
        self.ema_fast = bt.indicators.EMA(self.datas[0], period=self.p.ema_fast)
        self.ema_slow = bt.indicators.EMA(self.datas[0], period=self.p.ema_slow)
        self.crossover = bt.indicators.CrossOver(self.ema_fast, self.ema_slow)
        self.rsi = bt.indicators.RSI(self.datas[0], period=self.p.rsi_period)
        self.trade_count = 0
        self.win_count = 0
        self.last_buy_price = 0.0

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.datetime(0)
        print(f'  {dt.strftime("%Y-%m-%d %H:%M")} | {txt}')

    def next(self):
        if self.order:
            return
        if not self.position:
            if self.crossover > 0 and self.rsi[0] >= self.p.rsi_upper:
                self.last_buy_price = self.dataclose[0]
                available = self.broker.get_cash() * self.p.position_pct
                size = int(available / self.dataclose[0])
                if size >= 1:
                    self.order = self.buy(size=size)
        else:
            if self.crossover < 0 or self.rsi[0] <= self.p.rsi_lower:
                self.order = self.close()

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'買付 {order.executed.size:.0f}株 @ {order.executed.price:.2f}円')
            elif order.issell():
                self.log(f'売却 {order.executed.size:.0f}株 @ {order.executed.price:.2f}円')
                self.trade_count += 1
                if order.executed.price > self.last_buy_price:
                    self.win_count += 1
        self.order = None


def run_backtest_on_df(df: pd.DataFrame, initial_cash: float, verbose: bool = False) -> dict:
    """DataFrameを受け取ってバックテストを実行し、結果を返す"""
    if df.empty or len(df) < 30:
        return {"profit": 0, "profit_pct": 0, "trade_count": 0, "win_count": 0,
                "error": "データ不足"}

    import io, sys
    cerebro = bt.Cerebro()
    cerebro.addstrategy(EmaCrossRsiStrategy)
    cerebro.adddata(bt.feeds.PandasData(dataname=df))
    cerebro.broker.setcash(initial_cash)
    cerebro.broker.setcommission(commission=0.0)

    # verbose=False のときは標準出力を抑制
    if not verbose:
        old_stdout = sys.stdout
        sys.stdout = io.StringIO()

    results = cerebro.run()
    final_value = cerebro.broker.getvalue()

    if not verbose:
        sys.stdout = old_stdout

    strat = results[0]
    profit = final_value - initial_cash
    profit_pct = (profit / initial_cash) * 100
    win_rate = (strat.win_count / strat.trade_count * 100) if strat.trade_count > 0 else 0

    return {
        "profit": profit,
        "profit_pct": round(profit_pct, 2),
        "trade_count": strat.trade_count,
        "win_count": strat.win_count,
        "win_rate": round(win_rate, 1),
    }


def split_by_trading_days(df: pd.DataFrame, train_days: int):
    """DataFrameを取引日数ベースで学習/検証に分割する"""
    # タイムゾーンを除いた日付だけを取り出してユニークな取引日リストを作る
    df_copy = df.copy()
    df_copy.index = pd.to_datetime(df_copy.index).tz_localize(None)
    unique_dates = sorted(df_copy.index.normalize().unique())

    if len(unique_dates) <= train_days:
        return df_copy, pd.DataFrame()  # 分割できない場合

    split_date = unique_dates[train_days]  # 学習 = 0〜train_days日目, 検証 = それ以降

    train_df = df_copy[df_copy.index.normalize() < split_date]
    test_df  = df_copy[df_copy.index.normalize() >= split_date]
    return train_df, test_df


if __name__ == '__main__':
    # ===== 設定 =====
    TARGETS = [
        ("7203.T", "トヨタ自動車"),
        ("9984.T", "ソフトバンクグループ"),
        ("6758.T", "ソニーグループ"),
        ("8306.T", "三菱UFJフィナンシャル"),
        ("8035.T", "東京エレクトロン"),
        ("7974.T", "任天堂"),
        ("6861.T", "キーエンス"),
        ("9983.T", "ファーストリテイリング"),
        ("6098.T", "リクルートHD"),
        ("4063.T", "信越化学工業"),
    ]
    INITIAL_CASH   = 100_000_000  # 1億円
    INTERVAL       = "5m"
    PERIOD         = "60d"
    TRAIN_DAYS     = 40           # 学習期間 (取引日数)
    TEST_DAYS_NOTE = 20           # ※ 残り約20取引日が検証期間

    print("=" * 65)
    print("📊 ウォークフォワードバックテスト")
    print(f"   時間足: {INTERVAL} | 合計期間: {PERIOD}")
    print(f"   学習期間: 最初の {TRAIN_DAYS} 取引日  →  検証期間: 残り約 {TEST_DAYS_NOTE} 取引日")
    print(f"   初期資金: {INITIAL_CASH:,}円 | 手数料: 0円")
    print("=" * 65)

    # ===== Step 1: 全銘柄データを取得 & 学習/検証に分割 =====
    print("\n【Step 1】全銘柄のデータ取得・分割...")
    all_data = {}
    for symbol, name in TARGETS:
        raw_df = yf.download(symbol, period=PERIOD, interval=INTERVAL,
                             auto_adjust=True, progress=False)
        if isinstance(raw_df.columns, pd.MultiIndex):
            raw_df.columns = raw_df.columns.droplevel(1)
        raw_df.dropna(inplace=True)

        train_df, test_df = split_by_trading_days(raw_df, TRAIN_DAYS)
        actual_test_days = len(test_df.index.normalize().unique())
        print(f"  {name}: 学習 {len(train_df.index.normalize().unique())}日 / 検証 {actual_test_days}日")
        all_data[symbol] = {"name": name, "train": train_df, "test": test_df}

    # ===== Step 2: 学習期間でバックテスト → プラス銘柄を選定 =====
    print("\n【Step 2】学習期間でバックテスト中...")
    selected_symbols = []
    train_results = []
    for symbol, data in all_data.items():
        result = run_backtest_on_df(data["train"], INITIAL_CASH, verbose=False)
        pnl = result.get("profit", 0)
        pnl_pct = result.get("profit_pct", 0)
        flag = "✅ 選定" if pnl > 0 else "❌ 除外"
        print(f"  {flag} | {data['name']}: {pnl:+,.0f}円 ({pnl_pct:+.2f}%)")
        train_results.append({"symbol": symbol, "name": data["name"],
                              "学習損益(円)": int(pnl), "学習損益率(%)": pnl_pct})
        if pnl > 0:
            selected_symbols.append(symbol)

    print(f"\n  👉 選定銘柄数: {len(selected_symbols)} / {len(TARGETS)} 銘柄")
    if selected_symbols:
        selected_names = [all_data[s]["name"] for s in selected_symbols]
        print(f"  👉 選定銘柄: {', '.join(selected_names)}")

    # ===== Step 3: 検証期間で選定銘柄のみ売買 =====
    print("\n【Step 3】検証期間で売買シミュレーション中...")
    test_results = []
    if not selected_symbols:
        print("  ⚠️ 選定銘柄がないため、検証をスキップします。")
    else:
        for symbol in selected_symbols:
            data = all_data[symbol]
            result = run_backtest_on_df(data["test"], INITIAL_CASH, verbose=False)
            trade_count = result.get("trade_count", 0)
            pnl = result.get("profit", 0)
            pnl_pct = result.get("profit_pct", 0)
            win_rate = result.get("win_rate", 0)
            mark = "🟢" if pnl >= 0 else "🔴"
            print(f"  {mark} {data['name']}: {pnl:+,.0f}円 ({pnl_pct:+.2f}%) | 取引: {trade_count}回 | 勝率: {win_rate}%")
            test_results.append({
                "symbol": symbol,
                "name": data["name"],
                "検証損益(円)": int(pnl),
                "検証損益率(%)": pnl_pct,
                "検証取引回数": trade_count,
                "検証勝率(%)": win_rate,
            })

    # ===== 最終サマリー =====
    print("\n" + "=" * 65)
    print("📋 最終サマリー（検証期間）")
    print("=" * 65)
    if test_results:
        df_test = pd.DataFrame(test_results).set_index("symbol")
        df_test = df_test.sort_values("検証損益率(%)", ascending=False)
        print(df_test[["name", "検証損益(円)", "検証損益率(%)", "検証取引回数", "検証勝率(%)"]].to_string())

        total = df_test["検証損益(円)"].sum()
        avg_pct = df_test["検証損益率(%)"].mean()
        winners = len(df_test[df_test["検証損益(円)"] > 0])
        print("\n" + "-" * 65)
        print(f"  選定→検証でプラス: {winners} / {len(test_results)} 銘柄")
        print(f"  合計損益:          {total:+,}円")
        print(f"  平均損益率:        {avg_pct:+.2f}%")
        print("-" * 65)

        # 学習・検証を統合してCSVに保存
        df_train = pd.DataFrame(train_results).set_index("symbol")
        df_combined = df_train.join(df_test[["検証損益(円)", "検証損益率(%)", "検証取引回数", "検証勝率(%)"]], how="left")
        df_combined.to_csv("walkforward_results.csv", encoding="utf-8-sig")
        print(f"\n💾 結果を 'walkforward_results.csv' に保存しました。")
