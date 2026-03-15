import backtrader as bt
import yfinance as yf
import pandas as pd

# --- デイトレード戦略 (daytrade_backtest.pyと同じ) ---
class EmaCrossRsiStrategy(bt.Strategy):
    params = (
        ('ema_fast', 5),
        ('ema_slow', 20),
        ('rsi_period', 14),
        ('rsi_upper', 55),
        ('rsi_lower', 45),
        ('position_pct', 0.25),  # 1回の取引で資金の何%を使うか (デフォルト: 25%)
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
        print(f'{dt.strftime("%Y-%m-%d %H:%M")}, {txt}')

    def next(self):
        if self.order:
            return
        if not self.position:
            if self.crossover > 0 and self.rsi[0] >= self.p.rsi_upper:
                self.last_buy_price = self.dataclose[0]
                # 資金の position_pct% 分を購入できる株数を計算
                available = self.broker.get_cash() * self.p.position_pct
                size = int(available / self.dataclose[0])
                if size >= 1:
                    self.order = self.buy(size=size)
        else:
            if self.crossover < 0 or self.rsi[0] <= self.p.rsi_lower:
                # 保有している全株を売却
                self.order = self.close()

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'  → 買付完了: {order.executed.size:.0f}株 @ {order.executed.price:.2f}円')
            elif order.issell():
                self.log(f'  → 売却完了: {order.executed.size:.0f}株 @ {order.executed.price:.2f}円')
                self.trade_count += 1
                if order.executed.price > self.last_buy_price:
                    self.win_count += 1
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('  ⚠️ 注文キャンセル/残高不足')
        self.order = None


def run_backtest(symbol: str, name: str, interval: str = "5m",
                 period: str = "60d", initial_cash: float = 1_000_000.0):
    """指定銘柄でバックテストを実行し、結果を辞書で返す"""
    try:
        data_df = yf.download(symbol, period=period, interval=interval,
                              auto_adjust=True, progress=False)

        if isinstance(data_df.columns, pd.MultiIndex):
            data_df.columns = data_df.columns.droplevel(1)

        data_df.dropna(inplace=True)

        if len(data_df) < 30:
            return {"symbol": symbol, "name": name, "error": "データ不足"}

        cerebro = bt.Cerebro()
        cerebro.addstrategy(EmaCrossRsiStrategy)
        data_feed = bt.feeds.PandasData(dataname=data_df)
        cerebro.adddata(data_feed)
        cerebro.broker.setcash(initial_cash)
        cerebro.broker.setcommission(commission=0.0)

        results = cerebro.run()
        strat = results[0]

        final_value = cerebro.broker.getvalue()
        profit = final_value - initial_cash
        profit_pct = (profit / initial_cash) * 100
        win_rate = (strat.win_count / strat.trade_count * 100) if strat.trade_count > 0 else 0

        return {
            "symbol": symbol,
            "name": name,
            "最終資産(円)": int(final_value),
            "損益(円)": int(profit),
            "損益率(%)": round(profit_pct, 2),
            "取引回数": strat.trade_count,
            "勝率(%)": round(win_rate, 1),
        }

    except Exception as e:
        return {"symbol": symbol, "name": name, "error": str(e)}


if __name__ == '__main__':
    # 東証 売買金額上位10銘柄 (2024年時点の実績ベース)
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

    INITIAL_CASH = 100_000_000  # 1億円
    POSITION_PCT = 0.25          # 1回の取引で資金の25%を投入
    INTERVAL = "5m"
    PERIOD = "60d"

    print("=" * 65)
    print(f"📊 デイトレードバックテスト 一括実行")
    print(f"   時間足: {INTERVAL} | 期間: {PERIOD} | 初期資金: {INITIAL_CASH:,}円 | 手数料: 0円")
    print("=" * 65)

    all_results = []
    for symbol, name in TARGETS:
        # 銘柄ごとに position_pct を渡す
        print(f"  実行中... {name} ({symbol})", end="", flush=True)
        result = run_backtest(symbol, name, interval=INTERVAL,
                              period=PERIOD, initial_cash=float(INITIAL_CASH))
        all_results.append(result)
        if "error" in result:
            print(f"  ❌ エラー: {result['error']}")
        else:
            sign = "+" if result["損益(円)"] >= 0 else ""
            print(f"  {sign}{result['損益(円)']:,}円 ({sign}{result['損益率(%)']:.2f}%)")

    # --- 結果を集計して表示 ---
    print("\n" + "=" * 65)
    print("📋 結果サマリー")
    print("=" * 65)

    success_results = [r for r in all_results if "error" not in r]
    df_results = pd.DataFrame(success_results).set_index("symbol")

    if not df_results.empty:
        # 損益率でソート
        df_results = df_results.sort_values("損益率(%)", ascending=False)
        print(df_results[["name", "損益(円)", "損益率(%)", "取引回数", "勝率(%)"]].to_string())

        total_profit = df_results["損益(円)"].sum()
        avg_profit_pct = df_results["損益率(%)"].mean()
        winners = len(df_results[df_results["損益(円)"] > 0])

        print("\n" + "-" * 65)
        print(f"  プラス銘柄数:     {winners} / {len(df_results)} 銘柄")
        print(f"  合計損益:         {total_profit:+,}円")
        print(f"  平均損益率:       {avg_profit_pct:+.2f}%")
        print("-" * 65)

        # CSVにも保存
        output_csv = "backtest_results.csv"
        df_results.to_csv(output_csv, encoding="utf-8-sig")
        print(f"\n💾 結果を '{output_csv}' に保存しました。")
