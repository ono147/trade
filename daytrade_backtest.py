import backtrader as bt
import yfinance as yf
import pandas as pd

# --- デイトレード戦略の定義 ---
class EmaCrossRsiStrategy(bt.Strategy):
    """
    デイトレード向け: EMAクロス + RSIフィルター戦略
    ・短期EMAが長期EMAを下から上抜け、かつRSIが50以上 → 買い
    ・短期EMAが長期EMAを上から下抜け、かつRSIが50以下 → 売り(決済)
    """
    params = (
        ('ema_fast', 5),   # 短期EMAの期間 (デフォルト: 5本)
        ('ema_slow', 20),  # 長期EMAの期間 (デフォルト: 20本)
        ('rsi_period', 14), # RSIの期間 (デフォルト: 14本)
        ('rsi_upper', 55), # RSI上限フィルター (買いは55以上)
        ('rsi_lower', 45), # RSI下限フィルター (売りは45以下)
        ('trade_size', 100), # 取引株数
    )

    def __init__(self):
        self.dataclose = self.datas[0].close
        self.order = None

        # 指数移動平均線 (EMA)
        self.ema_fast = bt.indicators.EMA(self.datas[0], period=self.p.ema_fast)
        self.ema_slow = bt.indicators.EMA(self.datas[0], period=self.p.ema_slow)

        # クロスオーバー判定
        self.crossover = bt.indicators.CrossOver(self.ema_fast, self.ema_slow)

        # RSI (Relative Strength Index)
        self.rsi = bt.indicators.RSI(self.datas[0], period=self.p.rsi_period)

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.datetime(0)
        print(f'{dt.strftime("%Y-%m-%d %H:%M")}, {txt}')

    def next(self):
        if self.order:
            return

        if not self.position:
            # 買いシグナル: ゴールデンクロス AND RSIが上昇トレンドを示す (55以上)
            if self.crossover > 0 and self.rsi[0] >= self.p.rsi_upper:
                self.log(f'【買い】 終値: {self.dataclose[0]:.2f} RSI: {self.rsi[0]:.1f}')
                self.order = self.buy(size=self.p.trade_size)
        else:
            # 売りシグナル: デッドクロス OR RSIが下降トレンドを示す (45以下)
            if self.crossover < 0 or self.rsi[0] <= self.p.rsi_lower:
                self.log(f'【売り】 終値: {self.dataclose[0]:.2f} RSI: {self.rsi[0]:.1f}')
                self.order = self.sell(size=self.p.trade_size)

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'  → 買付完了: 価格={order.executed.price:.2f}')
            elif order.issell():
                self.log(f'  → 売却完了: 価格={order.executed.price:.2f}')
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('  ⚠️ 注文キャンセル/残高不足')
        self.order = None


if __name__ == '__main__':
    # --- 設定 ---
    symbol = "7203.T"     # 銘柄: トヨタ自動車
    interval = "5m"       # 時間足: 5分足
    period = "60d"        # 取得期間: 60日間 (5m足で取得できる最大)
    initial_cash = 1_000_000  # 初期資金: 100万円

    print(f"[{symbol}] の {interval} 足データを取得中 (直近{period})...")
    data_df = yf.download(symbol, period=period, interval=interval, auto_adjust=True)

    # MultiIndexカラムの場合はフラット化
    if isinstance(data_df.columns, pd.MultiIndex):
        data_df.columns = data_df.columns.droplevel(1)

    # 土日・NaN行を除去
    data_df.dropna(inplace=True)

    if data_df.empty:
        print("データが取得できませんでした。")
        exit()

    print(f"取得データ: {len(data_df)} 行 ({data_df.index[0]} ～ {data_df.index[-1]})")

    # --- Backtrader セットアップ ---
    cerebro = bt.Cerebro()
    cerebro.addstrategy(EmaCrossRsiStrategy)

    # Backtrader用データフィード作成
    data_feed = bt.feeds.PandasData(dataname=data_df)
    cerebro.adddata(data_feed)

    cerebro.broker.setcash(initial_cash)
    cerebro.broker.setcommission(commission=0.0)  # 手数料: 0円

    print('\n=======================================')
    print(f'💰 初期資金: {initial_cash:,.0f} 円')
    print(f'📊 戦略: EMA({EmaCrossRsiStrategy.params.ema_fast}/{EmaCrossRsiStrategy.params.ema_slow}) + RSI({EmaCrossRsiStrategy.params.rsi_period})')
    print('=======================================')

    cerebro.run()

    final_value = cerebro.broker.getvalue()
    profit = final_value - initial_cash

    print('\n=======================================')
    print(f'💰 最終資産: {final_value:,.0f} 円')
    print(f'📈 損益:     {profit:+,.0f} 円')
    print('=======================================')

    # グラフ描画 (コマンドラインで実行すると別ウィンドウで表示)
    cerebro.plot(style='candlestick', barup='red', bardown='green')
