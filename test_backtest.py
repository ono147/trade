import backtrader as bt
import yfinance as yf
import pandas as pd
import datetime

# --- 売買ルールの定義 (Strategy) ---
class SmaCrossStrategy(bt.Strategy):
    """
    シンプルな移動平均線のクロス戦略
    短期線が長期線を下から上に抜けたら買い (ゴールデンクロス)
    上から下に抜けたら売り (デッドクロス)
    """
    # 戦略のパラメータ設定 (外部から変更可能)
    params = (
        ('pfast', 5),  # 短期移動平均線の期間 (例:5日)
        ('pslow', 25), # 長期移動平均線の期間 (例:25日)
    )

    def __init__(self):
        # データソースの終値を取得
        self.dataclose = self.datas[0].close
        
        # 注文状態を管理する変数
        self.order = None

        # テクニカル指標の計算
        # 1. 短期移動平均線 (Simple Moving Average)
        self.sma_fast = bt.indicators.SimpleMovingAverage(
            self.datas[0], period=self.params.pfast
        )
        # 2. 長期移動平均線
        self.sma_slow = bt.indicators.SimpleMovingAverage(
            self.datas[0], period=self.params.pslow
        )

        # 3. クロスの判定 (Crossover)
        # 短期線が長期線を上に抜けたら1, 下に抜けたら-1, それ以外は0を返す
        self.crossover = bt.indicators.CrossOver(self.sma_fast, self.sma_slow)

    def log(self, txt, dt=None):
        """ログを出力する補助関数"""
        dt = dt or self.datas[0].datetime.date(0)
        print(f'{dt.isoformat()}, {txt}')

    def next(self):
        """新しいデータ（1日分）が届くたびに呼ばれる処理"""
        
        # すでに注文を出している場合は、それが処理されるまで何もしない
        if self.order:
            return

        # まだ株を持っていない（ポジションがない）場合
        if not self.position:
            # ゴールデンクロス (crossoverが1) なら「買い」
            if self.crossover > 0:
                self.log(f'【買いシグナル】 株価: {self.dataclose[0]:.2f}')
                # 今回は分かりやすく100株ずつ買う
                self.order = self.buy(size=100)

        # すでに株を持っている（ポジションがある）場合
        else:
            # デッドクロス (crossoverが-1) なら「売り（決済）」
            if self.crossover < 0:
                self.log(f'【売り(決済)シグナル】 株価: {self.dataclose[0]:.2f}')
                self.order = self.sell(size=100)

    def notify_order(self, order):
        """注文の状態が変化したときに呼ばれる"""
        if order.status in [order.Submitted, order.Accepted]:
            # 注文が受け付けられた状態（まだ約定していない）
            return

        if order.status in [order.Completed]:
            # 注文が約定(成立)した
            if order.isbuy():
                self.log(f'🎯 買付完了 -> 価格: {order.executed.price:.2f}, 手数料: {order.executed.comm:.2f}')
            elif order.issell():
                self.log(f'🎯 売却完了 -> 価格: {order.executed.price:.2f}, 手数料: {order.executed.comm:.2f}')
            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('⚠️ 注文キャンセル・残高不足・拒否')

        # 注文状態をリセット
        self.order = None

# --- メイン処理 (バックテストの実行) ---
if __name__ == '__main__':
    # 1. バックテストエンジンの作成 (頭脳)
    cerebro = bt.Cerebro()

    # 2. 売買ルールの組み込み
    cerebro.addstrategy(SmaCrossStrategy)

    # 3. データの取得と組み込み (トヨタ自動車を例に)
    symbol = "7203.T"
    print(f"[{symbol}] のデータを取得中...")
    
    # 2022年から2025年1月までのデータでテスト
    data_df = yf.download(symbol, start="2022-01-01", end="2025-01-01")
    
    # yfinance(v0.2.x以降)は列がMultiIndexになる場合があるためフラットにする
    if isinstance(data_df.columns, pd.MultiIndex):
        data_df.columns = data_df.columns.droplevel(1)
    
    # yfinanceのデータをBacktrader用に変換
    data = bt.feeds.PandasData(dataname=data_df)
    cerebro.adddata(data)

    # 4. 初期設定
    # 初期資金を1,000,000円に設定
    initial_cash = 1000000.0
    cerebro.broker.setcash(initial_cash)
    
    # 手数料の設定 (0に設定)
    cerebro.broker.setcommission(commission=0.0)

    # 5. バックテストの実行
    print('=======================================')
    print(f'💰 初期資金: {cerebro.broker.getvalue():,.0f} 円')
    print('=======================================')
    
    # シミュレーション開始
    cerebro.run()

    # 6. 結果の出力
    final_value = cerebro.broker.getvalue()
    profit = final_value - initial_cash
    
    print('=======================================')
    print(f'💰 最終資産: {final_value:,.0f} 円')
    print(f'📈 損益:     {profit:,.0f} 円')
    print('=======================================')

    # 7. グラフの描画 (バックグラウンド実行のため今回はコメントアウト)
    # cerebro.plot(style='candlestick', barup='red', bardown='green')
