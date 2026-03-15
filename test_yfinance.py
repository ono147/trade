import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf
import talib

def fetch_data(ticker_symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """指定した銘柄の株価データを取得する"""
    print(f"Fetching data for {ticker_symbol} from {start_date} to {end_date}...")
    ticker = yf.Ticker(ticker_symbol)
    df = ticker.history(start=start_date, end=end_date)
    return df

def calculate_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """テクニカル指標(SMA, RSI)を計算してDataFrameに追加する"""
    # 欠損値があるとTA-Libがエラーになるため、Close列をfloatのNumpy配列で処理する
    close_prices = df['Close'].values

    # 単純移動平均線 (Simple Moving Average: SMA)
    df['SMA25'] = talib.SMA(close_prices, timeperiod=25)
    df['SMA75'] = talib.SMA(close_prices, timeperiod=75)

    # 相対力指数 (Relative Strength Index: RSI)
    df['RSI14'] = talib.RSI(close_prices, timeperiod=14)

    return df

def plot_chart(df: pd.DataFrame, title: str):
    """取得したデータと指標をチャートに描画する(mplfinanceを使用)"""
    # 追加のプロット設定 (移動平均線)
    add_plots = [
        mpf.make_addplot(df['SMA25'], color='r', width=1.5, panel=0),
        mpf.make_addplot(df['SMA75'], color='b', width=1.5, panel=0),
        mpf.make_addplot(df['RSI14'], color='g', width=1.5, panel=2, ylabel='RSI (14)')
    ]

    # ローソク足チャートを描画（出来高込み）
    mpf.plot(df, type='candle', volume=True, addplot=add_plots,
             title=title, style='yahoo', panel_ratios=(4, 1, 1),
             figratio=(12, 8), figscale=1.2)

if __name__ == "__main__":
    # 例：トヨタ自動車 (銘柄コード + .T で日本の銘柄を指定)
    symbol = "7203.T"
    
    try:
        # 直近1年分のデータを取得
        df = fetch_data(symbol, start_date="2024-01-01", end_date="2025-01-01")
        
        if len(df) > 0:
            print("データ取得成功！テクニカル指標を計算します...")
            df = calculate_technical_indicators(df)
            
            print("チャートを描画します...")
            plot_chart(df, title=f"{symbol} Stock Price (2024)")
        else:
            print("データを取得できませんでした。")
            
    except Exception as e:
        print(f"エラーが発生しました: {e}")
