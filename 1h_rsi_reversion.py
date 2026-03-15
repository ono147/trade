"""
日経225 1時間足 ウォークフォワード検証（逆張り/デイトレ戦略）
=======================================================
[データ]
- 期間: 過去730日 (約2年間)
- 時間足: 1時間足 (1h)
- 資金: 100,000,000円 (1回のロット: 資金の20%)
- 手数料: 0%

[戦略] RSI 逆張り（ミーン・リバージョン）
- 買い: RSIが極端に低くなったら (例: 30未満) 「売られすぎ」と判断してロング
- 売り: RSIが極端に高くなったら (例: 70超え) 「買われすぎ」と判断してショート
- 決済: RSIが中立(50)に戻ったら、または逆のサインが出たら決済
- 時間で決済: デイトレ（スイング）要素として、長く持ちすぎない仕組み(N本経過)

[検証フロー]
- 前半1年間(約1200本) を学習期間にし、プラス銘柄を選定
- 後半1年間(残り) を検証期間にし、シミュレーション
"""
import backtrader as bt
import yfinance as yf
import pandas as pd
import io, sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ============================================================
# 日経225 銘柄リスト（216銘柄）
# ============================================================
NIKKEI225 = [
    ("1332.T","ニッスイ"),("1333.T","マルハニチロ"),("1605.T","INPEX"),
    ("1721.T","コムシスHD"),("1801.T","大成建設"),("1802.T","大林組"),
    ("1803.T","清水建設"),("1808.T","長谷工コーポレーション"),("1812.T","鹿島建設"),
    ("1925.T","大和ハウス工業"),("1928.T","積水ハウス"),("2002.T","日清製粉グループ"),
    ("2269.T","明治HD"),("2282.T","日本ハム"),("2413.T","エムスリー"),
    ("2432.T","ディー・エヌ・エー"),("2501.T","サッポロHD"),("2502.T","アサヒグループHD"),
    ("2503.T","キリンHD"),("2531.T","宝HD"),("2579.T","コカ・コーラBJH"),
    ("2593.T","伊藤園"),("2802.T","味の素"),("2871.T","ニチレイ"),
    ("2914.T","ジャパンタバコインターナショナル"),("3086.T","Jフロントリテイリング"),
    ("3099.T","三越伊勢丹HD"),("3101.T","東洋紡"),("3401.T","帝人"),
    ("3402.T","東レ"),("3405.T","クラレ"),("3407.T","旭化成"),
    ("3436.T","SUMCO"),("3769.T","GMOペイメントゲートウェイ"),
    ("3861.T","王子HD"),("3863.T","日本製紙"),("4004.T","レゾナック・HD"),
    ("4005.T","住友化学"),("4021.T","日産化学"),("4042.T","東ソー"),
    ("4043.T","トクヤマ"),("4061.T","デンカ"),("4063.T","信越化学工業"),
    ("4151.T","協和キリン"),("4183.T","三井化学"),("4188.T","三菱ケミカルグループ"),
    ("4208.T","UBE"),("4301.T","アミューズ"),("4307.T","野村総合研究所"),
    ("4324.T","電通グループ"),("4502.T","武田薬品工業"),("4503.T","アステラス製薬"),
    ("4507.T","塩野義製薬"),("4519.T","中外製薬"),("4523.T","エーザイ"),
    ("4528.T","小野薬品工業"),("4543.T","テルモ"),("4568.T","第一三共"),
    ("4578.T","大塚HD"),("4661.T","オリエンタルランド"),("4689.T","LINEヤフー"),
    ("4704.T","トレンドマイクロ"),("4751.T","サイバーエージェント"),
    ("4755.T","楽天グループ"),("4901.T","富士フイルムHD"),("4902.T","コニカミノルタ"),
    ("5012.T","東燃ゼネラル石油"),("5019.T","出光興産"),("5020.T","ENEOSホールディングス"),
    ("5101.T","横浜ゴム"),("5105.T","TOYO TIRES"),("5108.T","ブリヂストン"),
    ("5201.T","AGC"),("5202.T","日本板硝子"),("5214.T","日本電気硝子"),
    ("5232.T","住友大阪セメント"),("5233.T","太平洋セメント"),
    ("5301.T","東海カーボン"),("5332.T","TOTO"),("5333.T","日本碍子"),
    ("5334.T","日本特殊陶業"),("5401.T","日本製鉄"),("5406.T","神戸製鋼所"),
    ("5411.T","JFEホールディングス"),("5541.T","大平洋金属"),("5631.T","日本製鋼所"),
    ("5706.T","三井金属鉱業"),("5707.T","東邦亜鉛"),("5711.T","三菱マテリアル"),
    ("5713.T","住友金属鉱山"),("5714.T","DOWA ホールディングス"),("5715.T","古河機械金属"),
    ("5802.T","住友電気工業"),("5803.T","フジクラ"),("5901.T","洋缶ホールディングス"),
    ("6098.T","リクルートHD"),("6103.T","オークマ"),("6113.T","アマダ"),
    ("6178.T","日本郵政"),("6273.T","SMC"),("6301.T","小松製作所"),
    ("6302.T","住友重機械工業"),("6305.T","日立建機"),("6326.T","クボタ"),
    ("6361.T","荏原製作所"),("6367.T","ダイキン工業"),("6471.T","日本精工"),
    ("6473.T","ジェイテクト"),("6479.T","ミネベアミツミ"),("6506.T","安川電機"),
    ("6526.T","ソシオネクスト"),("6532.T","ベイカレント・コンサルティング"),
    ("6586.T","マキタ"),("6594.T","日本電産（ニデック）"),("6645.T","オムロン"),
    ("6674.T","ジーエス・ユアサ コーポレーション"),("6702.T","富士通"),
    ("6724.T","セイコーエプソン"),("6752.T","パナソニックHD"),("6753.T","シャープ"),
    ("6758.T","ソニーグループ"),("6762.T","TDK"),("6770.T","アルプスアルパイン"),
    ("6841.T","横河電機"),("6857.T","アドバンテスト"),("6861.T","キーエンス"),
    ("6902.T","デンソー"),("6920.T","レーザーテック"),("6954.T","ファナック"),
    ("6958.T","日本CMK"),("6971.T","京セラ"),("6976.T","太陽誘電"),
    ("6981.T","村田製作所"),("6988.T","日東電工"),("7003.T","三井E&S"),
    ("7011.T","三菱重工業"),("7012.T","川崎重工業"),("7013.T","IHI"),
    ("7182.T","ゆうちょ銀行"),("7201.T","日産自動車"),("7202.T","いすゞ自動車"),
    ("7203.T","トヨタ自動車"),("7211.T","三菱自動車工業"),("7261.T","マツダ"),
    ("7267.T","本田技研工業"),("7269.T","スズキ"),("7270.T","SUBARU"),
    ("7272.T","ヤマハ発動機"),("7309.T","シマノ"),("7733.T","オリンパス"),
    ("7741.T","HOYA"),("7751.T","キヤノン"),("7752.T","リコー"),
    ("7762.T","シチズン時計"),("7832.T","バンダイナムコHD"),("7911.T","凸版印刷（TOPPAN）"),
    ("7912.T","大日本印刷"),("7974.T","任天堂"),("8001.T","伊藤忠商事"),
    ("8002.T","丸紅"),("8015.T","豊田通商"),("8031.T","三井物産"),
    ("8035.T","東京エレクトロン"),("8053.T","住友商事"),("8058.T","三菱商事"),
    ("8233.T","高島屋"),("8252.T","丸井グループ"),("8267.T","イオン"),
    ("8301.T","日本銀行"),("8303.T","新生銀行（SBI新生銀行）"),
    ("8304.T","あおぞら銀行"),("8306.T","三菱UFJフィナンシャル"),
    ("8308.T","りそなHD"),("8309.T","三井住友トラスト・HD"),
    ("8316.T","三井住友フィナンシャルグループ"),("8331.T","千葉銀行"),
    ("8354.T","ふくおかフィナンシャルグループ"),("8355.T","静岡銀行"),
    ("8411.T","みずほフィナンシャルグループ"),("8591.T","オリックス"),
    ("8601.T","大和証券グループ本社"),("8604.T","野村HD"),
    ("8630.T","SOMPOホールディングス"),("8725.T","MS&ADインシュアランスグループHD"),
    ("8750.T","第一生命HD"),("8766.T","東京海上HD"),("8795.T","T&Dホールディングス"),
    ("9001.T","東武鉄道"),("9005.T","東京急行電鉄"),("9007.T","小田急電鉄"),
    ("9008.T","京王電鉄"),("9009.T","京成電鉄"),("9020.T","東日本旅客鉄道"),
    ("9021.T","西日本旅客鉄道"),("9022.T","東海旅客鉄道"),
    ("9064.T","ヤマトHD"),("9101.T","日本郵船"),("9104.T","商船三井"),
    ("9107.T","川崎汽船"),("9201.T","日本航空"),("9202.T","ANAホールディングス"),
    ("9301.T","三菱倉庫"),("9432.T","日本電信電話（NTT）"),("9433.T","KDDI"),
    ("9434.T","ソフトバンク"),("9501.T","東京電力HD"),("9502.T","中部電力"),
    ("9503.T","関西電力"),("9531.T","東京ガス"),("9532.T","大阪ガス"),
    ("9602.T","東宝"),("9613.T","NTTデータグループ"),("9735.T","セコム"),
    ("9766.T","コナミグループ"),("9983.T","ファーストリテイリング"),
    ("9984.T","ソフトバンクグループ"),
]

# ============================================================
# 売買戦略 (RSI 逆張り)
# ============================================================
class RsiReversionStrategy(bt.Strategy):
    params = (
        ('rsi_period', 14),      
        ('rsi_buy', 30),         # 30以下で売られすぎ（買い）
        ('rsi_sell', 70),        # 70以上で買われすぎ（売り）
        ('position_pct', 0.20),  
        ('max_hold_bars', 20),   # 最大保有期間 (1h*20≒数日間のデイトレ〜スイング)
    )

    def __init__(self):
        self.dataclose = self.datas[0].close
        self.order = None
        self.rsi = bt.indicators.RSI(self.datas[0], period=self.p.rsi_period)
        self.trade_count = 0
        self.win_count = 0
        self.bars_in_market = 0

    def next(self):
        if self.order:
            return

        # ポジションがない場合（エントリー）
        if not self.position:
            if self.rsi[0] < self.p.rsi_buy:
                size = int((self.broker.get_cash() * self.p.position_pct) / self.dataclose[0])
                if size >= 1:
                    self.order = self.buy(size=size)
                    self.bars_in_market = 0
                    
            elif self.rsi[0] > self.p.rsi_sell:
                size = int((self.broker.get_cash() * self.p.position_pct) / self.dataclose[0])
                if size >= 1:
                    self.order = self.sell(size=size)
                    self.bars_in_market = 0

        # ポジションがある場合（決済）
        else:
            self.bars_in_market += 1
            
            # ロングポジションの場合
            if self.position.size > 0:
                # 買われすぎ(70)に戻るか、最大保有期間を過ぎたら決済
                if self.rsi[0] >= self.p.rsi_sell or self.bars_in_market >= self.p.max_hold_bars:
                    self.order = self.close()
                    
            # ショートポジションの場合
            elif self.position.size < 0:
                # 売られすぎ(30)に戻るか、最大保有期間を過ぎたら決済
                if self.rsi[0] <= self.p.rsi_buy or self.bars_in_market >= self.p.max_hold_bars:
                    self.order = self.close()

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.trade_count += 1
        if trade.pnl > 0:
            self.win_count += 1


def run_bt(df: pd.DataFrame, initial_cash: float) -> dict:
    if df.empty or len(df) < 50:
        return {"profit": 0, "profit_pct": 0, "trade_count": 0,
                "win_count": 0, "win_rate": 0, "error": "データ不足"}
    old_stdout, sys.stdout = sys.stdout, io.StringIO()
    try:
        cerebro = bt.Cerebro()
        cerebro.addstrategy(RsiReversionStrategy)
        cerebro.adddata(bt.feeds.PandasData(dataname=df))
        cerebro.broker.setcash(initial_cash)
        cerebro.broker.setcommission(commission=0.0) 
        results = cerebro.run()
        strat = results[0]
        final = cerebro.broker.getvalue()
        profit = final - initial_cash
        pct = profit / initial_cash * 100
        win_rate = (strat.win_count / strat.trade_count * 100) if strat.trade_count > 0 else 0
        return {"profit": profit, "profit_pct": round(pct, 2),
                "trade_count": strat.trade_count, "win_count": strat.win_count,
                "win_rate": round(win_rate, 1)}
    except Exception as e:
        return {"profit": 0, "profit_pct": 0, "trade_count": 0,
                "win_count": 0, "win_rate": 0, "error": str(e)}
    finally:
        sys.stdout = old_stdout


def split_by_bars(df: pd.DataFrame, train_ratio: float = 0.5):
    """(日付ではなく)データの行数で学習・検証を半分に分ける"""
    df = df.copy()
    if len(df) < 100:
        return df, pd.DataFrame()
    split_idx = int(len(df) * train_ratio)
    return df.iloc[:split_idx], df.iloc[split_idx:]


def fetch_and_split(symbol: str, name: str, period: str, interval: str):
    try:
        raw = yf.download(symbol, period=period, interval=interval,
                          auto_adjust=True, progress=False)
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.droplevel(1)
        raw.dropna(inplace=True)
        if len(raw) < 100:
            return symbol, name, None, None, "データ不足"
        # 取得できたデータの前半を学習、後半を検証に分割
        train, test = split_by_bars(raw, 0.5)
        return symbol, name, train, test, None
    except Exception as e:
        return symbol, name, None, None, str(e)


if __name__ == '__main__':
    INITIAL_CASH = 100_000_000
    INTERVAL     = "1h"   # 1時間足
    PERIOD       = "730d" # 約2年間（yfinanceが1h足を出せる最大期間）
    MAX_WORKERS  = 8

    print("=" * 70)
    print("📈 日経225 1時間足 ウォークフォワード検証（RSI 逆張り戦略）")
    print(f"   対象: {len(NIKKEI225)}銘柄 | 時間足: {INTERVAL} | 期間: {PERIOD}")
    print(f"   分割: 前半1年(学習) → 後半1年(検証)")
    print(f"   戦略: RSI<30で買い、RSI>70で売り (最大20本保有)")
    print("=" * 70)

    print(f"\n【Step 1】{len(NIKKEI225)}銘柄のデータを並列取得・分割中...")
    all_data = {}
    failed   = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {
            ex.submit(fetch_and_split, sym, name, PERIOD, INTERVAL): (sym, name)
            for sym, name in NIKKEI225
        }
        done = 0
        for f in as_completed(futures):
            sym, name, train, test, err = f.result()
            done += 1
            if err:
                failed.append((sym, name, err))
            else:
                all_data[sym] = {"name": name, "train": train, "test": test}
            print(f"  [{done:3d}/{len(NIKKEI225)}] {name}({sym}) "
                  f"{'✅' if not err else '❌'}", end="\r", flush=True)

    print(f"\n  取得成功: {len(all_data)}銘柄 / 失敗: {len(failed)}銘柄")

    print(f"\n【Step 2】前半(学習期間) のバックテスト（{len(all_data)}銘柄）...")
    train_summary = []
    for i, (sym, data) in enumerate(all_data.items(), 1):
        res = run_bt(data["train"], INITIAL_CASH)
        data["train_result"] = res
        pnl = res.get("profit", 0)
        pct = res.get("profit_pct", 0)
        train_summary.append({"symbol": sym, "name": data["name"],
                               "学習損益(円)": int(pnl), "学習損益率(%)": pct})
        print(f"  [{i:3d}/{len(all_data)}] {data['name']}: {pnl:+,.0f}円", end="\r")

    print(f"\n【Step 3】後半(検証期間) のバックテスト（全{len(all_data)}銘柄）...")
    test_summary = []
    for i, (sym, data) in enumerate(all_data.items(), 1):
        res = run_bt(data["test"], INITIAL_CASH)
        pnl = res.get("profit", 0)
        pct = res.get("profit_pct", 0)
        tc  = res.get("trade_count", 0)
        wr  = res.get("win_rate", 0)
        test_summary.append({"symbol": sym, "name": data["name"],
                              "検証損益(円)": int(pnl), "検証損益率(%)": pct,
                              "検証取引回数": tc, "検証勝率(%)": wr})
        print(f"  [{i:3d}/{len(all_data)}] {data['name']}: {pnl:+,.0f}円", end="\r")


    # ===== 結果サマリー表示とCSV保存 =====
    df_train = pd.DataFrame(train_summary).set_index("symbol")
    df_test = pd.DataFrame(test_summary).set_index("symbol")
    df_all = df_train.join(df_test[["検証損益(円)", "検証損益率(%)", "検証取引回数", "検証勝率(%)"]], how="left")
    df_all["選定フラグ"] = df_all["学習損益(円)"] > 0
    csv_path = "1h_rsi_reversion.csv"
    df_all.to_csv(csv_path, encoding="utf-8-sig")

    print("\n\n" + "=" * 70)
    print("📈 選定組 vs 非選定組の成績比較 (逆張り・デイトレ)")
    print("=" * 70)
    selected_df = df_all[df_all["選定フラグ"] == True]
    unselected_df = df_all[df_all["選定フラグ"] == False]
    
    count_total = len(df_all)
    count_sel = len(selected_df)
    count_unsel = len(unselected_df)
    
    sel_win = len(selected_df[selected_df["検証損益(円)"] > 0])
    unsel_win = len(unselected_df[unselected_df["検証損益(円)"] > 0])
    
    print(f"全対象銘柄数: {count_total}銘柄")
    
    print(f"\n【学習期間でプラスだった銘柄 (選定)】: {count_sel}銘柄")
    if count_sel > 0:
        print(f"  → 検証期間でプラス: {sel_win}銘柄 ({(sel_win/count_sel*100):.1f}%)")
        print(f"  → 平均損益: {selected_df['検証損益(円)'].mean():,.0f}円")
    
    print(f"\n【学習期間でマイナス/ゼロだった銘柄 (非選定)】: {count_unsel}銘柄")
    if count_unsel > 0:
        print(f"  → 検証期間でプラス: {unsel_win}銘柄 ({(unsel_win/count_unsel*100):.1f}%)")
        print(f"  → 平均損益: {unselected_df['検証損益(円)'].mean():,.0f}円")

    print(f"\n💾 全結果を '{csv_path}' に保存しました。")
    print("=" * 70)
