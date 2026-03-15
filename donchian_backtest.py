"""
日経225 ウォークフォワードバックテスト（ドンチャンチャネル版）
=======================================================
[戦略] ドンチャンチャネル・ブレイクアウト (Donchian Channels)
- 買い: 現在の価格が過去N期間の最高値を更新したらロング
- 売り: 現在の価格が過去N期間の最安値を更新したらショート
- 決済: 買いポジションは最安値割れで決済、売りポジションは最高値抜けで決済（途転）

[検証フロー]
[Step 1] 全銘柄: 最初40取引日でバックテスト → プラス銘柄を選定
[Step 2] 選定銘柄のみ: 残り20取引日で売買シミュレーション → 評価
"""
import backtrader as bt
import yfinance as yf
import pandas as pd
import io, sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ============================================================
# 日経225 銘柄リスト（2024年時点）
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
# 売買戦略 (ドンチャンチャネル・ブレイクアウト)
# ============================================================
class DonchianStrategy(bt.Strategy):
    params = (
        ('period', 40),          # 過去N本の高値・安値を基準にする (5分足x40本 = 200分)
        ('position_pct', 0.25),  # 1回の取引で資金の25%を投入
    )

    def __init__(self):
        self.dataclose = self.datas[0].close
        self.datahigh = self.datas[0].high
        self.datalow = self.datas[0].low
        self.order = None
        
        # 過去N本の最高値と最安値を計算 (シフトして現在の足を含めないようにする)
        self.highest = bt.indicators.Highest(self.datahigh(-1), period=self.p.period)
        self.lowest = bt.indicators.Lowest(self.datalow(-1), period=self.p.period)
        
        self.trade_count = 0
        self.win_count = 0
        self.last_buy_price = 0.0

    def next(self):
        if self.order:
            return

        # 最高値上抜けで順張りロング、最安値下抜けで順張りショート
        is_break_high = self.dataclose[0] > self.highest[0]
        is_break_low = self.dataclose[0] < self.lowest[0]

        # ポジションなしの場合
        if not self.position:
            if is_break_high:
                self.last_buy_price = self.dataclose[0]
                available = self.broker.get_cash() * self.p.position_pct
                size = int(available / self.dataclose[0])
                if size >= 1:
                    self.order = self.buy(size=size)
            elif is_break_low:
                self.last_buy_price = self.dataclose[0]
                available = self.broker.get_cash() * self.p.position_pct
                size = int(available / self.dataclose[0])
                if size >= 1:
                    self.order = self.sell(size=size)

        # ロングポジション保有中の場合
        elif self.position.size > 0:
            # 手仕舞い条件 (最安値を下抜けたら損切り/利確して決済し、即座にショートへ途転)
            if is_break_low:
                self.order = self.close()
                
        # ショートポジション保有中の場合
        elif self.position.size < 0:
            # 手仕舞い条件 (最高値を上抜けたら損切り/利確して決済し、即座にロングへ途転)
            if is_break_high:
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
        cerebro.addstrategy(DonchianStrategy)
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


def split_by_trading_days(df: pd.DataFrame, train_days: int):
    df = df.copy()
    df.index = pd.to_datetime(df.index).tz_localize(None)
    dates = sorted(df.index.normalize().unique())
    if len(dates) <= train_days:
        return df, pd.DataFrame()
    split = dates[train_days]
    return df[df.index.normalize() < split], df[df.index.normalize() >= split]


def fetch_and_split(symbol: str, name: str, period: str, interval: str, train_days: int):
    try:
        raw = yf.download(symbol, period=period, interval=interval,
                          auto_adjust=True, progress=False)
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.droplevel(1)
        raw.dropna(inplace=True)
        if len(raw) < 50:
            return symbol, name, None, None, "データ不足"
        train, test = split_by_trading_days(raw, train_days)
        return symbol, name, train, test, None
    except Exception as e:
        return symbol, name, None, None, str(e)


if __name__ == '__main__':
    INITIAL_CASH = 100_000_000
    INTERVAL     = "5m"
    PERIOD       = "60d"
    TRAIN_DAYS   = 40
    MAX_WORKERS  = 8

    print("=" * 70)
    print("📊 日経225 ドンチャンチャネル ブレイクアウト ウォークフォワード検証")
    print(f"   対象: {len(NIKKEI225)}銘柄 | 時間足: {INTERVAL} | 期間: {PERIOD}")
    print(f"   学習: 最初{TRAIN_DAYS}取引日 → 検証: 残り約20取引日")
    print(f"   戦略: ドンチャンチャネル (過去40本 = 200分の高値・安値ブレイク)")
    print("=" * 70)

    print(f"\n【Step 1】{len(NIKKEI225)}銘柄のデータを並列取得中...")
    all_data = {}
    failed   = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {
            ex.submit(fetch_and_split, sym, name, PERIOD, INTERVAL, TRAIN_DAYS): (sym, name)
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

    print(f"\n【Step 2】学習期間バックテスト（{len(all_data)}銘柄）...")
    selected = []
    train_summary = []
    for i, (sym, data) in enumerate(all_data.items(), 1):
        res = run_bt(data["train"], INITIAL_CASH)
        data["train_result"] = res
        pnl = res.get("profit", 0)
        pct = res.get("profit_pct", 0)
        train_summary.append({"symbol": sym, "name": data["name"],
                               "学習損益(円)": int(pnl), "学習損益率(%)": pct})
        if pnl > 0 and "error" not in res:
            selected.append(sym)
        print(f"  [{i:3d}/{len(all_data)}] {data['name']}: {pnl:+,.0f}円", end="\r")

    print(f"\n  選定銘柄数: {len(selected)} / {len(all_data)} 銘柄")

    print(f"\n【Step 3】検証期間バックテスト（全{len(all_data)}銘柄）...")
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

    print("\n\n" + "=" * 70)
    print("📋 最終サマリー（検証期間 上位20銘柄）")
    print("=" * 70)
    df_test = pd.DataFrame(test_summary).set_index("symbol")
    df_test = df_test.sort_values("検証損益率(%)", ascending=False)

    print(df_test.head(20)[["name", "検証損益(円)", "検証損益率(%)", "検証取引回数", "検証勝率(%)"]].to_string())

    total   = df_test["検証損益(円)"].sum()
    avg_pct = df_test["検証損益率(%)"].mean()
    winners = len(df_test[df_test["検証損益(円)"] > 0])

    print("\n" + "-" * 70)
    print(f"  検証期間でプラスだった全銘柄: {winners} / {len(df_test)} 銘柄")
    print(f"  平均損益率:             {avg_pct:+.2f}%")
    print("-" * 70)

    # CSV保存
    df_train = pd.DataFrame(train_summary).set_index("symbol")
    df_all = df_train.join(df_test[["検証損益(円)", "検証損益率(%)", "検証取引回数", "検証勝率(%)"]], how="left")
    df_all["選定フラグ"] = df_all["学習損益(円)"] > 0
    csv_path = "donchian_walkforward.csv"
    df_all.to_csv(csv_path, encoding="utf-8-sig")
    print(f"\n💾 全結果を '{csv_path}' に保存しました。")
    print(f"   完了時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # ===== 選定・非選定の成績サマリーを表示 =====
    print("\n" + "=" * 70)
    print("📈 選定組 vs 非選定組の成績比較")
    print("=" * 70)
    selected_df = df_all[df_all["選定フラグ"] == True]
    unselected_df = df_all[df_all["選定フラグ"] == False]
    
    count_sel = len(selected_df)
    count_unsel = len(unselected_df)
    
    sel_win = len(selected_df[selected_df["検証損益(円)"] > 0])
    unsel_win = len(unselected_df[unselected_df["検証損益(円)"] > 0])
    
    print(f"【学習期間でプラスだった銘柄 (選定)】: {count_sel}銘柄")
    if count_sel > 0:
        print(f"  → 検証期間でプラス: {sel_win}銘柄 ({(sel_win/count_sel*100):.1f}%)")
        print(f"  → 平均損益: {selected_df['検証損益(円)'].mean():,.0f}円")
    
    print(f"【学習期間でマイナス/ゼロだった銘柄 (非選定)】: {count_unsel}銘柄")
    if count_unsel > 0:
        print(f"  → 検証期間でプラス: {unsel_win}銘柄 ({(unsel_win/count_unsel*100):.1f}%)")
        print(f"  → 平均損益: {unselected_df['検証損益(円)'].mean():,.0f}円")
