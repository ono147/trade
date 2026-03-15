import pandas as pd

try:
    df = pd.read_csv("nikkei225_walkforward.csv")
    tickers = []
    
    for index, row in df.iterrows():
        code = str(row['symbol']).strip()
        name = str(row['name']).strip()
        tickers.append(f'("{code}", "{name}")')
        
    with open("nikkei225_list.py", "w", encoding="utf-8") as f:
        f.write("NIKKEI225 = [\n    " + ",\n    ".join(tickers) + "\n]\n")
        
    print(f"Successfully wrote {len(tickers)} tickers to nikkei225_list.py")
except Exception as e:
    import traceback
    traceback.print_exc()
