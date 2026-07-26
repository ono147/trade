import urllib.request
import json
import ssl

ssl._create_default_https_context = ssl._create_unverified_context
url = "https://raw.githubusercontent.com/k-kurasawa/nikkei225_data/master/nikkei225.json"

req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
html = urllib.request.urlopen(req).read()
data = json.loads(html)

tickers = []
for item in data:
    code = str(item['code']) + ".T"
    name = str(item['name'])
    tickers.append(f'("{code}", "{name}")')

with open('nikkei225_list.py', 'w') as f:
    f.write(f"NIKKEI225 = [\n    " + ",\n    ".join(tickers) + "\n]\n")

print("Generated nikkei225_list.py successfully")
