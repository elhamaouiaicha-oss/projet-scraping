import sys, requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent


def desc(el):
    if el is None or not getattr(el, "name", None):
        return "-"
    c = ".".join(el.get("class", []))
    return f"{el.name}.{c}" if c else el.name


url = sys.argv[1]
r = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"}, timeout=15)
print("STATUS", r.status_code, "LEN", len(r.text))
soup = BeautifulSoup(r.text, "lxml")
seen, n = set(), 0
for a in soup.select("a[href]"):
    text = a.get_text(strip=True)
    href = a["href"]
    if len(text) < 30 or href in seen:
        continue
    seen.add(href)
    print(f"{desc(a)} < {desc(a.parent)} < {desc(a.parent.parent)} | {href[-60:]} | {text[:40]}")
    n += 1
    if n >= 12:
        break
