import sys, requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

H = {"User-Agent": UserAgent().random}
listing, selector = sys.argv[1], sys.argv[2]
soup = BeautifulSoup(requests.get(listing, headers=H, timeout=15).text, "lxml")
a = soup.select_one(selector)
href = a["href"]
if href.startswith("/"):
    href = "/".join(listing.split("/")[:3]) + href
print("ARTICLE", href)
art = BeautifulSoup(requests.get(href, headers=H, timeout=15).text, "lxml")
for h in art.select("h1")[:3]:
    print("H1", h.get("class"), h.get("id"), "|", h.get_text(strip=True)[:60])
for t in art.select("time")[:3]:
    print("TIME", t.get("class"), t.get("datetime"), "|", t.get_text(strip=True)[:30])
best = max(
    art.find_all(["div", "article", "section"]),
    key=lambda e: sum(len(p.get_text()) for p in e.find_all("p", recursive=False)),
    default=None,
)
if best is not None:
    print("CONTENT", best.name, best.get("class"), best.get("id"))