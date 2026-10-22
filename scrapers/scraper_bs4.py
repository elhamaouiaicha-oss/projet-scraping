"""
scraper_bs4.py — Collecte d'articles via BeautifulSoup
Filière IADATA · EMSI Casablanca · 2025-2026

Sites couverts :
  Marocains : Hespress, Akhbarona, Lakom, Barlamane
  Internationaux : Al Jazeera, CNN, BBC News, Reuters
"""

import json
import os
import time
import uuid
import logging
from datetime import datetime, timezone
from typing import Optional

import requests
from bs4 import BeautifulSoup
from langdetect import detect, LangDetectException
from fake_useragent import UserAgent
from confluent_kafka import Producer
from minio import Minio
from minio.error import S3Error
import schedule

# ─── Configuration ────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("scraper")

KAFKA_SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC_ARTICLES", "news-articles-raw")
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "localhost:9000").replace("http://", "")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY", "minioadmin")
SCRAPE_DELAY    = float(os.getenv("SCRAPE_DELAY", 2))       # secondes entre requêtes
BATCH_INTERVAL  = int(os.getenv("SCRAPE_BATCH_INTERVAL_HOURS", 1))

ua = UserAgent()

# ─── Sites configurés ─────────────────────────────────────────────────────────
# Chaque site définit : url de listing + sélecteurs CSS pour en extraire les liens,
# puis les sélecteurs pour parser chaque article.

SITES = [
    {
        "name": "hespress",
        "language": "fr",
        "country": "MA",
        "listing_url": "https://fr.hespress.com/",
        "link_selector": "a:has(h3)",
        "title_selector": "h1.post-title",
        "content_selector": "article p, main p",
        "author_selector": "span.author-name",
        "date_selector": "span.time",
        "category_selector": "span.category-name",
    },
    {
    "name": "akhbarona",
    "language": "ar",
    "country": "MA",
    "listing_url": "https://www.akhbarona.com/",
    "link_selector": "h3 a[href*='akhbarona.com/'], h3 a[href^='/']",
    "title_selector": "h1.article-detail-v1__title",
    "content_selector": "div.lc-para1",
    "author_selector": "",
    "date_selector": "time.story_date",
    "category_selector": "",
    },
    {
        "name": "aljazeera",
        "language": "ar",
        "country": "QA",
        "listing_url": "https://www.aljazeera.net/news/",
        "link_selector": "a:has(h2)",
        "title_selector": "h1",
        "content_selector": "article p, main p",
        "author_selector": "a[class*='author-link']",
        "date_selector": "time",
        "category_selector": "a[class*='topics']",
    },
    {
        "name": "bbc",
        "language": "en",
        "country": "GB",
        "listing_url": "https://www.bbc.com/news",
        "link_selector": "a[href*='/news/articles/']",
        "title_selector": "h1",
        "content_selector": "[data-component='text-block'] p",
        "author_selector": "span[class*='byline']",
        "date_selector": "time[datetime]",
        "category_selector": "a[class*='topicTag']",
    },
    {
        "name": "reuters",
        "language": "en",
        "country": "US",
        "listing_url": "https://www.reuters.com/",
        "link_selector": "a[data-testid='Heading'] ",
        "title_selector": "h1[data-testid='Heading']",
        "content_selector": "div[data-testid='paragraph-0']",
        "author_selector": "a[class*='author']",
        "date_selector": "time[datetime]",
        "category_selector": "a[data-testid='breadcrumb']",
    },
]

# ─── Clients ──────────────────────────────────────────────────────────────────

def make_kafka_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_SERVERS,
        "client.id": "news-scraper",
        "acks": "all",
        "retries": 3,
    })


def make_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS,
        secret_key=MINIO_SECRET,
        secure=False,
    )


def delivery_report(err, msg):
    if err:
        logger.error(f"Kafka delivery error: {err}")
    else:
        logger.debug(f"Message livré → topic={msg.topic()} partition={msg.partition()}")


# ─── Scraping ─────────────────────────────────────────────────────────────────

def fetch(url: str, retries: int = 3) -> Optional[BeautifulSoup]:
    """Télécharge une page et retourne un objet BeautifulSoup."""
    headers = {"User-Agent": ua.random}
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "lxml")
        except Exception as e:
            logger.warning(f"Tentative {attempt}/{retries} échouée pour {url}: {e}")
            time.sleep(SCRAPE_DELAY * attempt)
    return None


def detect_language(text: str) -> str:
    try:
        return detect(text[:500])
    except LangDetectException:
        return "unknown"


def scrape_article(url: str, site: dict) -> Optional[dict]:
    """Parse un article et retourne un dict structuré (couche Bronze)."""
    soup = fetch(url)
    if not soup:
        return None

   
    def get_text(selector: str) -> str:
        if not selector:
            return ""

        elements = soup.select(selector)

        return " ".join(
            el.get_text(" ", strip=True)
            for el in elements
            if el.get_text(strip=True)
        )

    title   = get_text(site["title_selector"])
    content = get_text(site["content_selector"])
    author  = get_text(site["author_selector"])
    date_el = soup.select_one(site["date_selector"])
    pub_date = (
        date_el.get("datetime") or date_el.get_text(strip=True)
        if date_el else ""
    )
    category = get_text(site["category_selector"])

    if not title or not content:
        return None

    return {
        "id":           str(uuid.uuid4()),
        "source":       site["name"],
        "country":      site["country"],
        "url":          url,
        "title":        title,
        "author":       author or "Inconnu",
        "published_at": pub_date,
        "category":     category,
        "content":      content,
        "language":     detect_language(content),
        "scraped_at":   datetime.now(timezone.utc).isoformat(),
        "word_count":   len(content.split()),
    }


def get_article_links(site: dict) -> list[str]:
    """Collecte les liens d'articles depuis la page de listing."""
    soup = fetch(site["listing_url"])
    if not soup:
        return []

    links = []
    for a in soup.select(site["link_selector"])[:20]:  # max 20 articles par run
        href = a.get("href", "")
        if href.startswith("http"):
            links.append(href)
        elif href.startswith("/"):
            base = "/".join(site["listing_url"].split("/")[:3])
            links.append(base + href)
    return list(set(links))


# ─── Ingestion ────────────────────────────────────────────────────────────────

def send_to_kafka(producer: Producer, article: dict):
    """Envoi streaming : un message par article."""
    payload = json.dumps(article, ensure_ascii=False).encode("utf-8")
    producer.produce(
        topic=KAFKA_TOPIC,
        key=article["id"].encode(),
        value=payload,
        callback=delivery_report,
    )
    producer.poll(0)


def save_to_minio_bronze(client: Minio, article: dict):
    """Sauvegarde batch dans MinIO — couche Bronze."""
    import io
    date_path = datetime.now(timezone.utc).strftime("%Y/%m/%d/%H")
    key = f"{date_path}/{article['source']}/{article['id']}.json"
    data = json.dumps(article, ensure_ascii=False, indent=2).encode("utf-8")
    try:
        client.put_object(
            bucket_name="bronze",
            object_name=key,
            data=io.BytesIO(data),
            length=len(data),
            content_type="application/json",
        )
        logger.debug(f"MinIO bronze → {key}")
    except S3Error as e:
        logger.error(f"MinIO error: {e}")


# ─── Pipeline principal ───────────────────────────────────────────────────────

def run_scraping_job():
    logger.info("=== Démarrage du job de scraping ===")
    producer = make_kafka_producer()
    minio    = make_minio_client()

    total = 0
    for site in SITES:
        logger.info(f"[{site['name']}] Collecte des liens...")
        links = get_article_links(site)
        logger.info(f"[{site['name']}] {len(links)} articles trouvés")

        for url in links:
            article = scrape_article(url, site)
            if article:
                # Streaming → Kafka
                send_to_kafka(producer, article)
                # Batch → MinIO Bronze
                save_to_minio_bronze(minio, article)
                total += 1
                logger.info(f"  ✓ [{site['name']}] {article['title'][:60]}…")
            time.sleep(SCRAPE_DELAY)

    producer.flush()
    logger.info(f"=== Job terminé — {total} articles collectés ===")


# ─── Planification (mode standalone) ─────────────────────────────────────────

if __name__ == "__main__":
    logger.info(f"Scraper démarré — intervalle batch: toutes les {BATCH_INTERVAL}h")
    run_scraping_job()                              # exécution immédiate au lancement
    schedule.every(BATCH_INTERVAL).hours.do(run_scraping_job)
    while True:
        schedule.run_pending()
        time.sleep(60)
