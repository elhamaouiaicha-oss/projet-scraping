"""
news_spider.py — Spider Scrapy multi-sites
Filière IADATA · EMSI Casablanca · 2025-2026

Utilisation :
  scrapy crawl news -o articles.json
  scrapy crawl news -a site=hespress -o hespress.json
"""

import uuid
from datetime import datetime, timezone

import scrapy
from langdetect import detect, LangDetectException


SITE_CONFIGS = {
    "hespress": {
        "start_urls": ["https://fr.hespress.com/"],
        "link_css":    "h3.post-title a::attr(href)",
        "title_css":   "h1.post-title::text",
        "content_css": "div.post-content *::text",
        "author_css":  "span.author-name::text",
        "date_css":    "span.time::text",
        "category_css":"span.category-name::text",
        "country": "MA",
    },
    "akhbarona": {
    "start_urls": ["https://www.akhbarona.com/"],
    "link_css": (
        "h3.story_date_bh_sw "
        "a[href*='/politic/']::attr(href)"
    ),
    "title_css": "h1.article-detail-v1__title::text",
    "content_css": "div.lc-para1 *::text",
    "author_css": "",
    "date_css": "time.story_date::attr(datetime)",
    "category_css": "",
    "country": "MA",
    },
    "aljazeera": {
        "start_urls": ["https://www.aljazeera.net/news/"],
        "link_css":    "article h3 a::attr(href)",
        "title_css":   "h1::text",
        "content_css": "div.wysiwyg *::text",
        "author_css":  "a[class*='author-link']::text",
        "date_css":    "time::attr(datetime)",
        "category_css":"a[class*='topics']::text",
        "country": "QA",
    },
    "bbc": {
        "start_urls": ["https://www.bbc.com/news"],
        "link_css":    "h3 a::attr(href)",
        "title_css":   "h1[id='main-heading']::text",
        "content_css": "article p::text",
        "author_css":  "span[class*='byline']::text",
        "date_css":    "time::attr(datetime)",
        "category_css":"a[class*='topicTag']::text",
        "country": "GB",
    },
    "reuters": {
        "start_urls": ["https://www.reuters.com/"],
        "link_css":    "a[data-testid='Heading']::attr(href)",
        "title_css":   "h1[data-testid='Heading']::text",
        "content_css": "div[data-testid*='paragraph'] *::text",
        "author_css":  "a[class*='author']::text",
        "date_css":    "time::attr(datetime)",
        "category_css":"a[data-testid='breadcrumb']::text",
        "country": "US",
    },
}


class NewsSpider(scrapy.Spider):
    name = "news"
    custom_settings = {
        "DOWNLOAD_DELAY": 2,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1,
        "AUTOTHROTTLE_MAX_DELAY": 10,
        "ROBOTSTXT_OBEY": True,
        "USER_AGENT": (
            "Mozilla/5.0 (compatible; EMSINewsBot/1.0; "
            "+https://emsi.ma/newsbot)"
        ),
        "ITEM_PIPELINES": {
            "news_spider.pipelines.KafkaPipeline": 300,
            "news_spider.pipelines.MinioPipeline": 400,
        },
    }

    def __init__(self, site: str = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if site and site in SITE_CONFIGS:
            self.sites = {site: SITE_CONFIGS[site]}
        else:
            self.sites = SITE_CONFIGS

    def start_requests(self):
        for site_name, cfg in self.sites.items():
            for url in cfg["start_urls"]:
                yield scrapy.Request(
                    url,
                    callback=self.parse_listing,
                    meta={"site_name": site_name, "cfg": cfg},
                )

    def parse_listing(self, response):
        cfg       = response.meta["cfg"]
        site_name = response.meta["site_name"]
        base_url  = response.url.split("/")[0] + "//" + response.url.split("/")[2]

        links = response.css(cfg["link_css"]).getall()
        for href in links[:20]:
            if not href.startswith("http"):
                href = base_url + href if href.startswith("/") else base_url + "/" + href
            yield scrapy.Request(
                href,
                callback=self.parse_article,
                meta={"site_name": site_name, "cfg": cfg},
            )

    def parse_article(self, response):
        cfg       = response.meta["cfg"]
        site_name = response.meta["site_name"]

        title = " ".join(
        response.css(cfg["title_css"]).getall()
         ).strip()
        content = " ".join(
        text.strip()
        for text in response.css(cfg["content_css"]).getall()
         if text.strip()
        )
        author = (
    " ".join(response.css(cfg["author_css"]).getall()).strip()
    if cfg["author_css"]
    else "Inconnu"
)
        pub_date = (
    response.css(cfg["date_css"]).get("")
    if cfg["date_css"]
    else ""
).strip()
        category = (
    " ".join(response.css(cfg["category_css"]).getall()).strip()
    if cfg["category_css"]
    else ""
)

        if not title or not content:
            return

        try:
            lang = detect(content[:500])
        except LangDetectException:
            lang = "unknown"

        yield {
            "id":           str(uuid.uuid4()),
            "source":       site_name,
            "country":      cfg["country"],
            "url":          response.url,
            "title":        title,
            "author":       author,
            "published_at": pub_date,
            "category":     category,
            "content":      content,
            "language":     lang,
            "scraped_at":   datetime.now(timezone.utc).isoformat(),
            "word_count":   len(content.split()),
        }
