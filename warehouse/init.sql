-- ─────────────────────────────────────────────────────────────────────────────
-- init.sql — Schéma du Data Warehouse PostgreSQL
-- Filière IADATA · EMSI Casablanca · 2025-2026
-- ─────────────────────────────────────────────────────────────────────────────

-- Extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;   -- recherche full-text trigrammes
CREATE EXTENSION IF NOT EXISTS unaccent;  -- normalisation des accents

-- ── Table de faits principale ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS articles (
    id              UUID        PRIMARY KEY,
    source          VARCHAR(50) NOT NULL,
    country         CHAR(2),
    url             TEXT        UNIQUE NOT NULL,
    title           TEXT        NOT NULL,
    author          VARCHAR(200),
    published_at    TIMESTAMP,
    category        VARCHAR(100),
    language        CHAR(10),
    word_count      INTEGER,
    scraped_at      TIMESTAMP   NOT NULL DEFAULT NOW(),
    processed_at    TIMESTAMP,
    tier            VARCHAR(10) DEFAULT 'silver'
);

CREATE INDEX IF NOT EXISTS idx_articles_source      ON articles(source);
CREATE INDEX IF NOT EXISTS idx_articles_published   ON articles(published_at DESC);
CREATE INDEX IF NOT EXISTS idx_articles_language    ON articles(language);
CREATE INDEX IF NOT EXISTS idx_articles_category    ON articles(category);
CREATE INDEX IF NOT EXISTS idx_articles_title_trgm  ON articles USING gin(title gin_trgm_ops);

-- ── Table analytique : Articles par jour ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS articles_by_day (
    pub_date        DATE        NOT NULL,
    source          VARCHAR(50) NOT NULL,
    country         CHAR(2),
    article_count   INTEGER     NOT NULL,
    updated_at      TIMESTAMP   DEFAULT NOW(),
    PRIMARY KEY (pub_date, source)
);

-- ── Table analytique : Articles par thème ────────────────────────────────────
CREATE TABLE IF NOT EXISTS articles_by_theme (
    category        VARCHAR(100) NOT NULL,
    source          VARCHAR(50)  NOT NULL,
    language        CHAR(10),
    article_count   INTEGER      NOT NULL,
    updated_at      TIMESTAMP    DEFAULT NOW(),
    PRIMARY KEY (category, source)
);

-- ── Table analytique : Articles par pays ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS articles_by_country (
    country         CHAR(2)      NOT NULL,
    language        CHAR(10),
    article_count   INTEGER      NOT NULL,
    avg_word_count  NUMERIC(8,2),
    updated_at      TIMESTAMP    DEFAULT NOW(),
    PRIMARY KEY (country, language)
);

-- ── Table analytique : Articles par langue ────────────────────────────────────
CREATE TABLE IF NOT EXISTS articles_by_language (
    language        CHAR(10)     NOT NULL,
    source          VARCHAR(50)  NOT NULL,
    article_count   INTEGER      NOT NULL,
    updated_at      TIMESTAMP    DEFAULT NOW(),
    PRIMARY KEY (language, source)
);

-- ── Table analytique : Top mots-clés ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS top_keywords (
    keyword         VARCHAR(100) NOT NULL,
    source          VARCHAR(50)  NOT NULL,
    language        CHAR(10),
    frequency       INTEGER      NOT NULL,
    pub_date        DATE,
    updated_at      TIMESTAMP    DEFAULT NOW(),
    PRIMARY KEY (keyword, source, language)
);

CREATE INDEX IF NOT EXISTS idx_keywords_freq   ON top_keywords(frequency DESC);
CREATE INDEX IF NOT EXISTS idx_keywords_lang   ON top_keywords(language);

-- ── Table analytique : Tendances sources (7 jours glissants) ─────────────────
CREATE TABLE IF NOT EXISTS trending_sources (
    pub_date        DATE         NOT NULL,
    source          VARCHAR(50)  NOT NULL,
    country         CHAR(2),
    article_count   INTEGER      NOT NULL,
    rolling_7d_avg  NUMERIC(8,2),
    updated_at      TIMESTAMP    DEFAULT NOW(),
    PRIMARY KEY (pub_date, source)
);

-- ── Table de log qualité ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS quality_reports (
    id              SERIAL       PRIMARY KEY,
    layer           VARCHAR(20)  NOT NULL,  -- bronze | silver | gold
    run_date        DATE         NOT NULL,
    total_records   INTEGER,
    completeness_rate NUMERIC(5,4),
    critical_failures INTEGER,
    report_json     JSONB,
    created_at      TIMESTAMP    DEFAULT NOW()
);

-- ── Vue : Dashboard tendances ──────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_daily_trends AS
SELECT
    d.pub_date,
    d.source,
    d.country,
    d.article_count,
    t.rolling_7d_avg,
    ROUND(
        100.0 * d.article_count / NULLIF(t.rolling_7d_avg, 0) - 100,
        1
    ) AS pct_vs_avg
FROM articles_by_day d
LEFT JOIN trending_sources t USING (pub_date, source)
ORDER BY d.pub_date DESC, d.article_count DESC;

-- ── Vue : Top sources du jour ──────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_top_sources_today AS
SELECT
    source,
    country,
    article_count,
    RANK() OVER (ORDER BY article_count DESC) AS rank
FROM articles_by_day
WHERE pub_date = CURRENT_DATE
ORDER BY article_count DESC;

-- ── Vue : Mots-clés tendance (derniers 7 jours) ───────────────────────────────
CREATE OR REPLACE VIEW v_trending_keywords AS
SELECT
    keyword,
    language,
    SUM(frequency) AS total_frequency,
    COUNT(DISTINCT source) AS source_count
FROM top_keywords
WHERE pub_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY keyword, language
ORDER BY total_frequency DESC
LIMIT 50;

-- ── Données de démo ────────────────────────────────────────────────────────────
INSERT INTO articles_by_day (pub_date, source, country, article_count) VALUES
    (CURRENT_DATE,     'hespress',  'MA', 45),
    (CURRENT_DATE,     'akhbarona', 'MA', 32),
    (CURRENT_DATE,     'aljazeera', 'QA', 28),
    (CURRENT_DATE,     'bbc',       'GB', 67),
    (CURRENT_DATE,     'reuters',   'US', 89),
    (CURRENT_DATE - 1, 'hespress',  'MA', 38),
    (CURRENT_DATE - 1, 'akhbarona', 'MA', 29),
    (CURRENT_DATE - 1, 'bbc',       'GB', 71)
ON CONFLICT DO NOTHING;

INSERT INTO top_keywords (keyword, source, language, frequency, pub_date) VALUES
    ('Maroc',      'hespress',  'fr', 150, CURRENT_DATE),
    ('économie',   'hespress',  'fr', 120, CURRENT_DATE),
    ('politique',  'akhbarona', 'ar', 95,  CURRENT_DATE),
    ('climate',    'bbc',       'en', 210, CURRENT_DATE),
    ('technology', 'reuters',   'en', 180, CURRENT_DATE),
    ('Gaza',       'aljazeera', 'ar', 320, CURRENT_DATE)
ON CONFLICT DO NOTHING;

-- ── Message de fin ────────────────────────────────────────────────────────────
DO $$
BEGIN
    RAISE NOTICE '✓ Schéma Data Warehouse initialisé avec succès';
END $$;
