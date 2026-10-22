# Plateforme Big Data — Scraping & Analyse de Presse

**EMSI Casablanca · Filière IADATA · AU 2025-2026**
Projet Architecture de Données — Date limite : 10 Mai 2026

---

## Vue d'ensemble

Cette plateforme collecte automatiquement des articles de presse depuis des sites marocains et internationaux, les stocke dans un Data Lake organisé en architecture Médaillon (Bronze / Silver / Gold), et expose des tableaux de bord analytiques via Apache Superset.

**Stack technologique :**

| Composante | Technologie |
|---|---|
| Web Scraping | Python + BeautifulSoup4 + Scrapy |
| Ingestion streaming | Apache Kafka |
| Data Lake | MinIO (S3-compatible) |
| Transformations | Apache Spark (PySpark) |
| Orchestration | Apache Airflow |
| Data Warehouse | PostgreSQL 16 |
| Visualisation | Apache Superset |
| Conteneurisation | Docker + Docker Compose |

---

## Structure du projet

```
projet-scraping/
├── docker-compose.yml          # Tous les services
├── .env.example                # Variables d'environnement
├── README.md                   # Ce fichier
│
├── scrapers/                   # 1. Collecte des données
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── scraper_bs4.py          # Scraper BeautifulSoup (batch)
│   └── scrapy_project/         # Scraper Scrapy (crawl avancé)
│       └── news_spider/
│           ├── spiders/news_spider.py
│           └── pipelines.py    # Kafka + MinIO
│
├── ingestion/                  # 2. Ingestion
│   └── kafka_producer.py       # Consommateur Kafka → Silver streaming
│
├── transformations/            # 3–5. Pipeline Spark
│   ├── bronze_to_silver.py     # Nettoyage, normalisation, dédup
│   └── silver_to_gold.py       # Agrégats analytiques → DW
│
├── airflow/                    # 6. Orchestration
│   ├── Dockerfile
│   └── dags/
│       └── batch_scraping_dag.py  # DAG principal (horaire)
│
├── quality/                    # 9. Qualité des données
│   └── data_quality_checks.py  # Complétude, cohérence, validité
│
├── warehouse/                  # 7. Data Warehouse
│   └── init.sql                # Schéma PostgreSQL + vues analytiques
│
└── governance/                 # 10. Gouvernance
    └── data_catalog.py         # Catalogue + lineage + reporting
```

---

## Démarrage rapide

### Prérequis

- Docker Desktop (>= 24) avec au moins **8 Go RAM** alloués
- Docker Compose v2

### Lancement

```bash
# 1. Cloner et configurer
git clone <repo-url>
cd projet-scraping
cp .env.example .env

# 2. Démarrer tous les services
docker compose up -d

# 3. Vérifier que tout est UP
docker compose ps
```

### Initialisation (première fois)

```bash
# Airflow — initialiser la base et créer l'admin
docker compose run --rm airflow-init

# MinIO — les buckets bronze/silver/gold sont créés automatiquement
# par le service minio-init

# Vérifier les buckets
docker compose exec minio mc ls local/
```

---

## Accès aux interfaces

| Service | URL | Identifiants |
|---|---|---|
| Airflow (DAGs) | http://localhost:8082 | admin / admin |
| MinIO (Data Lake) | http://localhost:9001 | minioadmin / minioadmin |
| Kafka UI | http://localhost:8090 | — |
| Spark UI | http://localhost:8080 | — |
| Superset (Dashboards) | http://localhost:8088 | admin / admin |
| PostgreSQL | localhost:5432 | dwuser / dwpass / newsdb |

---

## Pipeline de données

### Architecture Médaillon

```
Sites d'actualité
      │
      ▼ (Scrapy / BeautifulSoup)
┌─────────────┐
│   BRONZE    │  Articles bruts JSON  →  s3a://bronze/AAAA/MM/JJ/HH/source/*.json
└─────────────┘
      │ Spark: nettoyage HTML, normalisation, dédup
      ▼
┌─────────────┐
│   SILVER    │  Articles propres Parquet  →  s3a://silver/source/language/
└─────────────┘
      │ Spark: agrégats, TF mots-clés, tendances
      ▼
┌─────────────┐
│    GOLD     │  Tables analytiques  →  s3a://gold/ + PostgreSQL
└─────────────┘
      │
      ▼
  Superset Dashboards
```

### Modes d'ingestion

**Batch** (toutes les heures, orchestré par Airflow) :
```bash
# Lancement manuel du scraper
docker compose exec scraper python scraper_bs4.py
```

**Streaming** (Kafka, temps réel) :
```bash
# Démarrer le consommateur Kafka → Silver
docker compose exec scraper python /app/../ingestion/kafka_producer.py
```

---

## Scraping

### Sites configurés

| Site | Pays | Langue | Mode |
|---|---|---|---|
| Hespress (fr) | MA | Français | Batch + Stream |
| Akhbarona | MA | Arabe | Batch + Stream |
| Lakom | MA | Arabe/FR | Batch |
| Barlamane | MA | Arabe/FR | Batch |
| Al Jazeera | QA | Arabe | Batch + Stream |
| CNN | US | Anglais | Batch |
| BBC News | GB | Anglais | Batch + Stream |
| Reuters | US | Anglais | Batch + Stream |

### Données collectées par article

`id` · `source` · `country` · `url` · `title` · `author` · `published_at` · `category` · `content` · `language` · `scraped_at` · `word_count`

### Lancer le spider Scrapy

```bash
cd scrapers/scrapy_project
scrapy crawl news -o articles.json               # tous les sites
scrapy crawl news -a site=hespress -o hess.json  # un site spécifique
```

---

## Transformations Spark

```bash
# Bronze → Silver
docker compose exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  /opt/spark-apps/bronze_to_silver.py

# Silver → Gold
docker compose exec spark-master \
  spark-submit --master spark://spark-master:7077 \
  /opt/spark-apps/silver_to_gold.py
```

---

## Qualité des données

Le module `quality/data_quality_checks.py` vérifie trois dimensions :

| Dimension | Contrôles |
|---|---|
| **Complétude** | `id`, `url`, `title`, `content`, `source`, `scraped_at` présents |
| **Cohérence** | `word_count` vs contenu réel, titre ≥ 5 chars, date non future |
| **Validité** | Source dans liste blanche, langue reconnue, URL valide |

Les checks sont intégrés dans le DAG Airflow et bloquent la pipeline si le taux de complétude passe sous 85 %.

---

## Tableaux de bord Superset

Après connexion sur http://localhost:8088, créer une connexion vers :
```
postgresql+psycopg2://dwuser:dwpass@datawarehouse:5432/newsdb
```

**Dashboards suggérés :**
1. Tendances d'actualité par jour et par source
2. Top mots-clés (nuage de mots, 7 jours glissants)
3. Répartition géographique des articles
4. Évolution par langue (FR / AR / EN)
5. Alertes qualité (taux de complétude)

---

## Gouvernance

```bash
# Générer le rapport de gouvernance
docker compose exec scraper python /app/../governance/data_catalog.py

# Consulter le catalogue
python governance/data_catalog.py
```

Le rapport JSON généré inclut : catalogue des datasets, schémas, propriétaires, lineage des transformations, et historique de qualité.

---

## Commandes utiles

```bash
# Logs d'un service
docker compose logs -f scraper
docker compose logs -f airflow-scheduler

# Arrêter et supprimer les volumes
docker compose down -v

# Shell dans un container
docker compose exec scraper bash
docker compose exec datawarehouse psql -U dwuser -d newsdb

# Requêtes SQL utiles
# -- Top 10 mots-clés aujourd'hui
SELECT keyword, SUM(frequency) AS freq
FROM top_keywords WHERE pub_date = CURRENT_DATE
GROUP BY keyword ORDER BY freq DESC LIMIT 10;

# -- Volume par source
SELECT source, article_count FROM articles_by_day
WHERE pub_date = CURRENT_DATE ORDER BY article_count DESC;
```

---

## Livrables

- [x] Rapport (ce README + rapport PDF séparé)
- [x] `Dockerfile` (scrapers/ + airflow/)
- [x] `docker-compose.yml`
- [x] `README.md`

---

## Auteurs

Projet IADATA — EMSI Casablanca · 2025-2026
Encadrant : Youness Tabii
