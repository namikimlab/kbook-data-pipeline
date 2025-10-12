# 📚 NL OpenAPI Ingestion to Supabase

Automated ETL pipeline that collects book metadata from the **National Library of Korea’s OpenAPI (서지정보 SearchAPI)**, normalizes it, and loads it into **Supabase Postgres** for use in the [책판(KBooks)](https://github.com/namikimlab/kbooks-site) project.

## 🧭 Overview

This project connects to [`https://www.nl.go.kr/seoji/SearchApi.do`](https://www.nl.go.kr/seoji/SearchApi.do) to collect bibliographic data about published and upcoming books in Korea.

The goal is to:

1. **Fetch** newly registered or updated books daily.
2. **Backfill** older data in chronological windows.
3. **Normalize** and **store** book data in a structured Postgres schema.
4. **Expose** clean, public-safe views to the KBooks frontend.

## 🏗️ Architecture

```
                ┌────────────────────┐
                │  NL OpenAPI (JSON) │
                └─────────┬──────────┘
                          │
                    (psycopg / requests)
                          │
            ┌─────────────┴──────────────┐
            │  raw_nl_books (Bronze)     │
            │  - full jsonb              │
            │  - fetched_at, page_no     │
            └─────────────┬──────────────┘
                          │ transform
                          ▼
            ┌─────────────┴──────────────┐
            │  books_nl (Silver)         │
            │  - normalized columns      │
            │  - isbn13 / fallback_id    │
            │  - dedup + upsert          │
            └─────────────┬──────────────┘
                          │ view
                          ▼
            ┌─────────────┴──────────────┐
            │  books_public (Gold)       │
            │  - public RLS read only    │
            │  - used by Next.js app     │
            └────────────────────────────┘
```

## ⚙️ Flow Design

| Flow                     | Schedule    | Purpose                                       |
| ------------------------ | ----------- | --------------------------------------------- |
| `nl_forward_sync_daily`  | Every night | Pull new books by `INPUT_DATE`                |
| `nl_recent_update_check` | 3-day cycle | Refresh last 14 days for updates              |
| `nl_backfill_weekly`     | Weekly      | Fetch older data by `PUBLISH_PREDATE` windows |
| `nl_health_check`        | Daily       | Validate counts, errors, dedup rates          |

Each run updates a `sync_state` or `backfill_state` table to resume safely.


## 🧩 Key Design Points

* **Incremental ingestion** using `INPUT_DATE` sort order.
* **Idempotent upsert** by `isbn13` or SHA-256 fallback key.
* **Normalization** of titles, authors, ISBNs, and date formats.
* **Language filter** (optional) for Korean books.
* **Cover caching** into Supabase Storage (optional).
* **Retry + backoff** logic for rate limits and API errors.
* **Structured logging & DLQ** for failed records.

## 🗃️ Database Schema

**`raw_nl_books`**

| column        | type        | note                    |
| ------------- | ----------- | ----------------------- |
| id            | bigserial   | primary key             |
| fetched_at    | timestamptz | ingestion timestamp     |
| source_record | jsonb       | full API payload        |
| page_no       | int         | API page number         |
| hash          | text        | md5 or sha256 of record |

**`books_nl`**
Normalized form used by app queries.
Includes: `isbn13`, `title`, `authors`, `publisher`, `publish_date`, `form`, `cover_url`, `ebook_yn`, etc.

**`books_public` (view)**
Publicly readable subset with non-sensitive columns for frontend use.
RLS allows `SELECT` to `anon` and `authenticated`.


## 🔐 Secrets

| Variable                    | Description                  |
| --------------------------- | ---------------------------- |
| `NL_CERT_KEY`               | Your API key from NL OpenAPI |
| `SUPABASE_URL`              | Supabase project URL         |
| `SUPABASE_SERVICE_ROLE_KEY` | Service key for ingestion    |
| `SUPABASE_DB_URL`           | Postgres connection string   |

Store them in `.env` or Secret Manager.
Never commit `.env` to Git.


## 🚀 Run Modes

### 1. Local (manual)

```bash
python fetch_nl_books.py --mode daily --since 20251010
```

### 2. Scheduled

* Cron or Kestra flow triggers the above script.
* Store state in `sync_state` table.



## 📊 Monitoring

* ✅ **Row count trend** (new / updated)
* 🕒 **Average fetch duration**
* ⚠️ **Error ratio**
* 📚 **Top publishers & subjects**
* Tools: Supabase SQL editor, Metabase, or Grafana.



## 🔄 Backfill Strategy

* Crawl by 30-day `PUBLISH_PREDATE` windows (descending).
* Stop when earliest year reached or `TOTAL_COUNT=0`.
* Maintain `backfill_state` table for resuming.



## 🧠 Future Enhancements

* Parallel fetching for faster backfill.
* Smart diffing by `UPDATE_DATE`.
* Supabase function for weekly ranking refresh.
* Data quality dashboard (ISBN validity, missing covers).

## 📎 References

* 📘 API Docs: [국립중앙도서관 서지정보 OpenAPI](https://www.nl.go.kr/contents/N30501030700.do)
* 📦 Supabase: [https://supabase.com/docs](https://supabase.com/docs)
* 🔧 Related repo: [namikimlab/kbooks-site](https://github.com/namikimlab/kbooks-site)


