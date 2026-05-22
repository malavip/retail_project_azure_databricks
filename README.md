# Retail — Real-Time Retail Data Engineering Pipeline

An end-to-end data engineering project demonstrating a production-grade retail analytics pipeline on Azure. Ingests data from 4 different sources using 3 distinct ingestion patterns, processes through a medallion architecture (Bronze → Silver → Gold), and serves business-ready KPIs for analytics.


## Architecture

┌──────────────────────────────────────────────────────────────────────────────┐
│                           DATA SOURCES                                       │
│                                                                              │
│  ┌─────────────┐   ┌──────────────┐   ┌─────────────┐   ┌──────────────┐     │
│  │ Azure SQL   │   │ FastAPI POS  │   │ CSV Files   │   │ Azure SQL    │     │
│  │ (Products)  │   │ REST API     │   │ (Inventory) │   │ (Customers)  │     │
│  └──────┬──────┘   └──────┬───────┘   └──────┬──────┘   └──────┬───────┘     │
│         │                 │                  │                 │             │
└─────────┼─────────────────┼──────────────────┼─────────────────┼─────────────┘
          │                 │                  │                 │
          ▼                 ▼                  ▼                 ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                    AZURE DATA FACTORY (Orchestration)                        │
│                                                                              │
│  pl_products_ingest    pl_transactions_ingest   pl_inventory_load            │
│  (SQL → Parquet)       (REST → JSON)            (Event → CSV)                │
│  Manual/Daily          Tumbling 5min            On file arrival              │
│                        + Pagination                                          │
│                        + Bearer Auth                                         │
│  pl_customers_ingest                                                         │
│  (SQL → Parquet)                                                             │
│  Manual/6-hourly                                                             │
└──────────────────────────────────────────┬───────────────────────────────────┘
                                           │
                                           ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                         ADLS GEN2 (Data Lake)                                │
│                                                                              │
│   bronze container         transactions container                            │
│   ├── products.parquet     └── raw/                                          │
│   ├── customers.parquet        └── year=YYYY/month=MM/day=DD/hour=HH/        │
│   ├── inventory/raw/               └── transactions_YYYYMMDD_HHMM.json       │
│   │   └── year=YYYY/month=MM/                                                │
│   ├── products_delta/                                                        │
│   ├── customers_delta/                                                       │
│   ├── inventory_delta/                                                       │
│   └── transactions_delta/                                                    │
│                                                                              │
│   silver container              gold container                               │
│   ├── dim_customers/            ├── daily_revenue_by_store/                  │
│   ├── dim_products/             ├── top_categories_weekly/                   │
│   ├── current_inventory/        ├── customer_lifetime_value/                 │
│   ├── fact_transactions/        └── inventory_at_risk/                       │
│   └── _quarantine/                                                           │
└──────────────────────────────────────────┬───────────────────────────────────┘
                                           │
                                           ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                      DATABRICKS (Processing)                                 │
│                                                                              │
│  Workflow Job: retail_pipeline_full (every 30 min)                           │
│                                                                              │
│  ┌─────────────────────────┐                                                 │
│  │ Bronze Auto Loader      │                                                 │
│  │ (JSON + CSV → Delta)    │                                                 │
│  └───────────┬─────────────┘                                                 │
│         ┌────┼──────────┐                                                    │
│         ▼    ▼          ▼                                                    │
│  ┌──────────┐ ┌────────┐ ┌─────────────┐                                     │
│  │dim_cust  │ │dim_prod│ │current_inv  │                                     │
│  │(SCD2)    │ │(SCD2)  │ │(snapshot)   │                                     │
│  └────┬─────┘ └───┬────┘ └──────┬──────┘                                     │
│       └─────┬─────┘             │                                            │
│             ▼                   │                                            │
│  ┌──────────────────────┐       │                                            │
│  │fact_transactions     │       │                                            │
│  │(enriched + deduped)  │       │                                            │
│  └──────────┬───────────┘       │                                            │
│             └─────────┬─────────┘                                            │
│                       ▼                                                      │
│  ┌──────────────────────────────┐                                            │
│  │ Gold Aggregates              │                                            │
│  │ (revenue, CLV, inventory)    │                                            │
│  └──────────────────────────────┘                                            │
│                                                                              │
│  SQL Warehouse → Analysts / Power BI                                         │
└──────────────────────────────────────────────────────────────────────────────┘

## Technologies Used

| Technology | Purpose |
| Azure Data Factory | Pipeline orchestration, data ingestion from SQL, REST, and file sources |
| Azure Data Lake Storage Gen2 | Centralized data lake with hierarchical namespace |
| Azure Databricks | Spark-based data processing, Auto Loader, Delta Lake |
| Azure SQL Database | Source system for products and customers master data |
| Azure App Service | Hosted mock POS REST API (FastAPI) |
| Delta Lake | ACID-compliant storage format across all layers |
| Python / PySpark | Data transformation and processing logic |
| FastAPI | Mock REST API serving synthetic transaction data |


## Data Sources and Ingestion Patterns

### 1. SQL CDC — Products and Customers

 Azure SQL → ADF Copy Activity → ADLS Parquet → Databricks → Bronze Delta

Products change rarely (price updates, new products). Customers change slowly (address changes, loyalty tier upgrades). ADF copies from SQL using a Copy Activity. In production, this would use watermark-based incremental loading on 'modified_date'.

### 2. REST API with Tumbling Windows — Transactions

 FastAPI POS API → ADF REST Source + Pagination → ADLS JSON → Auto Loader → Bronze Delta

A custom-built FastAPI service simulates a Point-of-Sale system, serving 5,000 synthetic transactions with bearer token authentication, time-window filtering, and paginated responses with `next_page_url`. ADF's tumbling window trigger calls the API every 5 minutes, and the AbsoluteUrl pagination rule follows `$.pagination.next_page_url` until all pages are consumed.

### 3. File-Based Event-Driven — Inventory

SV file upload → ADLS → ADF Storage Event Trigger → Auto Loader → Bronze Delta

Inventory snapshots arrive as CSV files uploaded to ADLS. A storage event trigger detects the new file and fires the pipeline. Auto Loader with Hive-style partitioning processes files incrementally using checkpoints.


## Medallion Architecture

### Bronze Layer — Raw Ingestion

Raw data lands with minimal transformation. Dates stored as strings for resilience to source format drift. Every row includes lineage columns: `ingestion_ts`, `source_system`, `source_file`.

| Table | Source | Rows | Key Feature |
| products_delta | Azure SQL | 50 | Parquet intermediate |
| customers_delta | Azure SQL | 500 | Parquet intermediate |
| inventory_delta | CSV files | 250+ | Hive-style partitioning |
| transactions_delta | REST API | 5,000 | JSON explode from nested array |

### Silver Layer — Cleaned, Joined, Business-Ready

| Table | Pattern | Key Features |
| dim_customers | SCD Type 2 | SHA-256 hash surrogate key, tracks 7 attributes, effective_from/effective_to/is_current |
| dim_products | SCD Type 2 | SHA-256 hash surrogate key, tracks name/category/price/cost |
| current_inventory | Latest Snapshot | Window function (ROW_NUMBER) for most recent per store-SKU |
| fact_transactions | Enriched Fact | Point-in-time SCD2 joins, Delta MERGE dedup, derived metrics |

**Data Quality Patterns:**
- Multi-format date parsing with `try_to_date` and `coalesce`
- Quarantine zones for failed parses and duplicate transactions
- Hash-based change detection for SCD2 versioning
- Two-layer dedup: within-batch (groupBy) + cross-batch (Delta MERGE)

### Gold Layer — Business Aggregates

| Table | Business Question |
| daily_revenue_by_store | Revenue, transaction count, margin per store per day |
| top_categories_weekly | Category performance ranked by weekly revenue |
| customer_lifetime_value | Total spend, transaction frequency, margin per customer |
| inventory_at_risk | Stock status (CRITICAL/LOW/HEALTHY) per store-SKU |

---

## SCD Type 2 Implementation

### Hash-Based Surrogate Keys


customer_sk = SHA-256(customer_id || effective_from || row_hash)


Deterministic, idempotent (reprocessing produces identical keys), distributed-safe (no coordination between Spark executors). The `row_hash` component ensures uniqueness even for same-day attribute changes while maintaining reproducibility.

### Change Detection


row_hash = hash(concat_ws("||", first_name, last_name, email, phone, city, state, loyalty_tier))


If `row_hash` differs between incoming Bronze and current Silver: close old row, insert new version with fresh surrogate key.

### Point-in-Time Fact Table Joins


(transactions.customer_id == dim_customers.customer_id) &
(transactions.transaction_date >= dim_customers.effective_from) &
((transactions.transaction_date < dim_customers.effective_to) | dim_customers.effective_to.isNull())


Ensures each transaction is enriched with the customer's attributes as they were at the time of purchase, not their current state.



## Orchestration

### ADF (External Data Ingestion)

| Pipeline | Trigger | Timeout | Retry |
|---|---|---|---|
| pl_products_ingest | Manual / Daily | Default | None |
| pl_customers_ingest | Manual / 6-hourly | Default | None |
| pl_transactions_ingest | Tumbling window (5 min) | 5 min | 2x, 30s interval |
| pl_inventory_load | Storage event | Default | None |

Azure Monitor alert fires email on pipeline failure or timeout.

### Databricks (Internal Transformation)

Single Workflow job with 6-task DAG:

bronze_autoloader
    ├── dim_customers    (parallel)
    ├── dim_products     (parallel)
    └── current_inventory (parallel)
             │
fact_transactions (waits for both dimensions)
             │
gold_aggregates (waits for fact + inventory)


Schedule: every 30 minutes. Parallel where possible, sequential where dependencies exist.

### Decoupled Architecture

ADF and Databricks run independently, communicating through the data lake. Benefits:
- Ingestion failures don't block transformations
- Bronze serves as a replayable audit log
- Silver/Gold can be rebuilt from Bronze without re-querying sources
- Each layer refreshes at its own cadence


## Mock POS REST API

Custom FastAPI service deployed to Azure App Service (F1 free tier).

**Features:**
- Bearer token authentication (HTTPBearer scheme)
- Time-window filtering with `from`/`to` query parameters
- Pagination with `next_page_url` (full HTTPS URLs for ADF AbsoluteUrl rule)
- HTTPS enforcement behind TLS-terminating load balancer
- 5,000 synthetic transactions with referential integrity to products and customers

**Endpoints:**
- `GET /health` — Health check (no auth)
- `GET /api/v1/transactions` — Paginated transactions (Bearer auth required)
- `GET /docs` — Auto-generated Swagger UI

---

## Project Structure

### Databricks Workspace

Retail_project/
├── bronze/
│   ├── bronze_products_load
│   ├── bronze_customers_load
│   ├── bronze_inventory_autoloader
│   └── bronze_transactions_autoloader
├── silver/
│   ├── silver_dim_customers_scd2
│   ├── silver_dim_products_scd2
│   ├── silver_current_inventory
│   └── silver_fact_transactions
├── gold/
│   └── gold_aggregates

### Azure Resources

rg-retailproject1/
├── retailprojec1data          (ADLS Gen2 — bronze, transactions, silver, gold containers)
├── sql-retailproject1         (Azure SQL Server + Database)
├── retailproject1             (Databricks workspace)
├── adf-retailproject1         (Azure Data Factory)
└── retailpulse-pos-api-9472   (App Service — FastAPI)

## Key Design Decisions

| Decision | Rationale |
| Delta format for all layers | ACID transactions, time travel, schema evolution |
| String-first dates in Bronze | Resilient to source format drift; parsed in Silver with quarantine |
| Hash-based surrogate keys | Idempotent, distributed-safe, no central coordinator needed |
| Decoupled ADF + Databricks | Independent failure domains; Bronze is replayable |
| effective_from = signup_date | Ensures historical coverage for point-in-time joins |
| Tumbling window + timeout | Prevents hung runs; auto-retry handles transient failures |
| Concurrency = 1 | Sequential processing; prevents API overload on free tier |
| Overwrite mode for Gold | Aggregates are derived; rebuilt cheaply from Silver |
| try_to_date over to_date | Handles mixed date formats without pipeline failure |


## Production Enhancements

Documented improvements for scaling beyond portfolio scope:

- Switch surrogate keys to `monotonically_increasing_id()` or UUID for billion-row dimensions
- Add KQL-based pipeline lag monitoring via Log Analytics
- Implement backpressure handling when processing exceeds window duration
- Partition fact tables by `transaction_date` for MERGE optimization
- Switch Auto Loader to continuous mode for sub-minute latency
- Register all tables in Unity Catalog for centralized governance
- Add Great Expectations or DLT Expectations for declarative data quality rules

## How to Run

### Initial Setup

1. Deploy FastAPI POS API to Azure App Service
2. Load products and customers to Azure SQL
3. Run ADF pipelines for products and customers
4. Run Bronze notebooks for products and customers (one-time)
5. Start ADF triggers (tumbling window + storage event)

### Ongoing Pipeline

1. ADF tumbling window fires every 5 minutes (lands JSON)
2. Databricks job "retail_pipeline_full" runs every 30 minutes
3. Gold tables available via SQL Warehouse for analysts

**Built by Malavika** — End-to-end data engineering portfolio project demonstrating Azure Databricks, ADF, Delta Lake, REST API integration, SCD2 dimensions, and medallion architecture.
