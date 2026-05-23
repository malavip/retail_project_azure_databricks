# RetailPulse — Real-Time Retail Data Engineering Pipeline

An end-to-end data engineering project demonstrating a production-grade retail analytics pipeline on Azure. Ingests data from 4 different sources using 3 distinct ingestion patterns, processes through a medallion architecture (Bronze → Silver → Gold), and serves business-ready KPIs for analytics.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           DATA SOURCES                                       │
│                                                                              │
│  ┌─────────────┐   ┌──────────────┐   ┌─────────────┐   ┌──────────────┐     │
│  │ Azure SQL   │   │ FastAPI POS  │   │ CSV Files   │   │ Azure SQL    │     │
│  │ (Products)  │   │ REST API     │   │ (Inventory) │   │ (Customers)  │     │
│  └──────┬──────┘   └──────┬───────┘   └──────┬──────┘   └──────┬───────┘     │
│         │                 │                   │                  │           │
└─────────┼─────────────────┼───────────────────┼──────────────────┼───────────┘
          │                 │                   │                  │
          ▼                 ▼                   ▼                  ▼
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
│  Workflow Jobs (3 jobs, cadence-optimized):                                  │
│                                                                              │
│  Job 1: retail_master_products (daily 2:30 AM)                               │
│  Job 2: retail_master_customers (every 6 hrs, 30-min offset)                 │
│  Job 3: retail_pipeline_transactions (every 30 min)                          │
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
|---|---|
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

Products change rarely (price updates, new products). Customers change slowly (address changes, loyalty tier upgrades). ADF copies from SQL using a Copy Activity. In production, this would use watermark-based incremental loading on `modified_date`.

### 2. REST API with Tumbling Windows — Transactions

 FastAPI POS API → ADF REST Source + Pagination → ADLS JSON → Auto Loader → Bronze Delta

A custom-built FastAPI service simulates a Point-of-Sale system, serving 5,000 synthetic transactions with bearer token authentication, time-window filtering, and paginated responses with `next_page_url`. ADF's tumbling window trigger calls the API every 5 minutes, and the AbsoluteUrl pagination rule follows `$.pagination.next_page_url` until all pages are consumed.

### 3. File-Based Event-Driven — Inventory

CSV file upload → ADLS → ADF Storage Event Trigger → Auto Loader → Bronze Delta

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
| pl_products_ingest | Manual / Daily | Default | None |
| pl_customers_ingest | Manual / 6-hourly | Default | None |
| pl_transactions_ingest | Tumbling window (5 min) | 5 min | 2x, 30s interval |
| pl_inventory_load | Storage event | Default | None |

Azure Monitor alert fires email on pipeline failure or timeout.

### Databricks (Internal Transformation)

Three Workflow jobs, each scheduled by data change frequency to avoid wasteful compute:

**Job 1: `retail_master_products` (Daily at 2:30 AM)**


bronze_products_load → silver_dim_products_scd2


Products change rarely (price updates, new SKUs). Scheduled 30 minutes after the ADF products refresh (2:00 AM) to ensure fresh Parquet is available. Running SCD2 every 30 minutes would waste compute on guaranteed no-ops.

**Job 2: `retail_master_customers` (Every 6 hours, offset by 30 min)**


bronze_customers_load → silver_dim_customers_scd2


Customer attributes change slowly (address moves, loyalty tier upgrades). Runs at 12:30 AM, 6:30 AM, 12:30 PM, 6:30 PM — 30 minutes after ADF refreshes customer Parquet from SQL. A 6-hourly cadence balances freshness with efficiency.

**Job 3: `retail_pipeline_transactions` (Every 30 minutes)**


bronze_transactions_autoloader
    ├── silver_current_inventory (parallel)
    └── silver_fact_transactions (parallel)
              │
        gold_aggregates

Transactions are continuously generated. The fact table reads existing Silver dimensions (maintained by Jobs 1 and 2) without rebuilding them — it only needs fresh dimensions, not freshly-rebuilt dimensions.

**Typical daily timeline:**


 2:00 AM  — ADF refreshes products Parquet from SQL
 2:30 AM  — Job 1: products Bronze + SCD2 refresh

 6:00 AM  — ADF refreshes customers Parquet from SQL
 6:30 AM  — Job 2: customers Bronze + SCD2 refresh
 
 Throughout the day (every 30 min):
  7:00   — Job 3: transactions → fact → gold
  7:30   — Job 3: transactions → fact → gold
  ...
 12:00 PM — ADF refreshes customers Parquet from SQL
 12:30 PM — Job 2: customers refresh
  1:00 PM — Job 3: fact table now uses fresh customer data

  6:00 PM — ADF refreshes customers Parquet from SQL
  6:30 PM — Job 2: customers refresh


Each ADF trigger runs 30 minutes before its corresponding Databricks job, providing a safe buffer for retries and connection overhead.

### Fact Table Refresh Strategy

For the current data volume, the fact table uses full overwrite on each run, ensuring dimension attribute updates are reflected automatically. When a dimension hasn't refreshed yet (e.g., a new customer was added to SQL but the 6-hourly customer job hasn't run), transactions for that customer temporarily have NULL dimension attributes. On the next run after the dimension refreshes, the fact table rebuilds from Bronze with fresh joins — NULLs are replaced with correct attributes automatically. No special NULL-fixing logic is needed.

At enterprise scale (100M+ rows), the approach would shift to incremental processing for the 30-minute runs (processing only new transactions via Auto Loader checkpoints or watermarks) with a daily full refresh to resolve any NULL dimension attributes caused by dimension refresh lag. This balances real-time freshness with compute efficiency.

**Processing model by layer:**

| Layer | Processing Mode | Reason |
| Bronze Auto Loader | Incremental (checkpoint) | Only new files processed, never reprocesses |
| Silver Fact Table | Full overwrite | Ensures dimension updates propagate, NULLs self-heal |
| Gold Aggregates | Full overwrite | Derived from Silver, cheap to rebuild |

### Decoupled Architecture

ADF and Databricks run independently, communicating through the data lake. Benefits:
- Ingestion failures don't block transformations
- Bronze serves as a replayable audit log
- Silver/Gold can be rebuilt from Bronze without re-querying sources
- Each layer refreshes at its own cadence
- Master data jobs (daily/6-hourly) separated from transactional jobs (30-min) to minimize compute waste



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



## Project Structure

### Databricks Workspace


RetailPulse/
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
├── setup/
│   └── register_gold_tables
└── utilities/
    ├── clean_slate_reset
    └── data_quality_checks


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
| Full overwrite for fact table | Ensures dimension updates propagate; NULLs from dimension lag self-heal on next run |
| Separate jobs by cadence | Products daily, customers 6-hourly, transactions 30-min — avoids wasteful no-op runs |
| ADF runs 30 min before Databricks | Ensures fresh Parquet is available when Bronze notebooks execute |


## Production Enhancements

Documented improvements for scaling beyond portfolio scope:

- **Incremental fact table processing:** At 100M+ rows, switch 30-minute runs to incremental (process only new transactions via watermark). Add a daily full-refresh job at 3 AM to resolve NULL dimension attributes caused by dimension refresh lag. This reduces 30-minute run cost by ~90% while maintaining data completeness.
- **Surrogate key generation:** Switch from `row_number()` to `monotonically_increasing_id()` or hash-based for billion-row dimensions to avoid single-node shuffle bottleneck.
- **Pipeline lag monitoring:** KQL queries against Log Analytics to alert when tumbling window processing falls behind real-time.
- **Backpressure handling:** If processing time exceeds window duration, increase concurrency or widen window interval dynamically.
- **Fact table partitioning:** Partition by `transaction_date` and use MERGE with partition filters for targeted updates instead of full-table scans.
- **Auto Loader continuous mode:** Switch from `trigger(availableNow=True)` to continuous streaming for sub-minute Bronze latency.
- **Unity Catalog integration:** Register all tables as managed catalog tables for centralized governance, lineage tracking, and fine-grained access control.
- **Declarative data quality:** Add Great Expectations or DLT Expectations for automated quality checks with alerting on threshold breaches.



## How to Run

### Initial Setup

1. Deploy FastAPI POS API to Azure App Service
2. Load products and customers to Azure SQL
3. Run ADF pipelines `pl_products_ingest` and `pl_customers_ingest`
4. Run Bronze notebooks `bronze_products_load` and `bronze_customers_load` (one-time)
5. Start ADF triggers (tumbling window for transactions, storage event for inventory)
6. Upload initial inventory CSV files

### ADF Triggers

| Trigger | Type | Frequency | Pipeline |
| `trg_pos_transactions_5min` | Tumbling window | Every 5 min | pl_transactions_ingest |
| Storage event trigger | Storage event | On file arrival | pl_inventory_load |
| `trg_products_daily` | Schedule | Daily 2:00 AM | pl_products_ingest |
| `trg_customers_6hourly` | Schedule | Every 6 hrs (12 AM, 6 AM, 12 PM, 6 PM) | pl_customers_ingest |

ADF triggers run 30 minutes before corresponding Databricks jobs to ensure fresh Parquet is available.

### Databricks Jobs

| Job | Schedule | Tasks | What It Does |
| `retail_master_products` | Daily 2:30 AM | 2 | Bronze + SCD2 for products |
| `retail_master_customers` | 12:30 AM, 6:30 AM, 12:30 PM, 6:30 PM | 2 | Bronze + SCD2 for customers |
| `retail_pipeline_transactions` | Every 30 min | 4 | Bronze Auto Loader → fact table → Gold |

### Ongoing Pipeline

1. ADF tumbling window fires every 5 min (lands transaction JSON)
2. ADF schedule triggers refresh products/customers Parquet from SQL
3. Databricks `retail_master_products` runs daily (SCD2 dimension refresh)
4. Databricks `retail_master_customers` runs every 6 hours (SCD2 dimension refresh)
5. Databricks `retail_pipeline_transactions` runs every 30 min (fact table + Gold)
6. Gold tables available via SQL Warehouse for analysts / Power BI




## Sample Queries

-- Revenue by store
SELECT store_location, SUM(total_revenue) as revenue
FROM gold.daily_revenue_by_store
GROUP BY store_location ORDER BY revenue DESC;

-- Top 10 customers
SELECT customer_name, loyalty_tier, total_spent, transaction_count
FROM gold.customer_lifetime_value
ORDER BY total_spent DESC LIMIT 10;

-- Critical inventory
SELECT store_id, product_name, quantity_on_hand, stock_status
FROM gold.inventory_at_risk
WHERE stock_status IN ('CRITICAL', 'OUT_OF_STOCK');

End-to-end data engineering project demonstrating Azure Databricks, ADF, Delta Lake, REST API integration, SCD2 dimensions with hash-based surrogate keys, medallion architecture, and cadence-optimized orchestration across 4 ADF pipelines and 3 Databricks Workflow jobs.
