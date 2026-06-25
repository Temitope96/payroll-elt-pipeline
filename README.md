# Global Payroll ELT Pipeline

An end-to-end ELT pipeline for global payroll data processing, built with Apache Airflow, dbt, and PostgreSQL on a Dockerized infrastructure. Processes payroll transactions for 500 employees across 10 countries.

---

## Architecture

```
Raw CSV Files
     │
     ▼
┌─────────────────────────────────────────┐
│           Apache Airflow DAG            │
│  Task 1: ingest_raw_data                │
│  Task 2: validate_files                 │
│  Task 3: load_to_staging                │
│  Task 4: trigger_dbt_run                │
└─────────────────────────────────────────┘
     │
     ▼
┌─────────────────┐
│   PostgreSQL    │  ← staging schema (employees, pay_periods, transactions)
└─────────────────┘
     │
     ▼
┌─────────────────────────────────────────┐
│              dbt Layers                 │
│  Staging      → stg_employees           │
│                 stg_pay_periods         │
│                 stg_transactions        │
│  Intermediate → int_payroll_enriched    │
│  Marts        → payroll_summary         │
│                 country_totals          │
└─────────────────────────────────────────┘
     │
     ▼
Analytics-ready tables for BI reporting
```

---

## Tech Stack

| Layer           | Tool           | Version |
| --------------- | -------------- | ------- |
| Orchestration   | Apache Airflow | 2.8.1   |
| Transformation  | dbt Core       | Latest  |
| Database        | PostgreSQL     | 15      |
| Infrastructure  | Docker Compose | -       |
| Language        | Python         | 3.11    |
| Data Generation | Faker          | Latest  |
| Version Control | Git / GitHub   | -       |

---

## Project Structure

```
payroll-elt-pipeline/
├── dags/
│   └── payroll_elt_dag.py        ← Airflow DAG (4 tasks)
├── ingestion/
│   ├── generate_data.py          ← Synthetic data generator (Faker)
│   └── load_to_postgres.py       ← CSV → PostgreSQL loader
├── dbt/
│   └── payroll_dbt/
│       └── models/
│           ├── staging/
│           │   ├── sources.yml
│           │   ├── stg_employees.sql
│           │   ├── stg_pay_periods.sql
│           │   └── stg_transactions.sql
│           ├── intermediate/
│           │   └── int_payroll_enriched.sql
│           └── marts/
│               ├── payroll_summary.sql
│               └── country_totals.sql
├── data/                         ← Generated CSVs (gitignored)
├── docs/                         ← Architecture diagrams
├── plugins/                      ← Airflow plugins
├── docker-compose.yml
└── .gitignore
```

---

## How to Run

### Prerequisites

- Docker Desktop installed and running
- Python 3.11+
- Git

### 1. Clone the repository

```bash
git clone https://github.com/Temitope96/payroll-elt-pipeline.git
cd payroll-elt-pipeline
```

### 2. Start Docker containers

```bash
docker-compose up airflow-init
docker-compose up -d airflow-webserver airflow-scheduler postgres
```

Wait ~30 seconds, then go to `http://localhost:8080` (login: `admin` / `admin`)

### 3. Install Python dependencies in containers

```bash
docker exec payroll-elt-pipeline-airflow-scheduler-1 pip install faker pandas sqlalchemy psycopg2-binary
docker exec payroll-elt-pipeline-airflow-webserver-1 pip install faker pandas sqlalchemy psycopg2-binary
```

### 4. Set up local Python environment

```bash
python -m venv venv
source venv/Scripts/activate    # Windows Git Bash
pip install faker pandas psycopg2-binary sqlalchemy dbt-postgres
```

### 5. Generate and load data locally (optional — Airflow does this automatically)

```bash
cd ingestion
python generate_data.py
python load_to_postgres.py
```

### 6. Trigger the Airflow DAG

- Go to `http://localhost:8080`
- Find `payroll_elt_pipeline`
- Click the ▶ play button to trigger a manual run
- Watch all 4 tasks turn green in the Graph view

### 7. Run dbt models manually (optional)

```bash
cd dbt/payroll_dbt
dbt debug      # test connection
dbt run        # run all models
dbt test       # run data quality tests
```

### 8. Stop the environment

```bash
docker-compose down        # stops containers, preserves data
docker-compose down -v     # stops containers AND deletes data
```

---

## DAG Overview

The `payroll_elt_pipeline` DAG runs on a `@daily` schedule with the following task sequence:

```
ingest_raw_data → validate_files → load_to_staging → trigger_dbt_run
```

| Task              | Type           | Description                                  |
| ----------------- | -------------- | -------------------------------------------- |
| `ingest_raw_data` | PythonOperator | Generates synthetic payroll CSVs using Faker |
| `validate_files`  | PythonOperator | Checks all 3 files exist and are non-empty   |
| `load_to_staging` | PythonOperator | Loads CSVs into PostgreSQL staging schema    |
| `trigger_dbt_run` | BashOperator   | Triggers dbt run + test on all models        |

**Retry logic:** 1 retry with 1-minute delay on all tasks.

---

## dbt Models

### Staging layer (materialized as views)

Clean and cast raw data — no business logic.

| Model              | Source               | Description                                               |
| ------------------ | -------------------- | --------------------------------------------------------- |
| `stg_employees`    | staging.employees    | Cleaned employee records with full_name, lowercased email |
| `stg_pay_periods`  | staging.pay_periods  | Cast date columns, filtered nulls                         |
| `stg_transactions` | staging.transactions | Uppercased status and currency_code, cast numerics        |

### Intermediate layer (materialized as table)

Join and enrich — business logic lives here.

| Model                  | Description                                                              |
| ---------------------- | ------------------------------------------------------------------------ |
| `int_payroll_enriched` | Joins transactions + employees + pay_periods. Adds `is_successful` flag. |

### Mart layer (materialized as tables)

Analytics-ready aggregations for BI consumption.

| Model             | Description                                                        |
| ----------------- | ------------------------------------------------------------------ |
| `payroll_summary` | Headcount, gross/net pay totals by country, department, period     |
| `country_totals`  | Total employees, transactions, and pay volumes by country and year |

---

## Data Quality

dbt tests covering:

- Primary key uniqueness on all staging tables
- Not-null constraints on all critical fields
- Referential integrity (transactions → employees)
- Accepted values (country codes, transaction status)

Run tests with:

```bash
dbt test
```

---

## Design Decisions

**ELT over ETL** — raw data is loaded into PostgreSQL first before transformation. This preserves the original data for auditability and reprocessing.

**`_loaded_at` metadata column** — added to all staging tables at load time via Python (not a database default). Records exactly when each batch was loaded, enabling incremental load strategies and debugging.

**Environment-aware DB connection** — `load_to_postgres.py` reads `DB_HOST` from environment variables. Defaults to `localhost` for local runs, uses `postgres` (Docker service name) when called from Airflow inside Docker.

**`sys.executable` in DAG** — uses the same Python interpreter that Airflow is running rather than hardcoding a path, making the DAG portable across environments.

**Retry logic on all tasks** — 1 retry with 1-minute delay. Protects against transient failures (network blips, container startup timing).

**`catchup=False`** — prevents Airflow from backfilling historical runs when the DAG is first enabled or after a pause.

---

## Environment Variables

| Variable          | Default     | Description                                    |
| ----------------- | ----------- | ---------------------------------------------- |
| `DB_HOST`         | `localhost` | PostgreSQL host (use `postgres` inside Docker) |
| `DATA_OUTPUT_DIR` | `../data`   | Directory for generated CSV files              |

---

## Troubleshooting

**Docker not starting:**
Make sure Docker Desktop is open and the whale icon in your taskbar is steady (not spinning).

**`faker` module not found in Airflow:**

```bash
docker exec payroll-elt-pipeline-airflow-scheduler-1 pip install faker pandas sqlalchemy psycopg2-binary
```

**`could not translate host name "postgres"`:**
You're running the script locally. Either use `localhost` or set `DB_HOST=localhost`.

**Permission denied on `/opt/airflow/data`:**

```bash
docker exec -u root payroll-elt-pipeline-airflow-scheduler-1 chmod 777 //opt/airflow/data
```

**dbt `_loaded_at` column does not exist:**
Re-run `load_to_postgres.py` after the `_loaded_at` fix was applied.

---

## Resume Checklist

If starting a new session, run these in order:

```bash
# 1. Start Docker Desktop (wait for whale icon)
# 2. Start containers
docker-compose up -d

# 3. Confirm Airflow is up
# Go to http://localhost:8080

# 4. Activate virtual environment
source venv/Scripts/activate

# 5. Continue from where you left off
```

---

## What's Next

- [ ] Replace dbt placeholder task in Airflow with real `dbt run && dbt test`
- [ ] Add dbt schema tests (`schema.yml`) for all models
- [ ] Add `dbt docs generate` and host lineage graph
- [ ] Add Great Expectations data quality layer
- [ ] Write architecture diagram in `docs/`
- [ ] Add GitHub Actions CI to run `dbt test` on every push

---

## Author

**Temitope Mafimidiwo** — Data Engineer
[LinkedIn](https://linkedin.com/in/temitope-mafii-319ab416a) · [GitHub](https://github.com/Temitope96)
