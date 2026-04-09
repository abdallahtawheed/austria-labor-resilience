# Austrian Labor Market Resilience Analysis

> Do Austrian regions with older average populations recover more slowly from labor market shocks — and does sector composition change that recovery pattern?

**Live API:** `http://13.60.6.29:8000`  
**Swagger UI:** `http://13.60.6.29:8000/docs`

---

## Key Findings

- Demographic aging correlates with slower post-COVID employment recovery across Austrian regions
- Vienna is a structural outlier — youngest demographics, lowest manufacturing exposure, strongest recovery at **+7.74%**
- Once Vienna is excluded, sector composition explains nothing — the manufacturing/healthcare gap collapses from 1.38 to -0.03 percentage points
- Carinthia took the hardest COVID shock at **-3.47%**
- The 2021 ILO questionnaire change creates a structural break — pre and post-2021 figures are not directly comparable

---

## Architecture

```
Statistik Austria (STATcube)
        │
        ▼
Python + SQLAlchemy (ingestion)
        │
        ▼
AWS RDS PostgreSQL 15 (raw layer)
        │
        ▼
dbt (staging → intermediate → marts)
        │
        ├──▶ transformed_labour (117 rows, 11 cols)
        └──▶ recovery_scores (9 rows, 8 cols)
                │
                ├──▶ Prophet forecasting → model_output (2026–2028)
                │
                ├──▶ FastAPI on AWS EC2 (REST endpoints)
                │
                └──▶ Power BI Dashboard (4 analytical views)

Dagster (monthly orchestration) + GitHub Actions (CI/CD)
```

![Architecture](docs/architecture.png)
![Lineage Graph](docs/lineage_graph.png)

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.12 |
| Package manager | uv |
| Database | PostgreSQL 15 on AWS RDS |
| Ingestion | pandas, SQLAlchemy, psycopg2-binary |
| Transformation | dbt-postgres 1.10 |
| Forecasting | Prophet |
| API | FastAPI + uvicorn on AWS EC2 |
| Orchestration | Dagster (monthly schedule) |
| Dashboard | Power BI Desktop |
| Testing | pytest (19 tests) + dbt schema tests (7 tests) |
| CI/CD | GitHub Actions |

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/` | Project metadata |
| GET | `/regions` | All nine Bundesländer with recovery scores |
| GET | `/regions/{region}/forecast` | Prophet forecast 2026–2028 for a region |
| GET | `/regions/{region}/recovery` | Full demographic and recovery breakdown |

Example:
```
GET http://13.60.6.29:8000/regions/Vienna/forecast
```

Full interactive documentation at **`http://13.60.6.29:8000/docs`**.

---

## Data Sources

All data from [Statistik Austria STATcube](https://statcube.at/statistik.at/ext/statcube/jsf/dataCatalogueExplorer.xhtml), CSV format, latin1 encoding.

| File | Content | Rows |
|---|---|---|
| `raw_sector_employment` | Employment by sector (Manufacturing, Healthcare) by region by year | 234 |
| `raw_total_employment` | Total employment by region by year | 117 |
| `raw_age_demographics` | Population by age band by region by year | 819 |

Coverage: 9 Austrian Bundesländer (NUTS 2), 2013–2025.

---

## dbt Transformation Layer

```
models/
├── staging/
│   ├── stg_total_employment.sql       # Cleans raw employment table
│   ├── stg_sector_employment.sql      # Pivots manufacturing/healthcare by region
│   └── stg_age_demographics.sql       # Bucketes age bands into under_15/working_age/over_65
├── intermediate/
│   └── int_labour_joined.sql          # Joins all three staging models, computes shares and ratios
└── marts/
    ├── transformed_labour.sql         # Final analytical table (materialized)
    └── recovery_scores.sql            # One row per region with shock and recovery metrics (materialized)
```

Staging and intermediate models materialize as views. Mart models materialize as tables.

Run the full pipeline:
```bash
dbt run
dbt test
```

---

## Database Schema

**Raw layer** (ingested from STATcube CSVs):
- `raw_sector_employment` — 234 rows, 8 cols
- `raw_age_demographics` — 819 rows, 7 cols
- `raw_total_employment` — 117 rows, 7 cols

**Analytical layer** (built by dbt):
- `transformed_labour` — 117 rows, 11 cols: `year, region, total_employed, manufacturing_employed, healthcare_employed, manufacturing_share, healthcare_share, pop_under_15, pop_working_age, pop_over_65, old_age_dependency_ratio`
- `recovery_scores` — 9 rows, 8 cols: `region, employed_2019, employed_2020, employed_2023, old_age_dependency_ratio, manufacturing_share, healthcare_share, shock_magnitude, recovery_score`

**Model layer**:
- `model_output` — 117 rows, 6 cols: `year, region, yhat, yhat_lower, yhat_upper, is_forecast`

---

## Local Setup

### Prerequisites
- Python 3.12
- uv
- Access to AWS RDS instance (credentials required)
- dbt-postgres

### Install

```bash
git clone https://github.com/abdallahtawheed/austria-labor-resilience.git
cd austria-labor-resilience
uv venv --python 3.12
uv sync
```

### Configure environment

Copy `.env.example` to `.env` and fill in your RDS credentials:

```bash
cp .env.example .env
```

```env
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=your-rds-endpoint.rds.amazonaws.com
DB_PORT=5432
DB_NAME=postgres
```

### Configure dbt

```bash
dbt init dbt_labour
```

Follow the prompts using the same RDS credentials.

### Run the pipeline

```bash
# Ingest raw data
python ingestion/ingest_labour.py

# Run dbt transformations and tests
cd dbt_labour
dbt run
dbt test

# Run Prophet forecasting
cd ..
python models/forecast_employment.py
```

### Run tests

```bash
pytest tests/ -v
```

---

## Orchestration

Dagster orchestrates the full pipeline on a monthly schedule (`0 6 1 * *`):

```
raw_data → dbt_models (run + test) → forecast_data
```

Launch the Dagster UI:

```bash
dagster dev
```

---

## CI/CD

GitHub Actions runs on every push and PR to `main`:

1. Spins up the environment with Python 3.12 and uv
2. Writes dbt `profiles.yml` from GitHub Secrets
3. Runs dbt models and schema tests against live AWS RDS
4. Runs 19 pytest tests (unit + data quality)

All secrets stored as GitHub repository secrets: `DB_HOST`, `DB_USER`, `DB_PASSWORD`, `DB_PORT`, `DB_NAME`.

![CI Pipeline](https://github.com/abdallahtawheed/austria-labor-resilience/actions/workflows/ci.yml/badge.svg)

---

## Known Limitations

- **2021 methodology break** — ILO questionnaire change makes pre/post 2021 figures not directly comparable
- **Only 9 regions** — too small for statistical inference; findings are descriptive only
- **Migration data missing** — healthcare employment likely sustained by foreign workers, not captured in this dataset

---

## Project Structure

```
austria-labor-resilience/
├── ingestion/
│   ├── ingest_labour.py
│   └── ingest_labour_ci.py
├── transformation/
│   └── transform_labour.py
├── models/
│   └── forecast_employment.py
├── analysis/
│   └── eda.ipynb
├── dbt_labour/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   ├── dbt_project.yml
│   └── ...
├── api/
│   ├── main.py
│   └── requirements.txt
├── dagster_pipeline/
│   ├── assets.py
│   └── definitions.py
├── tests/
│   ├── test_transformations.py
│   └── test_data_quality.py
├── dashboard/
│   └── austria_labour_resilience.pbix
├── docs/
│   ├── architecture.png
│   ├── lineage_graph.png
│   └── ...
├── data/
│   ├── raw/                  # gitignored
│   └── samples/
├── .github/workflows/ci.yml
├── docker-compose.yml        # retained for local dev reference
├── pyproject.toml
└── .env.example
```


---

## Author

Abdallah Abdelmagid — MSc Data Science & AI, FH Joanneum Graz  
[LinkedIn](https://www.linkedin.com/in/abdallah-abdelmagid-8b5549175/) | 
[GitHub](https://github.com/abdallahtawheed)

---