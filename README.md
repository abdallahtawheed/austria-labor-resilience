# Austrian Labor Market Resilience Analysis

## Research Question
Do Austrian regions with older average populations recover more slowly from 
labor market shocks, and does sector composition (healthcare-heavy vs 
manufacturing-heavy) change that recovery pattern?

## What This Project Does
An end-to-end data pipeline that ingests Austrian regional labor market and 
demographic data, transforms and validates it, runs a forecasting model, and 
serves findings via an interactive dashboard.

## Key Finding
*To be filled in after analysis*

## Pipeline Architecture
*Insert architecture diagram here*

## Project Structure

austria-labor-resilience/
├── data/raw/               # Original source files, never modified
├── data/processed/         # Pipeline outputs
├── ingestion/              # Extract + Load scripts
├── transformation/         # Transform scripts
├── analysis/               # EDA notebooks
├── models/                 # Forecasting code
├── dagster_pipeline/       # Orchestration definitions
├── tests/                  # Unit and data quality tests
├── docs/                   # Architecture diagram and notes
├── docker-compose.yml      # Local infrastructure
└── requirements.txt        # Pinned dependencies

## Data Sources
- Statistik Austria / STATcube: Regional labor market statistics 2010–2023
- Statistik Austria: Regional demographic data 2010–2023

## Tech Stack
- **Orchestration:** Dagster
- **Storage:** PostgreSQL (Docker)
- **Transformation:** Python, pandas, SQL
- **Modeling:** Prophet
- **Visualization:** *Power BI or Plotly Dash — decide later*
- **Testing:** pytest, Great Expectations
- **CI/CD:** GitHub Actions

## How To Run Locally
### Prerequisites
- Docker Desktop installed and running
- Python 3.10+

### Steps
1. Clone the repo
```bash
   git clone https://github.com/abdallahtawheed/austria-labor-resilience.git
   cd austria-labor-resilience
```
2. Start PostgreSQL
```bash
   docker-compose up -d
```
3. Create virtual environment and install dependencies
```bash
   python -m venv venv
   source venv/bin/activate  # Windows: venv\Scripts\activate
   pip install -r requirements.txt
```
4. Run the pipeline
```bash
   dagster dev
```
5. Open Dagster UI at `http://localhost:3000` and trigger the pipeline

## Results
*[To be filled in after analysis]*

## Author
Abdallah — MSc Data Science & AI, FH Joanneum Graz