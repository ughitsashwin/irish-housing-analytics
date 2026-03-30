# Irish Housing Analytics — dbt + Snowflake Pipeline

An end-to-end data engineering project built with dbt Core and Snowflake,
using Irish CSO residential property price data (2005–2025).

## Stack
- **Snowflake** — cloud data warehouse (AWS, EU Ireland region)
- **dbt Core** — data transformation and testing
- **Python** — data loading via dbt seeds
- **GitHub** — version control

## Architecture
```
CSO Open Data (HPM09)
        ↓
   dbt seed → RAW schema (Snowflake)
        ↓
  stg_property_prices        ← cleaning, type casting, region extraction
        ↓
int_property_prices_pivoted  ← aggregation, month-over-month window functions
        ↓
mart_annual_price_trends     ← annual trends, YoY growth rates (materialised TABLE)
```

## Project Structure
```
models/
  staging/        # 1-to-1 with source, cleaning and standardisation only
  intermediate/   # business logic, window functions, MoM calculations
  marts/          # final tables optimised for analysis and reporting
seeds/            # raw CSO CSV loaded directly into Snowflake
tests/            # data quality test definitions
```

## Key Findings from the Data
The pipeline surfaces 20 years of Irish property market history:

- **2008–2011 crash**: Dublin prices fell 41% in 2009 alone — the steepest 
  single-year drop in the dataset. National prices fell 28% the same year.
- **2012–2013 turning point**: Dublin was the first region to recover, 
  posting +55.6% YoY growth in 2013 while national prices were still negative.
- **Post-COVID surge**: 2021 saw the strongest growth since the recovery 
  began — Dublin +16.8%, National +17.2% — driven by pandemic-era demand shifts.
- **2024 rebound**: After a 2023 dip, both Dublin (+15.7%) and National (+11.4%) 
  posted strong growth, suggesting continued supply constraints.

## Data Quality
- 13 automated dbt tests across all model layers
- Source nulls (2,960 rows) handled explicitly with `severity: warn` — 
  expected gaps in CSO data for certain regions and months
- `try_cast` in staging handles type conversion safely without failing the pipeline
- Only complete years (12 months of data) included in the annual mart

## How to Run
```bash
# Install dependencies
pip install dbt-snowflake

# Activate virtual environment
source venv/bin/activate

# Load raw CSO data into Snowflake
dbt seed

# Build all three model layers
dbt run

# Run all 13 data quality tests
dbt test

# Generate and serve documentation site
dbt docs generate && dbt docs serve
```

## Data Source
[CSO Ireland — Residential Property Price Index (HPM09)](https://data.cso.ie/)
Free and open under the CSO's open data policy. Updated monthly.
