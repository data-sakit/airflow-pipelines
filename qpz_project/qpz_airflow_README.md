# QPZ Airflow DAGs

This project contains several Airflow DAGs for loading data from MS SQL into ClickHouse and building QPZ reports.

## DAG list

### 1. `plan_app_dag`
- Source: MS SQL table `dbo.app_internal_order_plan_item`
- Target: ClickHouse table `pixbi.PLAN_APP`
- Runs daily (`@daily`)
- Technologies: `pymssql`, `clickhouse_driver`, `pandas`
- Uses Airflow Variables:
  - `MS_SERVER`
  - `MS_USER`
  - `MS_PASSWORD`
  - `CH_HOST`
  - `CH_PORT`
  - `CH_USER`
  - `CH_PASSWORD`

### 2. `plan_app_enl_dag`
- Source: MS SQL table `dbo.app_enlarged_nomenclature_plan_item`
- Target: ClickHouse table `pixbi.PLAN_APP_ENL`
- Runs daily (`@daily`)
- Uses the same MS SQL and ClickHouse connection variables as `plan_app_dag`.

### 3. `fact_app_dag`
- Source: MS SQL table `dbo.app_internal_order_fact_item`
- Target: ClickHouse table `pixbi.FACT_APP`
- Runs daily (`@daily`)
- Uses the same MS SQL and ClickHouse connection variables.

### 4. `qpz_project`
- Builds aggregated ClickHouse table `pixbi.qpz_project` from:
  - `pixbi.PLAN_APP_ENL`
  - `pixbi.PLAN_APP`
  - `pixbi.FACT_APP`
  - `pixbi.months_list`
- Runs every day at 01:00 server time (`00 1 * * *`).
- Contains ClickHouse `INSERT INTO ... SELECT` with several `UNION ALL` parts and business rules.

### 5. `QPZ_ETL_reports_25`
- Loads data from `pixbi.qpz_project`.
- Filters data for year 2025.
- Joins Excel mapping file `/opt/airflow/dags/QPZ_ETL_Image/qpz_s_2.xlsx`.
- Builds plots with matplotlib (PNG images encoded as base64).
- Intended for creating reporting visuals for 2025.

### 6. `QPZ_KT2_MSU90_SEM_SUS_25`
- Also loads data from `pixbi.qpz_project` for 2025.
- Splits data by business groups (KT2, MSU-90, SEM, SUS, etc.).
- Builds multiple visualizations (bar charts, cumulative lines, top-10 lists).
- Sends emails with embedded images to several email groups.

## Python dependencies

You need at least:

- `apache-airflow`
- `clickhouse-driver`
- `pymssql`
- `pandas`
- `numpy`
- `matplotlib`
- `Pillow`

Install via `pip` in your Airflow environment, for example:

```bash
pip install clickhouse-driver pymssql pandas numpy matplotlib pillow
```

## Airflow Variables

Configure these variables in Airflow UI (Admin → Variables) or via environment:

- `CH_HOST`, `CH_PORT`, `CH_USER`, `CH_PASSWORD`
- `MS_SERVER`, `MS_USER`, `MS_PASSWORD`

## Project structure in Git

Recommended repository structure:

```text
airflow-qpz/
  dags/
    qpz_ETL_eng.py
    qpz_KT2_MSU90_SEM_SUS_25_Email_eng.py
    qpz_plan_eng.py
    qpz_plan_enl_eng.py
    qpz_project_eng.py
    qpz_fact_eng.py
  README.md  (you can rename qpz_airflow_README.md to README.md)
```

Place all translated/cleaned DAG files into the `dags/` directory of your Airflow Git repository.

