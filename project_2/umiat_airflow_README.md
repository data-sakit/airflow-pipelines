# UMIAT Airflow DAGs

This project contains Airflow DAGs that load data for the UMIAT reports from Excel files into ClickHouse, plus an email notification DAG.

## DAG list

### 1. `umiat_sklad`
- Source file: `/mnt/shares/umiat/Склад.xlsx`
- Target ClickHouse table: `pixbi.umiat_sklad`
- Reads stock balance with multiple quantity/value columns and adds a date column extracted from the workbook.

### 2. `umiat_spravocn`
- Source file: `/mnt/shares/umiat/Справочник_затратам на ТС.xlsx`
- Target ClickHouse table: `pixbi.umiat_spravocn`
- Loads a simple mapping of cost items to groups.

### 3. `umiat_sto_moyka`
- Source file: `/mnt/shares/umiat/СТО, мойка, шиномонтаж.xlsx`
- Target ClickHouse table: `pixbi.umiat_sto_moyka`
- Loads service / car-wash / tire service aggregated data by object and article, with amounts and record counts.

### 4. `umiat_zadol_client`
- Source file: `/mnt/shares/umiat/Задолженность клиентов.xlsx`
- Target ClickHouse table: `pixbi.umiat_zadol_client`
- Loads client debt structure: total amount, overdue part, buckets by days, etc.

### 5. `umiat_zadol_pered_ka`
- Source file: `/mnt/shares/umiat/Задолженность перед КА.xlsx`
- Target ClickHouse table: `pixbi.umiat_zadol_pered_ka`
- Loads payables to counterparties (suppliers) with overdue buckets.

### 6. `umiat_zakazi_na_ts`
- Source file: `/mnt/shares/umiat/Заказы на ТС.xlsx`
- Target ClickHouse table: `pixbi.umiat_zakazi_na_ts`
- Loads vehicle order statistics: counts by status (open, assigned, rejected, completed), by ownership and customer.

### 7. `umiat_zatrat_sobstv_ts`
- Source file: `/mnt/shares/umiat/Затраты по собственным ТС.xlsx`
- Target ClickHouse table: `pixbi.umiat_zatrat_sobstv_ts`
- Loads cost per own vehicles: document, contractor, project, tariff, time, amount.

### 8. `umiat_omnikom`
- Source file: `/mnt/shares/umiat/Омникомм.xlsx`
- Target ClickHouse table: `pixbi.umiat_omnikom`
- Loads telematics data (Omnicomm): mileage, driving time, fuel level, fuel consumption per hour.

### 9. `umiat_sdacha_pl`
- Source file: `/mnt/shares/umiat/Отчет о сдаче ПЛ.xlsx`
- Target ClickHouse table: `pixbi.umiat_sdacha_pl`
- Loads trip sheet submission report: vehicle, driver, registrar, dates, contractor, trips count.

### 10. `email_ummiat`
- Sends a daily email with a link to the UMIAT report in PixBI.
- Schedule: `30 6 * * *` (every day at 06:30).
- Recipients are defined directly in the DAG file.

## Python dependencies

You need at least:

- apache-airflow
- clickhouse-driver
- pandas
- numpy (optional but commonly used)
- pymssql (not used here but typical in similar projects)
- openpyxl (for reading xlsx via pandas)

Install via pip in your Airflow environment, for example:

```bash
pip install clickhouse-driver pandas numpy pymssql openpyxl
```

## Airflow Variables

Configure these variables in Airflow UI (Admin → Variables) or via environment:

- `CH_HOST`, `CH_PORT`, `CH_USER`, `CH_PASSWORD`

## Recommended Git repository structure

```text
airflow-umiat/
  dags/
    Umiat_sklad_eng.py
    Umiat_spravocn_eng.py
    Umiat_sto_moyka_eng.py
    Umiat_zadol_client_eng.py
    Umiat_zadol_pered_ka_eng.py
    Umiat_zakazi_na_ts_eng.py
    Umiat_zatrat_sobstv_ts_eng.py
    ummiat_email_eng.py
    Umiat_omnikom_eng.py
    Umiat_sdacha_pl_eng.py
  README.md  (rename this file to README.md)
```

Place all `*_eng.py` DAG files into the `dags/` directory of your Airflow Git repository.
