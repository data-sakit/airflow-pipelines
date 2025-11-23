# PARSEC Airflow DAGs (Egypt & Russia)

This project contains Airflow DAGs for processing PARSEC access-control data for Egypt and Russia, loading processed results into ClickHouse, generating HTML/pygwalker reports, and sending daily email notifications.

## Included DAGs

### 1. `parsec_Egypt`
Processes raw access-control event logs for Egypt.

**Functions:**
- Connects to MS SQL (DWH) and extracts PARSEC event data.
- Builds hierarchical org-unit paths.
- Classifies events (entry/exit).
- Computes:
  - Shift (Morning / Evening)
  - Time difference between entry & exit
  - Missing entry/exit errors
  - Activity flag (Yesterday / Not yesterday)
  - Average working hours
- Writes final processed table into ClickHouse: `pixbi.parsec_Eg`.

**Schedule:** `45 5 * * *` (daily at 05:45)

---

### 2. `parsec_RF`
Processes raw access-control event logs for Russian locations.

**Functions:**
- Reads PARSEC events from MS SQL.
- Builds organization-unit paths.
- Splits events into in/out pairs.
- Handles events crossing midnight (splits into two records).
- Computes:
  - Entry/Exit timestamps
  - Shift (Day / Night)
  - Duration HH:MM
  - Missing entry/exit errors
  - Activity flag (Yesterday / Not yesterday)
  - Average duration
- Writes to ClickHouse table: `pixbi.parsec_RF_2`.

**Schedule:** `50 5 * * *` (daily at 05:50)

---

### 3. `Parsec_Egypt_Email`
Sends a daily email report for Egypt PARSEC results.

**Functions:**
- Loads processed ClickHouse data.
- Generates simple HTML summary.
- Sends notification to Egypt recipients list.

**Schedule:** varies by configuration.

---

### 4. `Parsec_Egypt_Email_pygwalker`
Builds advanced analytics report using Pygwalker and emails it.

**Functions:**
- Loads ClickHouse data.
- Generates interactive HTML visualization via Pygwalker.
- Saves file locally to Airflow DAGs folder.
- Sends email with:
  - Link to PixBI shared dashboard
  - Instructions for clearing browser cache if dashboard fails
  - Attached Pygwalker HTML file

**Schedule:** `30 7 * * *` (daily at 07:30)

---

## Common Configuration

### Required Airflow Variables
These must be defined in **Admin → Variables**:

| Variable | Description |
|---------|-------------|
| `CH_HOST` | ClickHouse host |
| `CH_PORT` | ClickHouse port |
| `CH_USER` | ClickHouse username |
| `CH_PASSWORD` | ClickHouse password |
| `MS_SERVER` | MS SQL server |
| `MS_USER` | MS SQL username |
| `MS_PASSWORD` | MS SQL password |

---

## Python Dependencies

Install these libraries in the Airflow environment:

```bash
pip install clickhouse-driver pandas numpy pymssql openpyxl pygwalker pillow matplotlib
```

---

## Recommended Repository Structure

```
airflow-parsec/
  dags/
    parsec_Egypt_eng.py
    parsec_RF_eng.py
    Parsec_Egypt_Email_eng.py
    Parsec_Egypt_Email_pygwalker_eng.py
  README.md
```

Place all *_eng.py files inside **dags/** and rename this file to **README.md**.

---

## Notes
- All Russian labels inside the code were translated to English equivalents.
- SQL queries still contain Cyrillic text only where necessary for matching MS SQL source system values.
- For full English transformation of source SQL text, ensure source data uses English codes/names.

