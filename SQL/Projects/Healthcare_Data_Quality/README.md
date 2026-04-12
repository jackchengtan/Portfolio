# Healthcare Data Quality Monitoring Project (SQL Server)

## Overview
This project is a **healthcare-inspired data quality monitoring framework** built in **SQL Server** to sit alongside a daily ETL pipeline for synthetic patient encounter data. It simulates how a reporting or analytics team would monitor the quality of loaded healthcare data after ingestion into a curated reporting table.

The solution was designed to demonstrate practical SQL Server skills commonly used in real-world data engineering, reporting, and healthcare analytics environments:

- post-load data quality monitoring
- configurable rule-based validation
- historical metric tracking
- anomaly detection using rolling baselines
- referential integrity and duplicate checks
- issue-level data quality logging
- monitoring-ready summary outputs

This project is intended for portfolio use and uses **fully synthetic data only**.

---

## Business Scenario
A healthcare provider receives a daily file of patient encounter records, which is loaded into SQL Server through an ETL pipeline. Although the ETL process validates and rejects clearly invalid rows during ingestion, the business also needs a **post-load data quality framework** to monitor the integrity and reliability of the curated reporting data over time.

The data quality layer is responsible for:

1. capturing daily data quality metrics after ETL load
2. checking for unexpected changes in record volume
3. monitoring field completeness
4. identifying duplicate business keys in curated data
5. checking referential integrity against dimension tables
6. detecting distribution drift in encounter types
7. flagging unusual cost outliers
8. storing rule results and issue-level details for investigation
9. providing historical outputs for operational monitoring and dashboarding

---

## Project Goal
The goal of this project is to simulate a **production-style SQL Server data quality framework** that works alongside an ETL process and helps identify data issues that may not be caught during raw file validation alone.

This demonstrates how analytics teams can move beyond simple load validation and implement a more mature monitoring layer for:

- trend-based anomaly detection
- business rule control
- data quality auditability
- issue investigation
- daily operational reporting

---

## Technical Design
### Database Structure
The data quality framework is implemented in a dedicated schema:

- **dq**: rule catalog, run tracking, metric snapshots, results, and issue detail

It is designed to work alongside the existing ETL layers:

- **dbo**: curated reporting tables and dimensions
- **stg**: raw and validated staging tables
- **etl**: ETL logs, file audit, and rejected row tracking
- **dq**: post-load data quality monitoring

---

## Main Tables
### Rule and Run Control
- `dq.dq_rule_catalog` – stores configurable data quality rules and thresholds
- `dq.dq_run` – stores one record per data quality execution run

### Monitoring and Results
- `dq.daily_metric_snapshot` – stores daily aggregate metrics for trend analysis
- `dq.dq_result` – stores rule results for each run
- `dq.dq_issue_detail` – stores issue-level detail for failed or warning checks

---

## Data Quality Checks Included
The framework includes multiple categories of checks that are commonly used in production reporting environments.

### 1. Volume Anomaly Detection
Monitors whether the daily encounter volume is significantly higher or lower than recent historical levels using a rolling baseline and standard deviation logic.

Example check:
- daily total encounters outside rolling average ± 2 standard deviations

### 2. Completeness Checks
Measures whether important fields are populated to an acceptable level.

Example check:
- diagnosis code null rate above threshold

### 3. Uniqueness Checks
Confirms that duplicate business keys do not exist in the curated reporting table.

Example business key:
- `encounter_id`
- `patient_id`
- `practice_id`
- `encounter_date`
- `encounter_type_code`
- `clinician_id`

### 4. Referential Integrity Checks
Checks whether codes in the curated data match valid reference values in dimension tables.

Examples:
- `practice_id` must exist in `dbo.dim_practice`
- `encounter_type_code` must exist in `dbo.dim_encounter_type`

### 5. Distribution Drift Checks
Compares the share of encounter types on a given day against recent historical averages to identify unusual shifts in the data mix.

Example:
- sudden spike in one encounter type that may indicate upstream coding or mapping issues

### 6. Cost Outlier Checks
Flags records with unusually high costs compared with recent historical values.

Example:
- cost above rolling average + 3 standard deviations

---

## ETL and DQ Relationship
This project is designed to work **after** the ETL pipeline has completed its daily load.

### ETL handles:
- raw file ingestion
- format and type validation
- invalid row rejection
- deduplication before load
- inserts and updates into curated reporting table
- ETL step logging

### Data Quality handles:
- post-load monitoring of curated data
- historical trend checks
- rule-based anomaly detection
- monitoring outputs for investigation and reporting

This separation reflects a more realistic enterprise design where ETL validation and post-load data quality monitoring are related but distinct processes.

---

## How the Framework Works
### 1. ETL loads curated encounter data
The daily CSV file is first processed by the ETL pipeline and loaded into `dbo.patient_encounter`.

### 2. Daily metric snapshot is captured
A stored procedure calculates daily summary metrics such as:

- total records
- distinct patients
- distinct practices
- distinct clinicians
- average cost
- total cost
- null diagnosis count
- diagnosis null rate

These are stored in `dq.daily_metric_snapshot`.

### 3. Data quality rules are executed
Each active rule is evaluated against the current day’s data and, where relevant, recent historical data.

### 4. Results are stored
Each rule execution writes a result row to `dq.dq_result` with values such as:

- rule code
- rule name
- severity
- metric value
- baseline value
- threshold values
- pass / warn / fail status
- rows affected
- message text

### 5. Issue detail is logged
Where a rule identifies specific problematic records, issue-level detail is written to `dq.dq_issue_detail` for investigation.

### 6. Run summary is tracked
Each execution is stored in `dq.dq_run`, allowing full monitoring of:

- when the framework ran
- how many checks were executed
- how many failed
- how many produced warnings
- whether the run completed successfully

---

## Stored Procedures
### Main execution procedure
- `dq.usp_run_data_quality`

This is the wrapper procedure that executes the full data quality framework for a given date.

### Supporting procedures
- `dq.usp_capture_daily_metrics`
- `dq.usp_check_daily_volume_anomaly`
- `dq.usp_check_completeness`
- `dq.usp_check_duplicate_business_keys`
- `dq.usp_check_referential_integrity`
- `dq.usp_check_distribution_drift`
- `dq.usp_check_cost_outliers`

---

## How to Run the Project
### 1. Run the ETL project first
Make sure the ETL pipeline and all base tables already exist.

### 2. Create the data quality objects
Run the SQL script in SQL Server Management Studio:

- `data_quality_project.sql`

This creates:
- the `dq` schema
- rule catalog table
- run tracking table
- metric snapshot table
- results table
- issue detail table
- all supporting stored procedures

### 3. Execute the data quality framework
Run the main stored procedure manually:

```sql
EXEC dq.usp_run_data_quality @metric_date = '2026-03-30';
