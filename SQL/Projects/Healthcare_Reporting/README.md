# Healthcare Reporting & Insights Layer (SQL Server)

## Overview
This project is a **healthcare-inspired reporting and analytics layer** built in **SQL Server**, designed to sit on top of an ETL pipeline and a data quality monitoring framework. It transforms curated patient encounter data into **business-ready insights**, supporting operational reporting, executive dashboards, and trend analysis.

The solution demonstrates practical SQL Server and analytics skills used in real-world BI and reporting environments:

- reporting data mart design
- KPI modelling and aggregation
- time-series and trend analysis
- ranking and performance benchmarking
- variance and rolling metrics
- integration with data quality outputs
- dashboard-ready views and snapshot tables

This project is intended for portfolio use and uses **fully synthetic data only**.

---

## Business Scenario
A healthcare organisation collects daily patient encounter data through an ETL pipeline. While the ETL ensures data is correctly loaded and validated, and a data quality framework monitors anomalies, stakeholders still require a **reporting layer** to:

- track operational performance
- monitor trends over time
- analyse practice and clinician activity
- understand cost and utilisation patterns
- identify high-level KPIs for decision-making
- combine data quality results with business reporting

This reporting layer provides a structured, reusable set of tables and views to support these needs.

---

## Project Goal
The goal of this project is to simulate a **production-style SQL Server reporting layer** that transforms curated healthcare data into:

- executive dashboards
- operational performance reports
- analytical datasets for BI tools
- time-series insights and variance analysis

It demonstrates how raw data evolves into **actionable insights** through structured reporting design.

---

## Technical Design
### Database Structure
The reporting layer is implemented in a dedicated schema:

- **rpt**: reporting tables, snapshots, and views

It works alongside:

- **dbo**: curated reporting table (`patient_encounter`)
- **etl**: ETL logging and processing
- **dq**: data quality monitoring outputs

---

## Reporting Architecture

### 1. Source Layer
- `dbo.patient_encounter`
  - curated, cleaned, deduplicated dataset from ETL

### 2. Snapshot Layer (Data Mart)
Pre-aggregated tables for performance and consistency:

- `rpt.daily_kpi_snapshot`
- `rpt.daily_practice_snapshot`
- `rpt.daily_encounter_type_snapshot`
- `rpt.daily_clinician_snapshot`
- `rpt.daily_diagnosis_snapshot`
- `rpt.monthly_practice_snapshot`
- `rpt.monthly_encounter_type_snapshot`

### 3. Reporting Views
Reusable views designed for:

- Power BI
- Excel dashboards
- ad hoc SQL analysis

---

## Key Reporting Features

### 1. Executive KPI Reporting
Daily and monthly high-level metrics:

- total encounters
- unique patients
- unique practices
- total and average cost
- cost per patient
- rolling 7-day and 30-day averages

These metrics support executive-level dashboards and decision-making.

---

### 2. Time-Series & Trend Analysis
The reporting layer includes built-in trend logic:

- daily trends
- monthly trends
- rolling averages (7-day, 30-day)
- day-on-day changes
- percentage growth/decline

Example insights:
- “Is patient activity increasing?”
- “Is cost trending upward over time?”

---

### 3. Practice Performance Analysis
Enables comparison across practices:

- total encounters per practice
- unique patients per practice
- cost per practice
- encounter share (% of total)
- ranking by volume and cost

Supports:
- benchmarking
- identifying high/low performing practices

---

### 4. Clinician Productivity Analysis
Tracks clinician-level activity:

- encounters per clinician
- patients seen per clinician
- number of practices covered
- cost generated per clinician
- ranking by workload

Useful for:
- workload balancing
- operational planning

---

### 5. Encounter Type Analysis
Analyses the mix of encounter types:

- total encounters by type
- cost by type
- average cost per type
- percentage share of total encounters
- daily mix change tracking

Supports:
- service utilisation analysis
- detection of unexpected shifts in care patterns

---

### 6. Diagnosis Analysis
Tracks diagnosis-level trends:

- encounter counts by diagnosis
- patient counts by diagnosis
- cost by diagnosis
- ranking of top diagnoses

Supports:
- clinical trend analysis
- demand forecasting

---

### 7. Variance & Change Analysis
Built-in calculations include:

- day-on-day volume change
- day-on-day cost change
- percentage change vs previous day
- variance from rolling averages

These are key for identifying:

- sudden spikes or drops
- operational anomalies
- emerging trends

---

### 8. Data Quality Integration
The reporting layer integrates with the DQ framework:

- DQ run status (success/failure)
- number of failed checks
- number of warning checks
- rule-level results

Key views:
- `rpt.vw_dq_reporting_summary`
- `rpt.vw_reporting_scorecard`

This allows users to see:
- whether data can be trusted
- which issues may impact reporting

---

## Stored Procedures

### Main Refresh Procedure
- `rpt.usp_refresh_reporting_layer`

This procedure:
1. refreshes all daily snapshots
2. refreshes all monthly snapshots

### Supporting Procedures
- `rpt.usp_refresh_daily_snapshots`
- `rpt.usp_refresh_monthly_snapshots`

---

## How to Run the Project

### 1. Ensure ETL and DQ layers are ready
Make sure:
- ETL pipeline has loaded data into `dbo.patient_encounter`
- (optional) DQ framework is installed and running

### 2. Run the reporting script
Execute:

```sql
-- Create reporting objects
-- (run the reporting SQL script)
