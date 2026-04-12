# Healthcare Reporting & Insights Layer

## Overview
This project is the reporting and analytics layer for a portfolio-safe healthcare data platform built in SQL Server. It sits on top of the earlier ETL and data quality projects and transforms synthetic patient encounter data into reusable business reporting outputs.

The aim of this project is to show how raw operational data can be turned into dashboards, KPIs, and decision-support reporting for analysts, SQL developers, and operations teams.

## Project goals
- Create a reporting layer on top of a curated healthcare encounter table
- Build reusable SQL views for dashboard and BI use
- Produce daily KPI snapshots for trend analysis and point-in-time reporting
- Support practical operational reporting such as practice performance, clinician workload, encounter mix, diagnosis trends, and cost metrics

## Tech stack
- SQL Server
- T-SQL
- Views
- Stored Procedures
- Snapshot Tables
- Power BI or Excel (recommended for visualisation)

## Source table
This project assumes the ETL project has already created and loaded the following table:

- `dbo.patient_encounter`

The dataset is synthetic and is designed to simulate a realistic healthcare encounter feed.

## Reporting objects created
### Reporting schema
- `rpt`

### Views
- `rpt.vw_daily_activity`
- `rpt.vw_practice_performance`
- `rpt.vw_encounter_type_analysis`
- `rpt.vw_monthly_encounter_trend`
- `rpt.vw_clinician_workload`
- `rpt.vw_diagnosis_summary`
- `rpt.vw_executive_kpis`

### Snapshot tables
- `rpt.daily_kpi_snapshot`
- `rpt.daily_practice_snapshot`
- `rpt.daily_encounter_type_snapshot`

### Stored procedures
- `rpt.usp_refresh_daily_kpi_snapshot`
- `rpt.usp_refresh_daily_practice_snapshot`
- `rpt.usp_refresh_daily_encounter_type_snapshot`
- `rpt.usp_refresh_reporting_layer`

## Example business questions this project answers
- How many encounters were recorded each day?
- How many unique patients and practices were active?
- Which practices handled the most encounter volume?
- Which encounter types were most common?
- Which clinicians carried the highest workload?
- Which diagnosis codes appeared most often?
- How did monthly activity and cost trends change over time?

## Dashboard recommendation
A simple dashboard can be built using the views in this project.

### Page 1: Executive Overview
Use KPI cards and a daily trend chart.
- Total encounters
- Total unique patients
- Total cost
- Average cost
- Daily encounters over time

### Page 2: Operational Performance
Compare workload and activity.
- Practice performance chart
- Encounter type breakdown
- Clinician workload table

### Page 3: Clinical / Coding Summary
Focus on diagnosis and cost distribution.
- Diagnosis code summary
- Monthly cost trend
- Encounter type cost analysis

## Recommended build sequence
1. Run the ETL project to create and load `dbo.patient_encounter`
2. Run the data quality project if you want quality monitoring in place
3. Run the reporting SQL script from this project
4. Execute `rpt.usp_refresh_reporting_layer`
5. Connect Power BI or Excel to the reporting views

## Portfolio value
This project demonstrates:
- SQL-based reporting design
- business KPI modelling
- dimensional / reporting thinking
- operational analytics
- healthcare-inspired dashboard preparation
- end-to-end data platform thinking when combined with the ETL and data quality projects

## CV-ready wording
**Healthcare Reporting & Analytics Project**
- Built a SQL Server reporting layer on top of synthetic healthcare encounter data to support dashboard and KPI reporting
- Developed reusable SQL views for daily activity, practice performance, encounter type mix, clinician workload, diagnosis trends, and executive metrics
- Created stored procedures to populate daily snapshot tables for point-in-time and trend reporting
- Designed the reporting structure for use in Power BI or Excel dashboards

## Interview-ready explanation
“I built this project as the reporting layer of a larger healthcare data platform. After setting up ingestion and data quality monitoring, I created reusable SQL views and KPI snapshots so that the data could be consumed by dashboards and business users. The goal was to simulate how reporting is typically built on top of curated operational healthcare data.”
