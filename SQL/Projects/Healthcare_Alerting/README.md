# Healthcare Monitoring & Alerting System (SQL Server)

## Overview
This project is a **healthcare-inspired monitoring and alerting framework** built in **SQL Server**, designed to sit on top of an ETL pipeline, a data quality framework, and a reporting layer. It simulates how a production-style data platform can detect operational issues, generate alerts, track alert lifecycle status, and support downstream notification workflows such as Power Automate, email, or Teams integration.

The solution demonstrates practical SQL Server and operational monitoring skills commonly used in real-world data engineering, BI, and support environments:

- operational alerting framework design
- alert queue and event history modelling
- failure and anomaly detection
- retry and escalation logic
- duplicate suppression and SLA tracking
- alert lifecycle management
- monitoring-ready operational reporting
- integration with ETL, data quality, and reporting layers

This project is intended for portfolio use and uses **fully synthetic data only**.

---

## Business Scenario
A healthcare organisation runs a daily data pipeline that loads patient encounter data into a reporting database. The platform already includes:

- an ETL pipeline to ingest and transform daily files
- a data quality framework to monitor load integrity and anomalies
- a reporting layer to provide business insights

However, in a realistic production environment, data teams also need an **operational monitoring and alerting system** so they can identify and respond to problems such as:

- ETL job failures
- missing daily runs
- failed or warning-level data quality checks
- stale reporting snapshots
- repeated delivery failures
- critical alerts that remain unresolved too long

This project adds that operational layer and simulates how incidents are tracked from detection through notification, acknowledgement, escalation, and closure.

---

## Project Goal
The goal of this project is to simulate a **production-style SQL Server alerting framework** that works alongside ETL, data quality, and reporting processes to support:

- faster issue detection
- operational visibility
- incident prioritisation
- notification workflow integration
- alert history and auditability
- SLA and ageing analysis

It demonstrates how a data platform can move beyond data processing and into **operational support and monitoring**.

---

## Technical Design
### Database Structure
The alerting framework is implemented in a dedicated schema:

- **ops**: alert configuration, queueing, event history, operational summaries, and monitoring procedures

It is designed to work alongside the rest of the platform:

- **dbo**: curated reporting data
- **etl**: ETL run logs and process outputs
- **dq**: data quality run and rule results
- **rpt**: reporting snapshots and KPI views
- **ops**: alerting and monitoring controls

---

## Alerting Architecture

### 1. Alert Configuration Layer
Defines how alerts behave, including:

- alert code
- source system
- alert category
- severity
- recipient groups
- escalation recipients
- deduplication window
- retry limits
- escalation timing
- SLA closure timing

Main table:
- `ops.alert_config`

This allows alert behaviour to be adjusted without changing the main logic.

---

### 2. Alert Queue Layer
Stores generated alerts and acts as the operational queue for downstream notification tools such as Power Automate.

Main table:
- `ops.alert_queue`

Each alert can include:

- source and category
- correlation and dedupe keys
- priority score
- owner
- acknowledgement details
- retry count
- escalation level
- SLA due timestamp
- current alert status

This table represents the live state of alert handling.

---

### 3. Alert Event History Layer
Captures the lifecycle history of each alert.

Main table:
- `ops.alert_event_history`

Example events:
- CREATED
- SENT
- FAILED
- ACKNOWLEDGED
- ESCALATED
- CLOSED
- RETRIED

This provides a full audit trail for operational review and troubleshooting.

---

### 4. Operational Summary Layer
Aggregates alert volumes for monitoring and reporting.

Main table:
- `ops.alert_operational_summary`

This supports dashboarding for:

- open alert backlog
- severity trends
- SLA breaches
- aged unresolved incidents
- alert source trends over time

---

## Alert Types Included
The framework supports multiple alert types and categories, reflecting a more realistic production monitoring setup.

### ETL Alerts
Examples:
- ETL step failure
- missing daily ETL run

These help identify job execution issues, scheduler failures, and upstream file problems.

### Data Quality Alerts
Examples:
- failed DQ rules
- warning-level DQ thresholds
- abnormal volume or completeness issues

These help identify trust or integrity issues in the data after load.

### Reporting Alerts
Examples:
- stale reporting snapshot
- failed reporting refresh

These help ensure business users are working with current and reliable outputs.

### Platform / Operational Alerts
Examples:
- alert delivery failure
- unresolved critical alert beyond SLA
- escalated open incident

These help monitor the monitoring process itself.

---

## Alert Lifecycle
A major part of the framework is the alert lifecycle model.

Supported statuses include:

- `NEW`
- `ACKNOWLEDGED`
- `SENT`
- `FAILED`
- `ESCALATED`
- `SUPPRESSED`
- `CLOSED`

This allows the project to simulate a more realistic incident workflow rather than simply creating and sending one-off alerts.

---

## Key Monitoring Features

### 1. Duplicate Suppression
The framework supports deduplication logic so the same issue is not repeatedly queued within a configured time window.

This reduces alert fatigue and makes notifications more realistic.

---

### 2. Retry Handling
If a downstream notification process fails, alerts can be marked as failed and then retried up to a configurable limit.

This simulates practical alert delivery handling in real operational systems.

---

### 3. Escalation Logic
Alerts can be escalated automatically if they remain unresolved past a configured threshold.

Examples:
- critical ETL failure not addressed within 30 minutes
- open high-severity issue that exceeds its SLA

This adds incident management depth to the project.

---

### 4. SLA Tracking
Each alert can be assigned an SLA due timestamp based on its configuration.

This allows the system to monitor:
- unresolved critical issues
- hours past SLA
- ageing open incidents

---

### 5. Ownership and Acknowledgement
Alerts can be acknowledged and assigned to an owner.

This supports a more realistic operational process where incidents are actively managed by named users or teams.

---

### 6. Operational Reporting
The framework includes monitoring outputs such as:

- open alert views
- alert history detail
- SLA breach views
- alert backlog summaries
- daily operational summary tables

These are useful for dashboards, support reporting, and trend analysis.

---

## Main Tables

### Configuration
- `ops.alert_config` – alert settings and thresholds

### Live Queue
- `ops.alert_queue` – active and historical alert records

### Audit Trail
- `ops.alert_event_history` – event-level lifecycle history

### Monitoring Summary
- `ops.alert_operational_summary` – aggregated alert reporting

---

## Stored Procedures

### Main Monitoring Procedure
- `ops.usp_run_daily_monitoring`

This procedure orchestrates the alerting process by:

1. generating alerts from ETL failures
2. generating alerts from DQ failures and warnings
3. checking for missing runs
4. checking for stale reporting snapshots
5. retrying failed notifications where eligible
6. escalating aged unresolved alerts
7. refreshing the operational alert summary

---

### Supporting Procedures
Examples include:

- `ops.usp_create_alert`
- `ops.usp_write_alert_history`
- `ops.usp_mark_alert_sent`
- `ops.usp_mark_alert_failed`
- `ops.usp_acknowledge_alert`
- `ops.usp_close_alert`
- `ops.usp_generate_alerts_for_etl`
- `ops.usp_generate_alerts_for_dq`
- `ops.usp_generate_missing_run_alerts`
- `ops.usp_generate_stale_snapshot_alerts`
- `ops.usp_retry_failed_alerts`
- `ops.usp_escalate_open_alerts`
- `ops.usp_refresh_alert_operational_summary`

---

## Reporting Views
The alerting layer also includes SQL views for operational reporting.

Examples:

- `ops.vw_open_alerts`
- `ops.vw_alert_history_detail`
- `ops.vw_alert_sla_breaches`
- `ops.vw_alert_backlog`
- `ops.vw_alert_operational_summary`

These views are designed for Power BI, Excel, or ad hoc operational queries.

---

## How the Framework Works

### 1. Source processes run
ETL, DQ, and reporting processes execute as part of the daily platform workflow.

### 2. Monitoring procedures evaluate results
The alerting layer checks for:

- ETL failures
- missing expected runs
- failed or warning DQ checks
- stale reporting outputs
- unresolved open alerts

### 3. Alerts are generated
Relevant issues are written into `ops.alert_queue` with severity, priority, status, routing metadata, and lifecycle fields.

### 4. Notification tools consume new alerts
A downstream tool such as Power Automate can poll the queue for alerts with `NEW` status, send notifications, and update the queue accordingly.

### 5. Alert lifecycle is tracked
As alerts move through delivery, acknowledgement, escalation, and closure, the system writes corresponding rows into `ops.alert_event_history`.

### 6. Operational summaries are refreshed
Daily operational aggregates are written to `ops.alert_operational_summary` for reporting and trend analysis.

---

## Example Notification Workflow
This project is designed so that a downstream tool such as Power Automate can process the queue.

A typical notification flow might be:

1. query `ops.alert_queue` for `NEW` alerts
2. send an email or Teams message using `alert_subject` and `alert_message`
3. on success, execute `ops.usp_mark_alert_sent`
4. on failure, execute `ops.usp_mark_alert_failed`
5. support analysts acknowledge or close alerts through SQL procedures or another interface

This creates a realistic bridge between SQL-based monitoring and external workflow tools.

---

## How to Run the Project

### 1. Ensure prerequisite layers exist
Before running this project, the following should already be available:

- ETL tables and logs
- DQ run and result tables
- reporting snapshots

### 2. Run the alerting SQL script
Execute the alerting script in SQL Server Management Studio to create:

- `ops` schema
- configuration table
- queue table
- event history table
- operational summary table
- alert generation procedures
- lifecycle procedures
- operational reporting views

### 3. Run the monitoring process
Example:

```sql
EXEC ops.usp_run_daily_monitoring;
