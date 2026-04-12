# Healthcare Data Pipeline Monitoring & Alerting System

## Overview
This project adds an operational alerting layer to a healthcare-style SQL Server data platform built with synthetic patient encounter data. It is designed to simulate what happens in a real production environment when ETL jobs fail or when data quality checks detect abnormal results.

The project sits on top of three earlier portfolio projects:

1. **ETL pipeline** – loads daily healthcare encounter data from CSV files into SQL Server  
2. **Data quality framework** – calculates metrics and detects anomalies in encounter data  
3. **Reporting layer** – exposes curated KPIs and reporting views for analytics

This fourth project completes the story by showing how a data platform can be monitored and operated, not just built.

---

## Business Scenario
A healthcare analytics team receives daily patient encounter data from an upstream operational system. The data platform must:

- load daily files
- validate and cleanse records
- monitor data quality
- expose reporting metrics
- notify the right people when something goes wrong

This project focuses on the final requirement: **automated operational alerting**.

---

## Objectives
The solution is designed to:

- detect failed ETL runs
- detect failed data quality checks
- create alerts in an SQL-backed queue
- provide an audit trail of alert lifecycle events
- support downstream notification tools such as Power Automate
- track alert status for reporting and operational dashboards

---

## Technology Stack
- **SQL Server**
- **T-SQL stored procedures**
- **SQL Agent** (optional scheduling)
- **Power Automate** (recommended downstream notification layer)
- **Synthetic healthcare encounter data**

---

## Solution Architecture
The alerting layer introduces a new operational schema: `ops`.

### Core tables
- `ops.alert_config`  
  Stores alert definitions, severity, recipient groups, and email recipients.

- `ops.alert_queue`  
  Stores generated alerts waiting to be sent or resolved.

- `ops.alert_event_history`  
  Tracks the lifecycle of each alert, including creation, sending, failure, and closure.

- `ops.daily_alert_summary`  
  Stores a daily operational summary for reporting and dashboards.

### Main procedures
- `ops.usp_create_alert`  
  Creates an alert and prevents duplicate active alerts for the same source record.

- `ops.usp_generate_etl_failure_alerts`  
  Reads `etl.etl_run_log` and creates alerts for ETL failures.

- `ops.usp_generate_dq_failure_alerts`  
  Reads `dq.dq_results` and creates alerts for failed data quality checks.

- `ops.usp_run_daily_alert_generation`  
  Wrapper procedure that runs all daily alert generation steps.

- `ops.usp_mark_alert_sent`  
  Marks an alert as sent after Power Automate or another tool notifies stakeholders.

- `ops.usp_mark_alert_failed`  
  Records notification failures.

- `ops.usp_close_alert`  
  Closes an alert after investigation or remediation.

- `ops.usp_refresh_daily_alert_summary`  
  Rebuilds daily summary counts for dashboarding.

---

## End-to-End Flow
1. The ETL process runs daily and writes status rows to `etl.etl_run_log`.
2. The data quality framework runs and writes results to `dq.dq_results`.
3. The alert generation procedure scans for:
   - ETL rows with `status = 'FAILED'`
   - data quality rows with `status = 'FAIL'`
4. Matching issues are inserted into `ops.alert_queue`.
5. Power Automate reads new alerts and sends notifications.
6. After sending:
   - success updates the alert to `SENT`
   - failures update the alert to `FAILED`
7. Once the issue is resolved, the alert can be marked as `CLOSED`.
8. Daily summary data is refreshed for reporting and operational review.

---

## Example Use Cases
### 1. ETL failure
A daily CSV file is missing or corrupted. The ETL wrapper writes a failed row to `etl.etl_run_log`. The alerting procedure creates a **CRITICAL** ETL alert, which can trigger an email to the data engineering team.

### 2. Data quality failure
The daily encounter volume suddenly drops outside the expected range. The data quality framework writes a failed result to `dq.dq_results`. The alerting procedure creates a **HIGH** or **MEDIUM** severity alert, depending on the rule type.

### 3. Notification lifecycle
Power Automate checks `ops.alert_queue` for `NEW` alerts, sends email notifications, then calls a status update procedure to mark the alert as sent or failed.

---

## Power Automate Design
A recommended Power Automate flow:

### Flow name
**Healthcare Alert Notifications**

### Trigger
- Recurrence trigger (for example, every 15 minutes)

### Actions
1. Query `ops.alert_queue` where `alert_status = 'NEW'`
2. Loop through each alert
3. Send an email using:
   - `alert_subject`
   - `alert_message`
   - recipients from `ops.alert_config`
4. On success, execute `ops.usp_mark_alert_sent`
5. On failure, execute `ops.usp_mark_alert_failed`

This keeps SQL Server focused on alert generation while Power Automate handles outbound communication.

---

## Files in This Project
- `alerting_project.sql` – full SQL script with database creation check, tables, views, procedures, comments, and optional SQL Agent job script
- `README.md` – this project overview
- project description document – portfolio-ready write-up for interviews and documentation

---

## How to Test
1. Ensure the earlier projects are already created:
   - ETL project
   - data quality project
   - reporting project
2. Insert a sample failed row into `etl.etl_run_log`
3. Insert a sample failed row into `dq.dq_results`
4. Run:
   ```sql
   EXEC ops.usp_run_daily_alert_generation;
   EXEC ops.usp_refresh_daily_alert_summary;
   ```
5. Review:
   ```sql
   SELECT * FROM ops.alert_queue ORDER BY alert_id DESC;
   SELECT * FROM ops.alert_event_history ORDER BY alert_event_id DESC;
   SELECT * FROM ops.daily_alert_summary ORDER BY summary_date DESC;
   ```

---

## Skills Demonstrated
This project demonstrates:

- SQL Server schema design
- operational alert queue design
- T-SQL stored procedures
- duplicate prevention logic
- failure monitoring
- support workflow design
- audit trail tracking
- integration design with Power Automate
- production-style thinking for healthcare analytics platforms

---

## CV / Portfolio Wording
**Healthcare Data Pipeline Monitoring & Alerting System**  
- Designed an SQL Server alerting framework to monitor ETL and data quality failures in a healthcare-style data platform  
- Built operational tables and stored procedures to generate alerts, track status, and maintain alert history  
- Integrated the solution design with Power Automate for automated stakeholder notifications  
- Created summary reporting structures to support operational monitoring and incident tracking  

---

## Interview Positioning
A strong way to explain this project:

> I wanted to go beyond ETL and reporting and show how a data platform can be operated in a more realistic production-style environment. I built an alerting layer that monitors ETL failures and data quality failures, generates alerts in SQL Server, and supports downstream notification through Power Automate.

---

## Suggested Next Extension
If you want to extend the portfolio even further, the best next step would be:

- a **service management / ticketing simulation**, where critical alerts automatically create incident records for follow-up

That would turn the portfolio into a full operational analytics platform.