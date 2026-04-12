/*
    ============================================================================================
    Project: Healthcare Data Pipeline Monitoring & Alerting System
    Purpose:
        This script adds an operational alerting layer on top of the earlier portfolio projects:
            1) Daily ETL pipeline
            2) Data quality monitoring
            3) Reporting layer

        The goal is to simulate a production-style healthcare data platform where:
            - ETL failures are captured
            - Data quality failures are captured
            - Alerts are generated into an SQL-backed queue
            - Alerts can be consumed by Power Automate or another notification tool
            - Alert status is updated after notification is sent

    Notes:
        - This project uses synthetic / portfolio-safe healthcare data only.
        - It assumes the following objects already exist from prior projects:
              dbo.patient_encounter
              etl.etl_run_log
              dq.dq_results
        - If those objects do not exist yet, run the ETL and data quality project scripts first.
        - The script is written for SQL Server.
    ============================================================================================
*/

-----------------------------------------------------------------------------------------------
-- 1. Create database if it does not already exist
-----------------------------------------------------------------------------------------------
IF DB_ID('PortfolioETL') IS NULL
BEGIN
    CREATE DATABASE PortfolioETL;
END
GO

USE PortfolioETL;
GO

-----------------------------------------------------------------------------------------------
-- 2. Safety checks for prerequisite tables
--    These checks help explain why the script might fail if the earlier projects were not built.
-----------------------------------------------------------------------------------------------
IF OBJECT_ID('etl.etl_run_log', 'U') IS NULL
BEGIN
    RAISERROR('Missing prerequisite table: etl.etl_run_log. Run the ETL project first.', 16, 1);
    RETURN;
END
GO

IF OBJECT_ID('dq.dq_results', 'U') IS NULL
BEGIN
    RAISERROR('Missing prerequisite table: dq.dq_results. Run the data quality project first.', 16, 1);
    RETURN;
END
GO

-----------------------------------------------------------------------------------------------
-- 3. Create operational schema
-----------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ops')
    EXEC('CREATE SCHEMA ops');
GO

-----------------------------------------------------------------------------------------------
-- 4. Alert configuration table
--    Stores thresholds, recipients, and whether a rule is active.
--    In a real system, this lets operations update alert settings without changing code.
-----------------------------------------------------------------------------------------------
IF OBJECT_ID('ops.alert_config', 'U') IS NOT NULL
    DROP TABLE ops.alert_config;
GO

CREATE TABLE ops.alert_config
(
    alert_config_id         INT IDENTITY(1,1) PRIMARY KEY,
    alert_code              VARCHAR(100)    NOT NULL,
    alert_source            VARCHAR(50)     NOT NULL, -- ETL / DQ / REPORTING / OTHER
    alert_name              VARCHAR(200)    NOT NULL,
    severity                VARCHAR(20)     NOT NULL, -- LOW / MEDIUM / HIGH / CRITICAL
    is_active               BIT             NOT NULL DEFAULT 1,
    notify_group            VARCHAR(200)    NULL,     -- e.g. Data Team / Operations / Analytics
    notify_emails           VARCHAR(1000)   NULL,     -- used by Power Automate or manual review
    notes                   VARCHAR(1000)   NULL,
    created_datetime        DATETIME2(0)    NOT NULL DEFAULT SYSDATETIME(),
    updated_datetime        DATETIME2(0)    NOT NULL DEFAULT SYSDATETIME()
);
GO

-----------------------------------------------------------------------------------------------
-- 5. Alert queue table
--    This acts as the "notification staging area".
--    Power Automate can poll this table for NEW alerts and send emails / Teams messages.
-----------------------------------------------------------------------------------------------
IF OBJECT_ID('ops.alert_queue', 'U') IS NOT NULL
    DROP TABLE ops.alert_queue;
GO

CREATE TABLE ops.alert_queue
(
    alert_id                 BIGINT IDENTITY(1,1) PRIMARY KEY,
    alert_config_id          INT              NULL,
    alert_code               VARCHAR(100)     NOT NULL,
    alert_source             VARCHAR(50)      NOT NULL,
    source_record_key        VARCHAR(200)     NULL,   -- e.g. ETL run id or DQ result id
    run_date                 DATE             NULL,
    alert_subject            VARCHAR(300)     NOT NULL,
    alert_message            VARCHAR(MAX)     NOT NULL,
    severity                 VARCHAR(20)      NOT NULL,
    alert_status             VARCHAR(20)      NOT NULL DEFAULT 'NEW',  -- NEW / SENT / FAILED / CLOSED
    sent_datetime            DATETIME2(0)     NULL,
    sent_to                  VARCHAR(1000)    NULL,
    failure_reason           VARCHAR(2000)    NULL,
    created_datetime         DATETIME2(0)     NOT NULL DEFAULT SYSDATETIME(),
    updated_datetime         DATETIME2(0)     NOT NULL DEFAULT SYSDATETIME()
);
GO

CREATE INDEX IX_alert_queue_status_created
    ON ops.alert_queue(alert_status, created_datetime);
GO

-----------------------------------------------------------------------------------------------
-- 6. Alert event history
--    Records status changes so the workflow has an audit trail.
-----------------------------------------------------------------------------------------------
IF OBJECT_ID('ops.alert_event_history', 'U') IS NOT NULL
    DROP TABLE ops.alert_event_history;
GO

CREATE TABLE ops.alert_event_history
(
    alert_event_id           BIGINT IDENTITY(1,1) PRIMARY KEY,
    alert_id                 BIGINT           NOT NULL,
    event_type               VARCHAR(50)      NOT NULL, -- CREATED / SENT / FAILED / CLOSED / RETRIED
    event_message            VARCHAR(2000)    NULL,
    event_datetime           DATETIME2(0)     NOT NULL DEFAULT SYSDATETIME(),
    event_by                 VARCHAR(200)     NULL
);
GO

-----------------------------------------------------------------------------------------------
-- 7. Alert summary snapshot table
--    This is useful for reporting and dashboarding on alert trends over time.
-----------------------------------------------------------------------------------------------
IF OBJECT_ID('ops.daily_alert_summary', 'U') IS NOT NULL
    DROP TABLE ops.daily_alert_summary;
GO

CREATE TABLE ops.daily_alert_summary
(
    summary_date             DATE            NOT NULL,
    alert_source             VARCHAR(50)     NOT NULL,
    severity                 VARCHAR(20)     NOT NULL,
    new_alert_count          INT             NOT NULL,
    sent_alert_count         INT             NOT NULL,
    failed_alert_count       INT             NOT NULL,
    closed_alert_count       INT             NOT NULL,
    created_datetime         DATETIME2(0)    NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_daily_alert_summary PRIMARY KEY (summary_date, alert_source, severity)
);
GO

-----------------------------------------------------------------------------------------------
-- 8. Seed alert configuration
--    You can add more rows later if you extend the platform.
-----------------------------------------------------------------------------------------------
INSERT INTO ops.alert_config
(
    alert_code,
    alert_source,
    alert_name,
    severity,
    is_active,
    notify_group,
    notify_emails,
    notes
)
VALUES
(
    'ETL_FAILURE',
    'ETL',
    'Daily ETL Failure',
    'CRITICAL',
    1,
    'Data Engineering',
    'data.team@example.com;ops.manager@example.com',
    'Triggered when a step in the ETL run log is marked as FAILED.'
),
(
    'DQ_FAILURE',
    'DQ',
    'Data Quality Failure',
    'HIGH',
    1,
    'Data Quality',
    'data.quality@example.com;analytics.lead@example.com',
    'Triggered when a DQ result has FAIL status.'
),
(
    'DQ_ANOMALY_VOLUME',
    'DQ',
    'Volume Anomaly',
    'MEDIUM',
    1,
    'Analytics',
    'analytics.team@example.com',
    'Triggered when volume checks fail and indicate a likely data drop or spike.'
);
GO

-----------------------------------------------------------------------------------------------
-- 9. Utility procedure: write alert history
-----------------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE ops.usp_write_alert_history
(
    @alert_id            BIGINT,
    @event_type          VARCHAR(50),
    @event_message       VARCHAR(2000) = NULL,
    @event_by            VARCHAR(200) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO ops.alert_event_history
    (
        alert_id,
        event_type,
        event_message,
        event_by
    )
    VALUES
    (
        @alert_id,
        @event_type,
        @event_message,
        @event_by
    );
END;
GO

-----------------------------------------------------------------------------------------------
-- 10. Utility procedure: create one alert
--     Prevents duplicate NEW or SENT alerts for the same source record and alert code.
-----------------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE ops.usp_create_alert
(
    @alert_code           VARCHAR(100),
    @alert_source         VARCHAR(50),
    @source_record_key    VARCHAR(200) = NULL,
    @run_date             DATE = NULL,
    @alert_subject        VARCHAR(300),
    @alert_message        VARCHAR(MAX),
    @severity             VARCHAR(20)
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @alert_id BIGINT;
    DECLARE @alert_config_id INT;

    SELECT @alert_config_id = alert_config_id
    FROM ops.alert_config
    WHERE alert_code = @alert_code
      AND alert_source = @alert_source
      AND is_active = 1;

    -- Duplicate prevention:
    -- Do not create another active alert for the same source record + alert code
    IF EXISTS
    (
        SELECT 1
        FROM ops.alert_queue
        WHERE alert_code = @alert_code
          AND ISNULL(source_record_key, '') = ISNULL(@source_record_key, '')
          AND alert_status IN ('NEW', 'SENT')
    )
    BEGIN
        RETURN;
    END

    INSERT INTO ops.alert_queue
    (
        alert_config_id,
        alert_code,
        alert_source,
        source_record_key,
        run_date,
        alert_subject,
        alert_message,
        severity
    )
    VALUES
    (
        @alert_config_id,
        @alert_code,
        @alert_source,
        @source_record_key,
        @run_date,
        @alert_subject,
        @alert_message,
        @severity
    );

    SET @alert_id = SCOPE_IDENTITY();

    EXEC ops.usp_write_alert_history
        @alert_id = @alert_id,
        @event_type = 'CREATED',
        @event_message = 'Alert created and added to alert_queue.',
        @event_by = 'SQL Procedure';
END;
GO

-----------------------------------------------------------------------------------------------
-- 11. Generate alerts from ETL failures
--     Reads etl.etl_run_log and creates alerts for FAILED steps.
--     Assumes etl_run_log includes:
--         etl_run_id, process_name, source_file_name, step_name, start_datetime, end_datetime,
--         rows_processed, rows_inserted, rows_updated, rows_rejected, status, error_message
-----------------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE ops.usp_generate_etl_failure_alerts
(
    @as_of_date DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    IF @as_of_date IS NULL
        SET @as_of_date = CAST(GETDATE() AS DATE);

    DECLARE
        @etl_run_id         BIGINT,
        @process_name       VARCHAR(200),
        @source_file_name   VARCHAR(260),
        @step_name          VARCHAR(200),
        @error_message      VARCHAR(MAX),
        @subject            VARCHAR(300),
        @body               VARCHAR(MAX);

    DECLARE etl_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT
            etl_run_id,
            process_name,
            source_file_name,
            step_name,
            error_message
        FROM etl.etl_run_log
        WHERE status = 'FAILED'
          AND CAST(start_datetime AS DATE) = @as_of_date;

    OPEN etl_cursor;

    FETCH NEXT FROM etl_cursor
    INTO @etl_run_id, @process_name, @source_file_name, @step_name, @error_message;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @subject =
            CONCAT('CRITICAL: ETL Failure - ', @process_name, ' - ', ISNULL(@source_file_name, 'No File'));

        SET @body =
            CONCAT(
                'An ETL failure was detected in the healthcare pipeline.', CHAR(13), CHAR(10),
                'Process Name: ', ISNULL(@process_name, 'N/A'), CHAR(13), CHAR(10),
                'Source File: ', ISNULL(@source_file_name, 'N/A'), CHAR(13), CHAR(10),
                'Failed Step: ', ISNULL(@step_name, 'N/A'), CHAR(13), CHAR(10),
                'Run Date: ', CONVERT(VARCHAR(10), @as_of_date, 120), CHAR(13), CHAR(10),
                'Error Message: ', ISNULL(@error_message, 'No message available'), CHAR(13), CHAR(10),
                'Action: Review ETL logs and investigate upstream data or job execution.'
            );

        EXEC ops.usp_create_alert
            @alert_code = 'ETL_FAILURE',
            @alert_source = 'ETL',
            @source_record_key = CAST(@etl_run_id AS VARCHAR(200)),
            @run_date = @as_of_date,
            @alert_subject = @subject,
            @alert_message = @body,
            @severity = 'CRITICAL';

        FETCH NEXT FROM etl_cursor
        INTO @etl_run_id, @process_name, @source_file_name, @step_name, @error_message;
    END

    CLOSE etl_cursor;
    DEALLOCATE etl_cursor;
END;
GO

-----------------------------------------------------------------------------------------------
-- 12. Generate alerts from data quality failures
--     This version is flexible because different DQ projects may have slightly different
--     column names. The script assumes the following columns exist:
--         dq_result_id, rule_name, run_date, metric_value, threshold_min, threshold_max, status
--     If your earlier script used rule_id instead of rule_name, adjust accordingly.
-----------------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE ops.usp_generate_dq_failure_alerts
(
    @as_of_date DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    IF @as_of_date IS NULL
        SET @as_of_date = CAST(GETDATE() AS DATE);

    DECLARE
        @dq_result_id       BIGINT,
        @rule_name          VARCHAR(200),
        @metric_value       DECIMAL(18,2),
        @threshold_min      DECIMAL(18,2),
        @threshold_max      DECIMAL(18,2),
        @subject            VARCHAR(300),
        @body               VARCHAR(MAX),
        @alert_code         VARCHAR(100),
        @severity           VARCHAR(20);

    DECLARE dq_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT
            dq_result_id,
            rule_name,
            metric_value,
            threshold_min,
            threshold_max
        FROM dq.dq_results
        WHERE status = 'FAIL'
          AND run_date = @as_of_date;

    OPEN dq_cursor;

    FETCH NEXT FROM dq_cursor
    INTO @dq_result_id, @rule_name, @metric_value, @threshold_min, @threshold_max;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Example of dynamic routing by rule name:
        -- volume anomalies are often medium severity; other failures can be high.
        SET @alert_code =
            CASE
                WHEN @rule_name LIKE '%Volume%' THEN 'DQ_ANOMALY_VOLUME'
                ELSE 'DQ_FAILURE'
            END;

        SET @severity =
            CASE
                WHEN @rule_name LIKE '%Volume%' THEN 'MEDIUM'
                ELSE 'HIGH'
            END;

        SET @subject =
            CONCAT('DATA QUALITY ALERT: ', ISNULL(@rule_name, 'Unknown Rule'), ' - ', CONVERT(VARCHAR(10), @as_of_date, 120));

        SET @body =
            CONCAT(
                'A healthcare data quality issue was detected.', CHAR(13), CHAR(10),
                'Rule Name: ', ISNULL(@rule_name, 'N/A'), CHAR(13), CHAR(10),
                'Run Date: ', CONVERT(VARCHAR(10), @as_of_date, 120), CHAR(13), CHAR(10),
                'Metric Value: ', ISNULL(CAST(@metric_value AS VARCHAR(50)), 'N/A'), CHAR(13), CHAR(10),
                'Expected Range: ',
                    ISNULL(CAST(@threshold_min AS VARCHAR(50)), 'N/A'),
                    ' to ',
                    ISNULL(CAST(@threshold_max AS VARCHAR(50)), 'N/A'),
                    CHAR(13), CHAR(10),
                'Action: Review the data quality framework, source loads, and recent ETL activity.'
            );

        EXEC ops.usp_create_alert
            @alert_code = @alert_code,
            @alert_source = 'DQ',
            @source_record_key = CAST(@dq_result_id AS VARCHAR(200)),
            @run_date = @as_of_date,
            @alert_subject = @subject,
            @alert_message = @body,
            @severity = @severity;

        FETCH NEXT FROM dq_cursor
        INTO @dq_result_id, @rule_name, @metric_value, @threshold_min, @threshold_max;
    END

    CLOSE dq_cursor;
    DEALLOCATE dq_cursor;
END;
GO

-----------------------------------------------------------------------------------------------
-- 13. Wrapper procedure to generate all alerts for the day
-----------------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE ops.usp_run_daily_alert_generation
(
    @as_of_date DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    IF @as_of_date IS NULL
        SET @as_of_date = CAST(GETDATE() AS DATE);

    EXEC ops.usp_generate_etl_failure_alerts @as_of_date = @as_of_date;
    EXEC ops.usp_generate_dq_failure_alerts  @as_of_date = @as_of_date;
END;
GO

-----------------------------------------------------------------------------------------------
-- 14. Procedure to mark an alert as SENT
--     This would typically be called by Power Automate after an email or Teams message is sent.
-----------------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE ops.usp_mark_alert_sent
(
    @alert_id            BIGINT,
    @sent_to             VARCHAR(1000)
)
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE ops.alert_queue
    SET
        alert_status = 'SENT',
        sent_datetime = SYSDATETIME(),
        sent_to = @sent_to,
        updated_datetime = SYSDATETIME()
    WHERE alert_id = @alert_id
      AND alert_status = 'NEW';

    IF @@ROWCOUNT > 0
    BEGIN
        EXEC ops.usp_write_alert_history
            @alert_id = @alert_id,
            @event_type = 'SENT',
            @event_message = CONCAT('Alert sent to ', ISNULL(@sent_to, 'unknown recipients')),
            @event_by = 'Power Automate / SQL';
    END
END;
GO

-----------------------------------------------------------------------------------------------
-- 15. Procedure to mark an alert as FAILED
--     Useful if the downstream email / notification process fails.
-----------------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE ops.usp_mark_alert_failed
(
    @alert_id            BIGINT,
    @failure_reason      VARCHAR(2000)
)
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE ops.alert_queue
    SET
        alert_status = 'FAILED',
        failure_reason = @failure_reason,
        updated_datetime = SYSDATETIME()
    WHERE alert_id = @alert_id
      AND alert_status IN ('NEW', 'SENT');

    IF @@ROWCOUNT > 0
    BEGIN
        EXEC ops.usp_write_alert_history
            @alert_id = @alert_id,
            @event_type = 'FAILED',
            @event_message = @failure_reason,
            @event_by = 'Power Automate / SQL';
    END
END;
GO

-----------------------------------------------------------------------------------------------
-- 16. Procedure to close an alert
--     Simulates a support analyst or engineer resolving the issue.
-----------------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE ops.usp_close_alert
(
    @alert_id            BIGINT,
    @closed_by           VARCHAR(200) = NULL,
    @close_note          VARCHAR(2000) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE ops.alert_queue
    SET
        alert_status = 'CLOSED',
        updated_datetime = SYSDATETIME()
    WHERE alert_id = @alert_id
      AND alert_status IN ('NEW', 'SENT', 'FAILED');

    IF @@ROWCOUNT > 0
    BEGIN
        EXEC ops.usp_write_alert_history
            @alert_id = @alert_id,
            @event_type = 'CLOSED',
            @event_message = @close_note,
            @event_by = @closed_by;
    END
END;
GO

-----------------------------------------------------------------------------------------------
-- 17. Build daily operational summary
--     Can be used by Power BI / Excel reporting to show alert volumes by day and severity.
-----------------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE ops.usp_refresh_daily_alert_summary
(
    @summary_date DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    IF @summary_date IS NULL
        SET @summary_date = CAST(GETDATE() AS DATE);

    DELETE
    FROM ops.daily_alert_summary
    WHERE summary_date = @summary_date;

    INSERT INTO ops.daily_alert_summary
    (
        summary_date,
        alert_source,
        severity,
        new_alert_count,
        sent_alert_count,
        failed_alert_count,
        closed_alert_count
    )
    SELECT
        @summary_date AS summary_date,
        alert_source,
        severity,
        SUM(CASE WHEN alert_status = 'NEW'    THEN 1 ELSE 0 END) AS new_alert_count,
        SUM(CASE WHEN alert_status = 'SENT'   THEN 1 ELSE 0 END) AS sent_alert_count,
        SUM(CASE WHEN alert_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_alert_count,
        SUM(CASE WHEN alert_status = 'CLOSED' THEN 1 ELSE 0 END) AS closed_alert_count
    FROM ops.alert_queue
    WHERE CAST(created_datetime AS DATE) = @summary_date
    GROUP BY alert_source, severity;
END;
GO

-----------------------------------------------------------------------------------------------
-- 18. Reporting views
--     These views make it easier to analyse operational performance in a dashboard.
-----------------------------------------------------------------------------------------------
CREATE OR ALTER VIEW ops.vw_open_alerts
AS
SELECT
    aq.alert_id,
    aq.alert_code,
    aq.alert_source,
    aq.run_date,
    aq.alert_subject,
    aq.severity,
    aq.alert_status,
    aq.created_datetime,
    ac.notify_group,
    ac.notify_emails
FROM ops.alert_queue aq
LEFT JOIN ops.alert_config ac
    ON aq.alert_config_id = ac.alert_config_id
WHERE aq.alert_status IN ('NEW', 'SENT', 'FAILED');
GO

CREATE OR ALTER VIEW ops.vw_alert_history_detail
AS
SELECT
    aq.alert_id,
    aq.alert_code,
    aq.alert_source,
    aq.alert_subject,
    aq.severity,
    aq.alert_status,
    aq.created_datetime,
    ah.event_type,
    ah.event_message,
    ah.event_datetime,
    ah.event_by
FROM ops.alert_queue aq
INNER JOIN ops.alert_event_history ah
    ON aq.alert_id = ah.alert_id;
GO

-----------------------------------------------------------------------------------------------
-- 19. Optional SQL Agent job script
--     This creates a simple daily job to generate alerts and refresh summary data.
--     Review before running in your environment.
-----------------------------------------------------------------------------------------------
/*
USE msdb;
GO

EXEC dbo.sp_add_job
    @job_name = 'Daily Healthcare Alert Generation',
    @enabled = 1,
    @description = 'Generates ETL and DQ alerts for the healthcare portfolio project.';
GO

EXEC dbo.sp_add_jobstep
    @job_name = 'Daily Healthcare Alert Generation',
    @step_name = 'Generate Alerts',
    @subsystem = 'TSQL',
    @database_name = 'PortfolioETL',
    @command = '
        EXEC ops.usp_run_daily_alert_generation;
        EXEC ops.usp_refresh_daily_alert_summary;
    ';
GO

EXEC dbo.sp_add_schedule
    @schedule_name = 'Daily 2AM Healthcare Alert Job',
    @freq_type = 4,           -- daily
    @freq_interval = 1,       -- every day
    @active_start_time = 020000;
GO

EXEC dbo.sp_attach_schedule
    @job_name = 'Daily Healthcare Alert Generation',
    @schedule_name = 'Daily 2AM Healthcare Alert Job';
GO

EXEC dbo.sp_add_jobserver
    @job_name = 'Daily Healthcare Alert Generation';
GO
*/

-----------------------------------------------------------------------------------------------
-- 20. Power Automate implementation notes
--     Suggested flow design:
--
--     Flow name:
--         Healthcare Alert Notifications
--
--     Trigger:
--         Recurrence (every 15 minutes)
--         OR SQL trigger pattern via scheduled query
--
--     Steps:
--         1) Query ops.alert_queue where alert_status = 'NEW'
--         2) For each alert:
--             - Build email subject from alert_subject
--             - Build email body from alert_message
--             - Send email to configured recipients
--             - On success: execute ops.usp_mark_alert_sent
--             - On failure: execute ops.usp_mark_alert_failed
--
--     Suggested recipients:
--         - Data engineering team
--         - Analytics / BI team
--         - Operations / support manager
-----------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------
-- 21. Test data notes
--     To test this project:
--         1) Insert or generate a FAILED row in etl.etl_run_log
--         2) Insert or generate a FAIL row in dq.dq_results
--         3) Run:
--                EXEC ops.usp_run_daily_alert_generation;
--                EXEC ops.usp_refresh_daily_alert_summary;
--         4) Review:
--                SELECT * FROM ops.alert_queue ORDER BY alert_id DESC;
--                SELECT * FROM ops.alert_event_history ORDER BY alert_event_id DESC;
--                SELECT * FROM ops.daily_alert_summary ORDER BY summary_date DESC;
-----------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------
-- 22. Example test statements
--     Uncomment and adjust only if you want to simulate a failure.
-----------------------------------------------------------------------------------------------
/*
-- Example ETL failure test row
INSERT INTO etl.etl_run_log
(
    process_name,
    source_file_name,
    step_name,
    start_datetime,
    end_datetime,
    rows_processed,
    rows_inserted,
    rows_updated,
    rows_rejected,
    status,
    error_message
)
VALUES
(
    'patient_encounter_daily_etl',
    'patient_encounter_20260330.csv',
    'Load raw CSV',
    SYSDATETIME(),
    SYSDATETIME(),
    0,
    0,
    0,
    0,
    'FAILED',
    'Test error: inbound file could not be opened.'
);

-- Example DQ failure test row
INSERT INTO dq.dq_results
(
    rule_name,
    run_date,
    metric_value,
    threshold_min,
    threshold_max,
    status,
    created_datetime
)
VALUES
(
    'Volume Check',
    CAST(GETDATE() AS DATE),
    120.00,
    500.00,
    900.00,
    'FAIL',
    SYSDATETIME()
);

EXEC ops.usp_run_daily_alert_generation;
EXEC ops.usp_refresh_daily_alert_summary;

SELECT * FROM ops.alert_queue ORDER BY alert_id DESC;
SELECT * FROM ops.alert_event_history ORDER BY alert_event_id DESC;
SELECT * FROM ops.daily_alert_summary ORDER BY summary_date DESC;
*/