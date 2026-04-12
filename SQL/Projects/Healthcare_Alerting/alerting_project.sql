/*
    =========================================================
    Project: Enhanced Healthcare Monitoring & Alerting System
    Database: PortfolioHealthcareETL
    Purpose:
        Production-style operational alerting layer for:
          - ETL
          - Data Quality
          - Reporting
          - Platform monitoring

    Enhancements:
      - richer alert lifecycle
      - escalation support
      - retry management
      - deduplication windowing
      - stale process checks
      - missing run checks
      - reporting layer monitoring
      - SLA / aging analytics
      - more dashboard-ready outputs
    =========================================================
*/

USE master;
GO

IF DB_ID('PortfolioHealthcareETL') IS NULL
BEGIN
    RAISERROR('Database PortfolioHealthcareETL does not exist. Run ETL/DQ/reporting setup first.', 16, 1);
END
GO

USE PortfolioHealthcareETL;
GO

/* =========================================================
   1. PRE-CHECKS
   ========================================================= */
IF OBJECT_ID('etl.etl_run_log', 'U') IS NULL
BEGIN
    RAISERROR('Missing prerequisite table: etl.etl_run_log', 16, 1);
    RETURN;
END
GO

IF OBJECT_ID('dq.dq_run', 'U') IS NULL
BEGIN
    RAISERROR('Missing prerequisite table: dq.dq_run', 16, 1);
    RETURN;
END
GO

IF OBJECT_ID('dq.dq_result', 'U') IS NULL
BEGIN
    RAISERROR('Missing prerequisite table: dq.dq_result', 16, 1);
    RETURN;
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ops')
    EXEC('CREATE SCHEMA ops');
GO

/* =========================================================
   2. DROP OBJECTS IF RE-RUNNING
   ========================================================= */
IF OBJECT_ID('ops.usp_run_daily_monitoring', 'P') IS NOT NULL DROP PROCEDURE ops.usp_run_daily_monitoring;
GO
IF OBJECT_ID('ops.usp_generate_alerts_for_reporting', 'P') IS NOT NULL DROP PROCEDURE ops.usp_generate_alerts_for_reporting;
GO
IF OBJECT_ID('ops.usp_generate_alerts_for_dq', 'P') IS NOT NULL DROP PROCEDURE ops.usp_generate_alerts_for_dq;
GO
IF OBJECT_ID('ops.usp_generate_alerts_for_etl', 'P') IS NOT NULL DROP PROCEDURE ops.usp_generate_alerts_for_etl;
GO
IF OBJECT_ID('ops.usp_generate_missing_run_alerts', 'P') IS NOT NULL DROP PROCEDURE ops.usp_generate_missing_run_alerts;
GO
IF OBJECT_ID('ops.usp_generate_stale_snapshot_alerts', 'P') IS NOT NULL DROP PROCEDURE ops.usp_generate_stale_snapshot_alerts;
GO
IF OBJECT_ID('ops.usp_retry_failed_alerts', 'P') IS NOT NULL DROP PROCEDURE ops.usp_retry_failed_alerts;
GO
IF OBJECT_ID('ops.usp_escalate_open_alerts', 'P') IS NOT NULL DROP PROCEDURE ops.usp_escalate_open_alerts;
GO
IF OBJECT_ID('ops.usp_refresh_alert_operational_summary', 'P') IS NOT NULL DROP PROCEDURE ops.usp_refresh_alert_operational_summary;
GO
IF OBJECT_ID('ops.usp_acknowledge_alert', 'P') IS NOT NULL DROP PROCEDURE ops.usp_acknowledge_alert;
GO
IF OBJECT_ID('ops.usp_close_alert', 'P') IS NOT NULL DROP PROCEDURE ops.usp_close_alert;
GO
IF OBJECT_ID('ops.usp_mark_alert_failed', 'P') IS NOT NULL DROP PROCEDURE ops.usp_mark_alert_failed;
GO
IF OBJECT_ID('ops.usp_mark_alert_sent', 'P') IS NOT NULL DROP PROCEDURE ops.usp_mark_alert_sent;
GO
IF OBJECT_ID('ops.usp_create_alert', 'P') IS NOT NULL DROP PROCEDURE ops.usp_create_alert;
GO
IF OBJECT_ID('ops.usp_write_alert_history', 'P') IS NOT NULL DROP PROCEDURE ops.usp_write_alert_history;
GO

IF OBJECT_ID('ops.vw_alert_backlog', 'V') IS NOT NULL DROP VIEW ops.vw_alert_backlog;
GO
IF OBJECT_ID('ops.vw_alert_sla_breaches', 'V') IS NOT NULL DROP VIEW ops.vw_alert_sla_breaches;
GO
IF OBJECT_ID('ops.vw_open_alerts', 'V') IS NOT NULL DROP VIEW ops.vw_open_alerts;
GO
IF OBJECT_ID('ops.vw_alert_history_detail', 'V') IS NOT NULL DROP VIEW ops.vw_alert_history_detail;
GO
IF OBJECT_ID('ops.vw_alert_operational_summary', 'V') IS NOT NULL DROP VIEW ops.vw_alert_operational_summary;
GO

IF OBJECT_ID('ops.alert_operational_summary', 'U') IS NOT NULL DROP TABLE ops.alert_operational_summary;
GO
IF OBJECT_ID('ops.alert_event_history', 'U') IS NOT NULL DROP TABLE ops.alert_event_history;
GO
IF OBJECT_ID('ops.alert_queue', 'U') IS NOT NULL DROP TABLE ops.alert_queue;
GO
IF OBJECT_ID('ops.alert_config', 'U') IS NOT NULL DROP TABLE ops.alert_config;
GO

/* =========================================================
   3. ALERT CONFIGURATION
   ========================================================= */
CREATE TABLE ops.alert_config
(
    alert_config_id             INT IDENTITY(1,1) PRIMARY KEY,
    alert_code                  VARCHAR(100)  NOT NULL UNIQUE,
    alert_source                VARCHAR(50)   NOT NULL,   -- ETL / DQ / REPORTING / PLATFORM
    alert_category              VARCHAR(100)  NOT NULL,   -- FAILURE / STALENESS / VOLUME / SLA / OTHER
    alert_name                  VARCHAR(200)  NOT NULL,
    severity                    VARCHAR(20)   NOT NULL,   -- LOW / MEDIUM / HIGH / CRITICAL
    default_priority_score      INT           NOT NULL DEFAULT 50,
    is_active                   BIT           NOT NULL DEFAULT 1,
    notify_group                VARCHAR(200)  NULL,
    notify_emails               VARCHAR(1000) NULL,
    escalation_emails           VARCHAR(1000) NULL,
    dedupe_window_minutes       INT           NOT NULL DEFAULT 1440,
    max_retry_count             INT           NOT NULL DEFAULT 3,
    escalation_after_minutes    INT           NULL,
    sla_close_hours             INT           NULL,
    suppress_duplicates         BIT           NOT NULL DEFAULT 1,
    notes                       VARCHAR(2000) NULL,
    created_datetime            DATETIME2(0)  NOT NULL DEFAULT SYSDATETIME(),
    updated_datetime            DATETIME2(0)  NOT NULL DEFAULT SYSDATETIME()
);
GO

/* =========================================================
   4. ALERT QUEUE
   ========================================================= */
CREATE TABLE ops.alert_queue
(
    alert_id                    BIGINT IDENTITY(1,1) PRIMARY KEY,
    alert_config_id             INT             NULL,
    alert_code                  VARCHAR(100)    NOT NULL,
    alert_source                VARCHAR(50)     NOT NULL,
    alert_category              VARCHAR(100)    NOT NULL,
    source_record_key           VARCHAR(200)    NULL,
    correlation_key             VARCHAR(300)    NULL,
    dedupe_key                  VARCHAR(500)    NULL,
    run_date                    DATE            NULL,
    alert_subject               VARCHAR(300)    NOT NULL,
    alert_message               VARCHAR(MAX)    NOT NULL,
    severity                    VARCHAR(20)     NOT NULL,
    priority_score              INT             NOT NULL,
    alert_status                VARCHAR(20)     NOT NULL DEFAULT 'NEW', 
        -- NEW / ACKNOWLEDGED / SENT / FAILED / ESCALATED / SUPPRESSED / CLOSED
    owner_name                  VARCHAR(200)    NULL,
    acknowledged_by             VARCHAR(200)    NULL,
    acknowledged_datetime       DATETIME2(0)    NULL,
    escalation_level            INT             NOT NULL DEFAULT 0,
    retry_count                 INT             NOT NULL DEFAULT 0,
    last_retry_datetime         DATETIME2(0)    NULL,
    suppress_until_datetime     DATETIME2(0)    NULL,
    sla_due_datetime            DATETIME2(0)    NULL,
    sent_datetime               DATETIME2(0)    NULL,
    sent_to                     VARCHAR(1000)   NULL,
    failure_reason              VARCHAR(2000)   NULL,
    created_datetime            DATETIME2(0)    NOT NULL DEFAULT SYSDATETIME(),
    updated_datetime            DATETIME2(0)    NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT FK_ops_alert_queue_config FOREIGN KEY (alert_config_id) REFERENCES ops.alert_config(alert_config_id)
);
GO

CREATE INDEX IX_ops_alert_queue_status_created
    ON ops.alert_queue(alert_status, created_datetime);
GO

CREATE INDEX IX_ops_alert_queue_source_run
    ON ops.alert_queue(alert_source, run_date, severity);
GO

CREATE INDEX IX_ops_alert_queue_dedupe
    ON ops.alert_queue(alert_code, dedupe_key, created_datetime);
GO

/* =========================================================
   5. ALERT HISTORY
   ========================================================= */
CREATE TABLE ops.alert_event_history
(
    alert_event_id              BIGINT IDENTITY(1,1) PRIMARY KEY,
    alert_id                    BIGINT         NOT NULL,
    event_type                  VARCHAR(50)    NOT NULL,
    event_message               VARCHAR(2000)  NULL,
    old_status                  VARCHAR(20)    NULL,
    new_status                  VARCHAR(20)    NULL,
    event_datetime              DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
    event_by                    VARCHAR(200)   NULL,
    CONSTRAINT FK_ops_alert_event_history_alert FOREIGN KEY (alert_id) REFERENCES ops.alert_queue(alert_id)
);
GO

/* =========================================================
   6. OPERATIONAL SUMMARY
   ========================================================= */
CREATE TABLE ops.alert_operational_summary
(
    summary_date                DATE           NOT NULL,
    alert_source                VARCHAR(50)    NOT NULL,
    alert_category              VARCHAR(100)   NOT NULL,
    severity                    VARCHAR(20)    NOT NULL,
    new_alert_count             INT            NOT NULL,
    acknowledged_alert_count    INT            NOT NULL,
    sent_alert_count            INT            NOT NULL,
    failed_alert_count          INT            NOT NULL,
    escalated_alert_count       INT            NOT NULL,
    closed_alert_count          INT            NOT NULL,
    open_alert_count            INT            NOT NULL,
    avg_alert_age_hours         DECIMAL(18,2)  NULL,
    sla_breach_count            INT            NOT NULL,
    created_datetime            DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_ops_alert_operational_summary
        PRIMARY KEY (summary_date, alert_source, alert_category, severity)
);
GO

/* =========================================================
   7. SEED ALERT CONFIG
   ========================================================= */
INSERT INTO ops.alert_config
(
    alert_code, alert_source, alert_category, alert_name, severity,
    default_priority_score, is_active, notify_group, notify_emails,
    escalation_emails, dedupe_window_minutes, max_retry_count,
    escalation_after_minutes, sla_close_hours, suppress_duplicates, notes
)
VALUES
('ETL_FAILURE',            'ETL',       'FAILURE',   'Daily ETL Failure',                'CRITICAL', 100, 1, 'Data Engineering', 'data.team@example.com',                'head.data@example.com',      1440, 3, 30,  4, 1, 'Triggered for FAILED ETL steps'),
('ETL_MISSING_RUN',        'ETL',       'STALENESS', 'Missing Daily ETL Run',            'CRITICAL', 95,  1, 'Data Engineering', 'data.team@example.com',                'head.data@example.com',      1440, 3, 15,  2, 1, 'Triggered when no ETL run exists for the day'),
('DQ_FAILURE',             'DQ',        'FAILURE',   'Data Quality Failure',             'HIGH',     80,  1, 'Data Quality',     'data.quality@example.com',             'head.analytics@example.com', 1440, 3, 60,  8, 1, 'Triggered when DQ results fail'),
('DQ_WARNING',             'DQ',        'THRESHOLD', 'Data Quality Warning',             'MEDIUM',   60,  1, 'Analytics',        'analytics.team@example.com',           'head.analytics@example.com', 1440, 2, 180, 24, 1, 'Triggered when DQ warning-level result occurs'),
('REPORTING_FAILURE',      'REPORTING', 'FAILURE',   'Reporting Refresh Failure',        'HIGH',     75,  1, 'BI Team',          'bi.team@example.com',                  'head.analytics@example.com', 1440, 3, 60,  8, 1, 'Triggered when reporting refresh does not complete'),
('REPORTING_STALE',        'REPORTING', 'STALENESS', 'Reporting Snapshot Stale',         'HIGH',     70,  1, 'BI Team',          'bi.team@example.com',                  'head.analytics@example.com', 1440, 2, 90,  12,1, 'Triggered when reporting snapshot not refreshed'),
('ALERT_DELIVERY_FAILED',  'PLATFORM',  'FAILURE',   'Alert Delivery Failure',           'HIGH',     85,  1, 'Platform Support', 'platform.support@example.com',         'head.platform@example.com',  60,   5, 30,  6, 1, 'Triggered when notification delivery repeatedly fails'),
('OPEN_CRITICAL_AGEING',   'PLATFORM',  'SLA',       'Critical Alert Not Resolved',      'CRITICAL', 98,  1, 'Operations',       'ops.manager@example.com',              'director@example.com',       60,   1, 0,   1, 1, 'Triggered when critical alerts exceed SLA');
GO

/* =========================================================
   8. HELPER: HISTORY WRITER
   ========================================================= */
CREATE OR ALTER PROCEDURE ops.usp_write_alert_history
(
    @alert_id        BIGINT,
    @event_type      VARCHAR(50),
    @event_message   VARCHAR(2000) = NULL,
    @old_status      VARCHAR(20) = NULL,
    @new_status      VARCHAR(20) = NULL,
    @event_by        VARCHAR(200) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO ops.alert_event_history
    (
        alert_id, event_type, event_message, old_status, new_status, event_by
    )
    VALUES
    (
        @alert_id, @event_type, @event_message, @old_status, @new_status, @event_by
    );
END;
GO

/* =========================================================
   9. HELPER: CREATE ALERT
   ========================================================= */
CREATE OR ALTER PROCEDURE ops.usp_create_alert
(
    @alert_code             VARCHAR(100),
    @alert_source           VARCHAR(50),
    @alert_category         VARCHAR(100),
    @source_record_key      VARCHAR(200) = NULL,
    @correlation_key        VARCHAR(300) = NULL,
    @dedupe_key             VARCHAR(500) = NULL,
    @run_date               DATE = NULL,
    @alert_subject          VARCHAR(300),
    @alert_message          VARCHAR(MAX),
    @severity               VARCHAR(20),
    @priority_score         INT = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @alert_id BIGINT;
    DECLARE @alert_config_id INT;
    DECLARE @dedupe_window_minutes INT;
    DECLARE @suppress_duplicates BIT;
    DECLARE @sla_close_hours INT;
    DECLARE @final_priority INT;

    SELECT
        @alert_config_id = alert_config_id,
        @dedupe_window_minutes = dedupe_window_minutes,
        @suppress_duplicates = suppress_duplicates,
        @sla_close_hours = sla_close_hours,
        @final_priority = ISNULL(@priority_score, default_priority_score)
    FROM ops.alert_config
    WHERE alert_code = @alert_code
      AND alert_source = @alert_source
      AND is_active = 1;

    IF @alert_config_id IS NULL
        RETURN;

    IF @suppress_duplicates = 1
    BEGIN
        IF EXISTS
        (
            SELECT 1
            FROM ops.alert_queue
            WHERE alert_code = @alert_code
              AND ISNULL(dedupe_key, '') = ISNULL(@dedupe_key, '')
              AND created_datetime >= DATEADD(MINUTE, -ISNULL(@dedupe_window_minutes, 1440), SYSDATETIME())
              AND alert_status IN ('NEW','ACKNOWLEDGED','SENT','ESCALATED')
        )
        BEGIN
            RETURN;
        END
    END

    INSERT INTO ops.alert_queue
    (
        alert_config_id, alert_code, alert_source, alert_category,
        source_record_key, correlation_key, dedupe_key, run_date,
        alert_subject, alert_message, severity, priority_score,
        sla_due_datetime
    )
    VALUES
    (
        @alert_config_id, @alert_code, @alert_source, @alert_category,
        @source_record_key, @correlation_key, @dedupe_key, @run_date,
        @alert_subject, @alert_message, @severity, @final_priority,
        CASE WHEN @sla_close_hours IS NULL THEN NULL ELSE DATEADD(HOUR, @sla_close_hours, SYSDATETIME()) END
    );

    SET @alert_id = SCOPE_IDENTITY();

    EXEC ops.usp_write_alert_history
        @alert_id = @alert_id,
        @event_type = 'CREATED',
        @event_message = 'Alert created and added to queue.',
        @new_status = 'NEW',
        @event_by = 'SQL Procedure';
END;
GO

/* =========================================================
   10. ALERT STATUS MANAGEMENT
   ========================================================= */
CREATE OR ALTER PROCEDURE ops.usp_mark_alert_sent
(
    @alert_id        BIGINT,
    @sent_to         VARCHAR(1000)
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @old_status VARCHAR(20);

    SELECT @old_status = alert_status
    FROM ops.alert_queue
    WHERE alert_id = @alert_id;

    UPDATE ops.alert_queue
    SET
        alert_status = 'SENT',
        sent_datetime = SYSDATETIME(),
        sent_to = @sent_to,
        updated_datetime = SYSDATETIME()
    WHERE alert_id = @alert_id
      AND alert_status IN ('NEW','ACKNOWLEDGED','ESCALATED');

    IF @@ROWCOUNT > 0
    BEGIN
        EXEC ops.usp_write_alert_history
            @alert_id = @alert_id,
            @event_type = 'SENT',
            @event_message = CONCAT('Alert sent to ', ISNULL(@sent_to,'unknown recipients')),
            @old_status = @old_status,
            @new_status = 'SENT',
            @event_by = 'Notification Workflow';
    END
END;
GO

CREATE OR ALTER PROCEDURE ops.usp_mark_alert_failed
(
    @alert_id            BIGINT,
    @failure_reason      VARCHAR(2000)
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @old_status VARCHAR(20);

    SELECT @old_status = alert_status
    FROM ops.alert_queue
    WHERE alert_id = @alert_id;

    UPDATE ops.alert_queue
    SET
        alert_status = 'FAILED',
        failure_reason = @failure_reason,
        retry_count = retry_count + 1,
        last_retry_datetime = SYSDATETIME(),
        updated_datetime = SYSDATETIME()
    WHERE alert_id = @alert_id
      AND alert_status IN ('NEW','ACKNOWLEDGED','SENT','ESCALATED');

    IF @@ROWCOUNT > 0
    BEGIN
        EXEC ops.usp_write_alert_history
            @alert_id = @alert_id,
            @event_type = 'FAILED',
            @event_message = @failure_reason,
            @old_status = @old_status,
            @new_status = 'FAILED',
            @event_by = 'Notification Workflow';
    END
END;
GO

CREATE OR ALTER PROCEDURE ops.usp_acknowledge_alert
(
    @alert_id            BIGINT,
    @acknowledged_by     VARCHAR(200),
    @owner_name          VARCHAR(200) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @old_status VARCHAR(20);

    SELECT @old_status = alert_status
    FROM ops.alert_queue
    WHERE alert_id = @alert_id;

    UPDATE ops.alert_queue
    SET
        alert_status = 'ACKNOWLEDGED',
        acknowledged_by = @acknowledged_by,
        acknowledged_datetime = SYSDATETIME(),
        owner_name = COALESCE(@owner_name, owner_name),
        updated_datetime = SYSDATETIME()
    WHERE alert_id = @alert_id
      AND alert_status IN ('NEW','SENT','FAILED','ESCALATED');

    IF @@ROWCOUNT > 0
    BEGIN
        EXEC ops.usp_write_alert_history
            @alert_id = @alert_id,
            @event_type = 'ACKNOWLEDGED',
            @event_message = 'Alert acknowledged by operator.',
            @old_status = @old_status,
            @new_status = 'ACKNOWLEDGED',
            @event_by = @acknowledged_by;
    END
END;
GO

CREATE OR ALTER PROCEDURE ops.usp_close_alert
(
    @alert_id        BIGINT,
    @closed_by       VARCHAR(200) = NULL,
    @close_note      VARCHAR(2000) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @old_status VARCHAR(20);

    SELECT @old_status = alert_status
    FROM ops.alert_queue
    WHERE alert_id = @alert_id;

    UPDATE ops.alert_queue
    SET
        alert_status = 'CLOSED',
        updated_datetime = SYSDATETIME()
    WHERE alert_id = @alert_id
      AND alert_status IN ('NEW','ACKNOWLEDGED','SENT','FAILED','ESCALATED');

    IF @@ROWCOUNT > 0
    BEGIN
        EXEC ops.usp_write_alert_history
            @alert_id = @alert_id,
            @event_type = 'CLOSED',
            @event_message = @close_note,
            @old_status = @old_status,
            @new_status = 'CLOSED',
            @event_by = @closed_by;
    END
END;
GO

/* =========================================================
   11. ETL ALERT GENERATION
   ========================================================= */
CREATE OR ALTER PROCEDURE ops.usp_generate_alerts_for_etl
(
    @as_of_date DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    IF @as_of_date IS NULL
        SET @as_of_date = CAST(GETDATE() AS DATE);

    INSERT INTO #tmp_etl_failures
    SELECT
        etl_run_id,
        process_name,
        source_file_name,
        step_name,
        error_message
    FROM etl.etl_run_log
    WHERE status = 'FAILED'
      AND CAST(start_datetime AS DATE) = @as_of_date;
END;
GO

CREATE OR ALTER PROCEDURE ops.usp_generate_alerts_for_etl
(
    @as_of_date DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    IF @as_of_date IS NULL
        SET @as_of_date = CAST(GETDATE() AS DATE);

    DECLARE
        @etl_run_id BIGINT,
        @process_name VARCHAR(200),
        @source_file_name VARCHAR(260),
        @step_name VARCHAR(200),
        @error_message VARCHAR(MAX),
        @subject VARCHAR(300),
        @body VARCHAR(MAX),
        @dedupe_key VARCHAR(500);

    DECLARE c CURSOR LOCAL FAST_FORWARD FOR
        SELECT
            etl_run_id,
            process_name,
            source_file_name,
            step_name,
            error_message
        FROM etl.etl_run_log
        WHERE status = 'FAILED'
          AND CAST(start_datetime AS DATE) = @as_of_date;

    OPEN c;
    FETCH NEXT FROM c INTO @etl_run_id, @process_name, @source_file_name, @step_name, @error_message;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @dedupe_key = CONCAT('ETL_FAILURE|', ISNULL(CAST(@etl_run_id AS VARCHAR(50)),'0'));
        SET @subject = CONCAT('CRITICAL: ETL Failure - ', ISNULL(@process_name,'Unknown Process'));
        SET @body = CONCAT(
            'An ETL failure was detected.', CHAR(13), CHAR(10),
            'Process Name: ', ISNULL(@process_name,'N/A'), CHAR(13), CHAR(10),
            'Source File: ', ISNULL(@source_file_name,'N/A'), CHAR(13), CHAR(10),
            'Failed Step: ', ISNULL(@step_name,'N/A'), CHAR(13), CHAR(10),
            'Run Date: ', CONVERT(VARCHAR(10), @as_of_date, 120), CHAR(13), CHAR(10),
            'Error Message: ', ISNULL(@error_message,'No error message'), CHAR(13), CHAR(10),
            'Action: Review ETL logs and upstream file/data dependencies.'
        );

        EXEC ops.usp_create_alert
            @alert_code = 'ETL_FAILURE',
            @alert_source = 'ETL',
            @alert_category = 'FAILURE',
            @source_record_key = CAST(@etl_run_id AS VARCHAR(200)),
            @correlation_key = CONCAT('ETL|', ISNULL(@process_name,'UNKNOWN')),
            @dedupe_key = @dedupe_key,
            @run_date = @as_of_date,
            @alert_subject = @subject,
            @alert_message = @body,
            @severity = 'CRITICAL',
            @priority_score = 100;

        FETCH NEXT FROM c INTO @etl_run_id, @process_name, @source_file_name, @step_name, @error_message;
    END

    CLOSE c;
    DEALLOCATE c;
END;
GO

/* =========================================================
   12. DQ ALERT GENERATION
   ========================================================= */
CREATE OR ALTER PROCEDURE ops.usp_generate_alerts_for_dq
(
    @as_of_date DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    IF @as_of_date IS NULL
        SET @as_of_date = CAST(GETDATE() AS DATE);

    DECLARE
        @dq_result_id BIGINT,
        @rule_code VARCHAR(100),
        @rule_name VARCHAR(200),
        @rule_category VARCHAR(100),
        @rule_status VARCHAR(20),
        @severity VARCHAR(20),
        @metric_value DECIMAL(18,4),
        @threshold_min DECIMAL(18,4),
        @threshold_max DECIMAL(18,4),
        @subject VARCHAR(300),
        @body VARCHAR(MAX),
        @alert_code VARCHAR(100),
        @priority_score INT,
        @dedupe_key VARCHAR(500);

    DECLARE c CURSOR LOCAL FAST_FORWARD FOR
        SELECT
            dq_result_id,
            rule_code,
            rule_name,
            rule_category,
            status,
            severity,
            metric_value,
            threshold_min,
            threshold_max
        FROM dq.dq_result
        WHERE metric_date = @as_of_date
          AND status IN ('FAIL','WARN');

    OPEN c;
    FETCH NEXT FROM c INTO
        @dq_result_id, @rule_code, @rule_name, @rule_category, @rule_status, @severity,
        @metric_value, @threshold_min, @threshold_max;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @alert_code =
            CASE WHEN @rule_status = 'WARN' THEN 'DQ_WARNING' ELSE 'DQ_FAILURE' END;

        SET @priority_score =
            CASE
                WHEN @rule_status = 'FAIL' AND @severity = 'CRITICAL' THEN 90
                WHEN @rule_status = 'FAIL' THEN 80
                ELSE 60
            END;

        SET @dedupe_key = CONCAT('DQ|', ISNULL(CAST(@dq_result_id AS VARCHAR(50)),'0'));

        SET @subject = CONCAT('DQ ', @rule_status, ': ', ISNULL(@rule_name,'Unknown Rule'));

        SET @body = CONCAT(
            'A data quality issue was detected.', CHAR(13), CHAR(10),
            'Rule Code: ', ISNULL(@rule_code,'N/A'), CHAR(13), CHAR(10),
            'Rule Name: ', ISNULL(@rule_name,'N/A'), CHAR(13), CHAR(10),
            'Rule Category: ', ISNULL(@rule_category,'N/A'), CHAR(13), CHAR(10),
            'Result Status: ', ISNULL(@rule_status,'N/A'), CHAR(13), CHAR(10),
            'Run Date: ', CONVERT(VARCHAR(10), @as_of_date, 120), CHAR(13), CHAR(10),
            'Metric Value: ', ISNULL(CAST(@metric_value AS VARCHAR(50)),'N/A'), CHAR(13), CHAR(10),
            'Threshold Min: ', ISNULL(CAST(@threshold_min AS VARCHAR(50)),'N/A'), CHAR(13), CHAR(10),
            'Threshold Max: ', ISNULL(CAST(@threshold_max AS VARCHAR(50)),'N/A'), CHAR(13), CHAR(10),
            'Action: Review the DQ result and upstream ETL or source-system conditions.'
        );

        EXEC ops.usp_create_alert
            @alert_code = @alert_code,
            @alert_source = 'DQ',
            @alert_category = CASE WHEN @rule_status = 'WARN' THEN 'THRESHOLD' ELSE 'FAILURE' END,
            @source_record_key = CAST(@dq_result_id AS VARCHAR(200)),
            @correlation_key = CONCAT('DQ|', ISNULL(@rule_code,'UNKNOWN')),
            @dedupe_key = @dedupe_key,
            @run_date = @as_of_date,
            @alert_subject = @subject,
            @alert_message = @body,
            @severity = CASE WHEN @rule_status = 'WARN' THEN 'MEDIUM' ELSE 'HIGH' END,
            @priority_score = @priority_score;

        FETCH NEXT FROM c INTO
            @dq_result_id, @rule_code, @rule_name, @rule_category, @rule_status, @severity,
            @metric_value, @threshold_min, @threshold_max;
    END

    CLOSE c;
    DEALLOCATE c;
END;
GO

/* =========================================================
   13. MISSING RUN CHECKS
   ========================================================= */
CREATE OR ALTER PROCEDURE ops.usp_generate_missing_run_alerts
(
    @as_of_date DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    IF @as_of_date IS NULL
        SET @as_of_date = CAST(GETDATE() AS DATE);

    IF NOT EXISTS
    (
        SELECT 1
        FROM etl.etl_run_log
        WHERE CAST(start_datetime AS DATE) = @as_of_date
    )
    BEGIN
        EXEC ops.usp_create_alert
            @alert_code = 'ETL_MISSING_RUN',
            @alert_source = 'ETL',
            @alert_category = 'STALENESS',
            @source_record_key = CONVERT(VARCHAR(10), @as_of_date, 120),
            @correlation_key = 'ETL_MISSING_RUN',
            @dedupe_key = CONCAT('ETL_MISSING_RUN|', CONVERT(VARCHAR(10), @as_of_date, 120)),
            @run_date = @as_of_date,
            @alert_subject = CONCAT('CRITICAL: No ETL run found for ', CONVERT(VARCHAR(10), @as_of_date, 120)),
            @alert_message = 'No ETL run was found for the expected processing date. Investigate scheduler, job execution, and upstream file availability.',
            @severity = 'CRITICAL',
            @priority_score = 95;
    END
END;
GO

/* =========================================================
   14. REPORTING STALENESS CHECK
   ========================================================= */
CREATE OR ALTER PROCEDURE ops.usp_generate_stale_snapshot_alerts
(
    @as_of_date DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    IF @as_of_date IS NULL
        SET @as_of_date = CAST(GETDATE() AS DATE);

    IF OBJECT_ID('rpt.daily_kpi_snapshot', 'U') IS NOT NULL
    BEGIN
        IF NOT EXISTS
        (
            SELECT 1
            FROM rpt.daily_kpi_snapshot
            WHERE snapshot_date = @as_of_date
        )
        BEGIN
            EXEC ops.usp_create_alert
                @alert_code = 'REPORTING_STALE',
                @alert_source = 'REPORTING',
                @alert_category = 'STALENESS',
                @source_record_key = CONVERT(VARCHAR(10), @as_of_date, 120),
                @correlation_key = 'REPORTING_STALE',
                @dedupe_key = CONCAT('REPORTING_STALE|', CONVERT(VARCHAR(10), @as_of_date, 120)),
                @run_date = @as_of_date,
                @alert_subject = CONCAT('HIGH: Reporting snapshot missing for ', CONVERT(VARCHAR(10), @as_of_date, 120)),
                @alert_message = 'Reporting snapshot was not found for the expected date. Investigate reporting refresh jobs and ETL/DQ dependencies.',
                @severity = 'HIGH',
                @priority_score = 70;
        END
    END
END;
GO

/* =========================================================
   15. ESCALATION
   ========================================================= */
CREATE OR ALTER PROCEDURE ops.usp_escalate_open_alerts
AS
BEGIN
    SET NOCOUNT ON;

    ;WITH candidates AS
    (
        SELECT
            q.alert_id,
            q.alert_status,
            q.severity,
            q.escalation_level,
            q.created_datetime,
            c.escalation_after_minutes
        FROM ops.alert_queue q
        INNER JOIN ops.alert_config c
            ON q.alert_config_id = c.alert_config_id
        WHERE q.alert_status IN ('NEW','SENT','FAILED','ACKNOWLEDGED')
          AND c.escalation_after_minutes IS NOT NULL
          AND q.created_datetime <= DATEADD(MINUTE, -c.escalation_after_minutes, SYSDATETIME())
          AND q.escalation_level = 0
    )
    UPDATE q
    SET
        alert_status = 'ESCALATED',
        escalation_level = 1,
        updated_datetime = SYSDATETIME()
    OUTPUT inserted.alert_id
    INTO #escalated_ids
    FROM ops.alert_queue q
    INNER JOIN candidates c
        ON q.alert_id = c.alert_id;
END;
GO

CREATE OR ALTER PROCEDURE ops.usp_escalate_open_alerts
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Escalated TABLE (alert_id BIGINT);

    ;WITH candidates AS
    (
        SELECT
            q.alert_id
        FROM ops.alert_queue q
        INNER JOIN ops.alert_config c
            ON q.alert_config_id = c.alert_config_id
        WHERE q.alert_status IN ('NEW','SENT','FAILED','ACKNOWLEDGED')
          AND c.escalation_after_minutes IS NOT NULL
          AND q.created_datetime <= DATEADD(MINUTE, -c.escalation_after_minutes, SYSDATETIME())
          AND q.escalation_level = 0
    )
    UPDATE q
    SET
        alert_status = 'ESCALATED',
        escalation_level = 1,
        updated_datetime = SYSDATETIME()
    OUTPUT inserted.alert_id INTO @Escalated(alert_id)
    FROM ops.alert_queue q
    INNER JOIN candidates c
        ON q.alert_id = c.alert_id;

    INSERT INTO ops.alert_event_history
    (
        alert_id, event_type, event_message, old_status, new_status, event_by
    )
    SELECT
        e.alert_id,
        'ESCALATED',
        'Alert exceeded escalation threshold.',
        NULL,
        'ESCALATED',
        'SQL Procedure'
    FROM @Escalated e;

    IF EXISTS (SELECT 1 FROM @Escalated)
    BEGIN
        EXEC ops.usp_create_alert
            @alert_code = 'OPEN_CRITICAL_AGEING',
            @alert_source = 'PLATFORM',
            @alert_category = 'SLA',
            @source_record_key = NULL,
            @correlation_key = 'ESCALATED_CRITICAL_ALERTS',
            @dedupe_key = CONCAT('OPEN_CRITICAL_AGEING|', CONVERT(VARCHAR(16), GETDATE(), 120)),
            @run_date = CAST(GETDATE() AS DATE),
            @alert_subject = 'CRITICAL: One or more alerts exceeded escalation threshold',
            @alert_message = 'One or more operational alerts have remained unresolved past escalation threshold and were escalated.',
            @severity = 'CRITICAL',
            @priority_score = 98;
    END
END;
GO

/* =========================================================
   16. RETRY FAILED ALERTS
   ========================================================= */
CREATE OR ALTER PROCEDURE ops.usp_retry_failed_alerts
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE q
    SET
        alert_status = 'NEW',
        updated_datetime = SYSDATETIME()
    FROM ops.alert_queue q
    INNER JOIN ops.alert_config c
        ON q.alert_config_id = c.alert_config_id
    WHERE q.alert_status = 'FAILED'
      AND q.retry_count < c.max_retry_count;

    INSERT INTO ops.alert_event_history
    (
        alert_id, event_type, event_message, old_status, new_status, event_by
    )
    SELECT
        q.alert_id,
        'RETRIED',
        'Alert reset to NEW for retry.',
        'FAILED',
        'NEW',
        'SQL Procedure'
    FROM ops.alert_queue q
    INNER JOIN ops.alert_config c
        ON q.alert_config_id = c.alert_config_id
    WHERE q.alert_status = 'NEW'
      AND q.retry_count <= c.max_retry_count
      AND q.last_retry_datetime IS NOT NULL;
END;
GO

/* =========================================================
   17. OPERATIONAL SUMMARY REFRESH
   ========================================================= */
CREATE OR ALTER PROCEDURE ops.usp_refresh_alert_operational_summary
(
    @summary_date DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    IF @summary_date IS NULL
        SET @summary_date = CAST(GETDATE() AS DATE);

    DELETE
    FROM ops.alert_operational_summary
    WHERE summary_date = @summary_date;

    INSERT INTO ops.alert_operational_summary
    (
        summary_date, alert_source, alert_category, severity,
        new_alert_count, acknowledged_alert_count, sent_alert_count,
        failed_alert_count, escalated_alert_count, closed_alert_count,
        open_alert_count, avg_alert_age_hours, sla_breach_count
    )
    SELECT
        @summary_date,
        alert_source,
        alert_category,
        severity,
        SUM(CASE WHEN alert_status = 'NEW' THEN 1 ELSE 0 END),
        SUM(CASE WHEN alert_status = 'ACKNOWLEDGED' THEN 1 ELSE 0 END),
        SUM(CASE WHEN alert_status = 'SENT' THEN 1 ELSE 0 END),
        SUM(CASE WHEN alert_status = 'FAILED' THEN 1 ELSE 0 END),
        SUM(CASE WHEN alert_status = 'ESCALATED' THEN 1 ELSE 0 END),
        SUM(CASE WHEN alert_status = 'CLOSED' THEN 1 ELSE 0 END),
        SUM(CASE WHEN alert_status IN ('NEW','ACKNOWLEDGED','SENT','FAILED','ESCALATED') THEN 1 ELSE 0 END),
        CAST(AVG(CASE WHEN alert_status IN ('NEW','ACKNOWLEDGED','SENT','FAILED','ESCALATED')
                 THEN DATEDIFF(MINUTE, created_datetime, SYSDATETIME()) / 60.0 END) AS DECIMAL(18,2)),
        SUM(CASE WHEN sla_due_datetime IS NOT NULL
                  AND alert_status IN ('NEW','ACKNOWLEDGED','SENT','FAILED','ESCALATED')
                  AND sla_due_datetime < SYSDATETIME()
                 THEN 1 ELSE 0 END)
    FROM ops.alert_queue
    WHERE CAST(created_datetime AS DATE) = @summary_date
    GROUP BY alert_source, alert_category, severity;
END;
GO

/* =========================================================
   18. MASTER DAILY MONITORING
   ========================================================= */
CREATE OR ALTER PROCEDURE ops.usp_run_daily_monitoring
(
    @as_of_date DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    IF @as_of_date IS NULL
        SET @as_of_date = CAST(GETDATE() AS DATE);

    EXEC ops.usp_generate_alerts_for_etl @as_of_date = @as_of_date;
    EXEC ops.usp_generate_alerts_for_dq @as_of_date = @as_of_date;
    EXEC ops.usp_generate_missing_run_alerts @as_of_date = @as_of_date;
    EXEC ops.usp_generate_stale_snapshot_alerts @as_of_date = @as_of_date;
    EXEC ops.usp_retry_failed_alerts;
    EXEC ops.usp_escalate_open_alerts;
    EXEC ops.usp_refresh_alert_operational_summary @summary_date = @as_of_date;
END;
GO

/* =========================================================
   19. REPORTING VIEWS
   ========================================================= */
CREATE OR ALTER VIEW ops.vw_open_alerts
AS
SELECT
    q.alert_id,
    q.alert_code,
    q.alert_source,
    q.alert_category,
    q.run_date,
    q.alert_subject,
    q.severity,
    q.priority_score,
    q.alert_status,
    q.owner_name,
    q.acknowledged_by,
    q.acknowledged_datetime,
    q.escalation_level,
    q.retry_count,
    q.sla_due_datetime,
    DATEDIFF(MINUTE, q.created_datetime, SYSDATETIME()) / 60.0 AS current_age_hours,
    q.created_datetime,
    c.notify_group,
    c.notify_emails,
    c.escalation_emails
FROM ops.alert_queue q
LEFT JOIN ops.alert_config c
    ON q.alert_config_id = c.alert_config_id
WHERE q.alert_status IN ('NEW','ACKNOWLEDGED','SENT','FAILED','ESCALATED');
GO

CREATE OR ALTER VIEW ops.vw_alert_history_detail
AS
SELECT
    q.alert_id,
    q.alert_code,
    q.alert_source,
    q.alert_category,
    q.alert_subject,
    q.severity,
    q.alert_status,
    q.created_datetime,
    h.event_type,
    h.old_status,
    h.new_status,
    h.event_message,
    h.event_datetime,
    h.event_by
FROM ops.alert_queue q
INNER JOIN ops.alert_event_history h
    ON q.alert_id = h.alert_id;
GO

CREATE OR ALTER VIEW ops.vw_alert_sla_breaches
AS
SELECT
    alert_id,
    alert_code,
    alert_source,
    alert_category,
    severity,
    alert_status,
    owner_name,
    created_datetime,
    sla_due_datetime,
    DATEDIFF(MINUTE, sla_due_datetime, SYSDATETIME()) / 60.0 AS hours_past_sla
FROM ops.alert_queue
WHERE sla_due_datetime IS NOT NULL
  AND alert_status IN ('NEW','ACKNOWLEDGED','SENT','FAILED','ESCALATED')
  AND sla_due_datetime < SYSDATETIME();
GO

CREATE OR ALTER VIEW ops.vw_alert_backlog
AS
SELECT
    alert_source,
    alert_category,
    severity,
    COUNT(*) AS open_alert_count,
    CAST(AVG(DATEDIFF(MINUTE, created_datetime, SYSDATETIME()) / 60.0) AS DECIMAL(18,2)) AS avg_open_age_hours
FROM ops.alert_queue
WHERE alert_status IN ('NEW','ACKNOWLEDGED','SENT','FAILED','ESCALATED')
GROUP BY alert_source, alert_category, severity;
GO

CREATE OR ALTER VIEW ops.vw_alert_operational_summary
AS
SELECT *
FROM ops.alert_operational_summary;
GO
