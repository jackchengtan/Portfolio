/* =========================================================
   Project: Enhanced Healthcare Data Quality Monitoring
   Platform: SQL Server
   Target DB: PortfolioHealthcareETL
   Purpose:
       Production-style post-load data quality framework for
       patient encounter ETL monitoring.
   ========================================================= */

USE master;
GO

IF DB_ID('PortfolioHealthcareETL') IS NULL
BEGIN
    RAISERROR('Database PortfolioHealthcareETL does not exist. Run the ETL script first.', 16, 1);
END
GO

USE PortfolioHealthcareETL;
GO

/* =========================================================
   1. CREATE SCHEMA
   ========================================================= */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dq')
    EXEC('CREATE SCHEMA dq');
GO

/* =========================================================
   2. DROP OBJECTS IF RE-RUNNING
   ========================================================= */
IF OBJECT_ID('dq.usp_run_data_quality', 'P') IS NOT NULL DROP PROCEDURE dq.usp_run_data_quality;
GO
IF OBJECT_ID('dq.usp_check_cost_outliers', 'P') IS NOT NULL DROP PROCEDURE dq.usp_check_cost_outliers;
GO
IF OBJECT_ID('dq.usp_check_distribution_drift', 'P') IS NOT NULL DROP PROCEDURE dq.usp_check_distribution_drift;
GO
IF OBJECT_ID('dq.usp_check_referential_integrity', 'P') IS NOT NULL DROP PROCEDURE dq.usp_check_referential_integrity;
GO
IF OBJECT_ID('dq.usp_check_duplicate_business_keys', 'P') IS NOT NULL DROP PROCEDURE dq.usp_check_duplicate_business_keys;
GO
IF OBJECT_ID('dq.usp_check_completeness', 'P') IS NOT NULL DROP PROCEDURE dq.usp_check_completeness;
GO
IF OBJECT_ID('dq.usp_check_daily_volume_anomaly', 'P') IS NOT NULL DROP PROCEDURE dq.usp_check_daily_volume_anomaly;
GO
IF OBJECT_ID('dq.usp_capture_daily_metrics', 'P') IS NOT NULL DROP PROCEDURE dq.usp_capture_daily_metrics;
GO
IF OBJECT_ID('dq.usp_write_issue_detail', 'P') IS NOT NULL DROP PROCEDURE dq.usp_write_issue_detail;
GO
IF OBJECT_ID('dq.usp_write_result', 'P') IS NOT NULL DROP PROCEDURE dq.usp_write_result;
GO
IF OBJECT_ID('dq.dq_issue_detail', 'U') IS NOT NULL DROP TABLE dq.dq_issue_detail;
GO
IF OBJECT_ID('dq.dq_result', 'U') IS NOT NULL DROP TABLE dq.dq_result;
GO
IF OBJECT_ID('dq.dq_run', 'U') IS NOT NULL DROP TABLE dq.dq_run;
GO
IF OBJECT_ID('dq.daily_metric_snapshot', 'U') IS NOT NULL DROP TABLE dq.daily_metric_snapshot;
GO
IF OBJECT_ID('dq.dq_rule_catalog', 'U') IS NOT NULL DROP TABLE dq.dq_rule_catalog;
GO

/* =========================================================
   3. RULE CATALOG
   ========================================================= */
CREATE TABLE dq.dq_rule_catalog
(
    rule_id                    INT IDENTITY(1,1) PRIMARY KEY,
    rule_code                  VARCHAR(100) NOT NULL UNIQUE,
    rule_name                  VARCHAR(200) NOT NULL,
    rule_category              VARCHAR(100) NOT NULL,
    severity                   VARCHAR(20)  NOT NULL,   -- INFO / WARN / CRITICAL
    threshold_type             VARCHAR(50)  NULL,       -- RANGE / PERCENT / STDDEV / COUNT
    threshold_min              DECIMAL(18,4) NULL,
    threshold_max              DECIMAL(18,4) NULL,
    stddev_multiplier          DECIMAL(18,4) NULL,
    lookback_days              INT NULL,
    minimum_history_days       INT NULL,
    is_active                  BIT NOT NULL DEFAULT(1),
    description                VARCHAR(1000) NULL,
    created_datetime           DATETIME2(0) NOT NULL DEFAULT SYSDATETIME()
);
GO

/* =========================================================
   4. RUN HEADER
   ========================================================= */
CREATE TABLE dq.dq_run
(
    dq_run_id                  BIGINT IDENTITY(1,1) PRIMARY KEY,
    run_datetime               DATETIME2(0) NOT NULL DEFAULT SYSDATETIME(),
    run_date                   DATE NOT NULL DEFAULT CAST(GETDATE() AS DATE),
    source_file_name           VARCHAR(260) NULL,
    status                     VARCHAR(20) NOT NULL DEFAULT 'STARTED',
    checks_run                 INT NULL,
    checks_failed              INT NULL,
    checks_warned              INT NULL,
    created_by                 VARCHAR(128) NULL DEFAULT SUSER_SNAME(),
    error_message              VARCHAR(MAX) NULL
);
GO

/* =========================================================
   5. METRIC SNAPSHOT
   Daily aggregated metrics for trend monitoring
   ========================================================= */
CREATE TABLE dq.daily_metric_snapshot
(
    metric_snapshot_id         BIGINT IDENTITY(1,1) PRIMARY KEY,
    metric_date                DATE NOT NULL,
    source_file_name           VARCHAR(260) NULL,
    total_records              INT NOT NULL,
    distinct_patients          INT NOT NULL,
    distinct_practices         INT NOT NULL,
    distinct_clinicians        INT NOT NULL,
    avg_cost                   DECIMAL(18,4) NULL,
    min_cost                   DECIMAL(18,4) NULL,
    max_cost                   DECIMAL(18,4) NULL,
    total_cost                 DECIMAL(18,4) NULL,
    null_diagnosis_count       INT NOT NULL,
    diagnosis_null_rate        DECIMAL(18,4) NOT NULL,
    created_datetime           DATETIME2(0) NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT UQ_dq_daily_metric UNIQUE(metric_date, source_file_name)
);
GO

/* =========================================================
   6. RESULT TABLE
   One row per executed rule
   ========================================================= */
CREATE TABLE dq.dq_result
(
    dq_result_id               BIGINT IDENTITY(1,1) PRIMARY KEY,
    dq_run_id                  BIGINT NOT NULL,
    rule_id                    INT NOT NULL,
    rule_code                  VARCHAR(100) NOT NULL,
    rule_name                  VARCHAR(200) NOT NULL,
    rule_category              VARCHAR(100) NOT NULL,
    severity                   VARCHAR(20)  NOT NULL,
    metric_date                DATE NULL,
    dimension_1                VARCHAR(200) NULL,
    metric_value               DECIMAL(18,4) NULL,
    baseline_value             DECIMAL(18,4) NULL,
    threshold_min              DECIMAL(18,4) NULL,
    threshold_max              DECIMAL(18,4) NULL,
    status                     VARCHAR(20) NOT NULL,    -- PASS / WARN / FAIL / INFO
    rows_affected              INT NULL,
    message_text               VARCHAR(2000) NULL,
    created_datetime           DATETIME2(0) NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT FK_dq_result_run FOREIGN KEY (dq_run_id) REFERENCES dq.dq_run(dq_run_id),
    CONSTRAINT FK_dq_result_rule FOREIGN KEY (rule_id) REFERENCES dq.dq_rule_catalog(rule_id)
);
GO

/* =========================================================
   7. ISSUE DETAIL TABLE
   Stores granular failure detail for investigation
   ========================================================= */
CREATE TABLE dq.dq_issue_detail
(
    dq_issue_detail_id         BIGINT IDENTITY(1,1) PRIMARY KEY,
    dq_result_id               BIGINT NOT NULL,
    issue_type                 VARCHAR(100) NOT NULL,
    encounter_id               INT NULL,
    patient_id                 INT NULL,
    practice_id                INT NULL,
    encounter_date             DATE NULL,
    encounter_type_code        VARCHAR(20) NULL,
    clinician_id               INT NULL,
    issue_value                VARCHAR(500) NULL,
    issue_notes                VARCHAR(2000) NULL,
    created_datetime           DATETIME2(0) NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT FK_dq_issue_detail_result FOREIGN KEY (dq_result_id) REFERENCES dq.dq_result(dq_result_id)
);
GO

/* =========================================================
   8. SEED RULE CATALOG
   ========================================================= */
INSERT INTO dq.dq_rule_catalog
(
    rule_code, rule_name, rule_category, severity,
    threshold_type, threshold_min, threshold_max,
    stddev_multiplier, lookback_days, minimum_history_days, description
)
VALUES
('VOL_DAILY_TOTAL',       'Daily encounter volume anomaly',            'Volume',          'CRITICAL', 'STDDEV', NULL, NULL, 2.0, 14, 7, 'Flags daily volume outside rolling avg +/- N std dev'),
('COMP_DIAGNOSIS_NULL',   'Diagnosis completeness rate',               'Completeness',    'WARN',     'PERCENT', NULL, 0.10, NULL, NULL, NULL, 'Flags diagnosis null rate above 10%'),
('DUP_BUSINESS_KEY',      'Duplicate business key check',              'Uniqueness',      'CRITICAL', 'COUNT',   0, 0, NULL, NULL, NULL, 'Business key duplicates should not exist in curated table'),
('REF_PRACTICE',          'Practice reference integrity',              'Referential',     'CRITICAL', 'COUNT',   0, 0, NULL, NULL, NULL, 'All practice_id values must exist in dim_practice'),
('REF_ENCOUNTER_TYPE',    'Encounter type reference integrity',        'Referential',     'CRITICAL', 'COUNT',   0, 0, NULL, NULL, NULL, 'All encounter_type_code values must exist in dim_encounter_type'),
('DIST_ENCOUNTER_TYPE',   'Encounter type distribution drift',         'Distribution',    'WARN',     'PERCENT', NULL, 0.15, NULL, 14, 7, 'Flags encounter type share variance above 15 percentage points'),
('OUTLIER_COST',          'High cost outlier check',                   'Outlier',         'WARN',     'STDDEV',  NULL, NULL, 3.0, 30, 10, 'Flags records with unusually high cost'),
('VOL_PRACTICE_MINIMUM',  'Practice daily minimum volume check',       'Volume',          'WARN',     'COUNT',   1, NULL, NULL, NULL, NULL, 'Flags practices with zero or unusually low daily volume');
GO

/* =========================================================
   9. HELPER: WRITE RESULT
   ========================================================= */
CREATE OR ALTER PROCEDURE dq.usp_write_result
(
    @dq_run_id        BIGINT,
    @rule_code        VARCHAR(100),
    @metric_date      DATE = NULL,
    @dimension_1      VARCHAR(200) = NULL,
    @metric_value     DECIMAL(18,4) = NULL,
    @baseline_value   DECIMAL(18,4) = NULL,
    @threshold_min    DECIMAL(18,4) = NULL,
    @threshold_max    DECIMAL(18,4) = NULL,
    @status           VARCHAR(20),
    @rows_affected    INT = NULL,
    @message_text     VARCHAR(2000) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dq.dq_result
    (
        dq_run_id, rule_id, rule_code, rule_name, rule_category, severity,
        metric_date, dimension_1, metric_value, baseline_value,
        threshold_min, threshold_max, status, rows_affected, message_text
    )
    SELECT
        @dq_run_id,
        c.rule_id,
        c.rule_code,
        c.rule_name,
        c.rule_category,
        c.severity,
        @metric_date,
        @dimension_1,
        @metric_value,
        @baseline_value,
        @threshold_min,
        @threshold_max,
        @status,
        @rows_affected,
        @message_text
    FROM dq.dq_rule_catalog c
    WHERE c.rule_code = @rule_code;
END;
GO

/* =========================================================
   10. HELPER: WRITE ISSUE DETAIL
   ========================================================= */
CREATE OR ALTER PROCEDURE dq.usp_write_issue_detail
(
    @dq_result_id         BIGINT,
    @issue_type           VARCHAR(100),
    @encounter_id         INT = NULL,
    @patient_id           INT = NULL,
    @practice_id          INT = NULL,
    @encounter_date       DATE = NULL,
    @encounter_type_code  VARCHAR(20) = NULL,
    @clinician_id         INT = NULL,
    @issue_value          VARCHAR(500) = NULL,
    @issue_notes          VARCHAR(2000) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dq.dq_issue_detail
    (
        dq_result_id, issue_type, encounter_id, patient_id, practice_id,
        encounter_date, encounter_type_code, clinician_id, issue_value, issue_notes
    )
    VALUES
    (
        @dq_result_id, @issue_type, @encounter_id, @patient_id, @practice_id,
        @encounter_date, @encounter_type_code, @clinician_id, @issue_value, @issue_notes
    );
END;
GO

/* =========================================================
   11. CAPTURE DAILY METRICS
   ========================================================= */
CREATE OR ALTER PROCEDURE dq.usp_capture_daily_metrics
(
    @metric_date       DATE = NULL,
    @source_file_name  VARCHAR(260) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    IF @metric_date IS NULL
        SET @metric_date = CAST(GETDATE() AS DATE);

    MERGE dq.daily_metric_snapshot AS tgt
    USING
    (
        SELECT
            CAST(encounter_date AS DATE) AS metric_date,
            MAX(source_file_name) AS source_file_name,
            COUNT(*) AS total_records,
            COUNT(DISTINCT patient_id) AS distinct_patients,
            COUNT(DISTINCT practice_id) AS distinct_practices,
            COUNT(DISTINCT clinician_id) AS distinct_clinicians,
            AVG(CAST(cost AS DECIMAL(18,4))) AS avg_cost,
            MIN(CAST(cost AS DECIMAL(18,4))) AS min_cost,
            MAX(CAST(cost AS DECIMAL(18,4))) AS max_cost,
            SUM(CAST(cost AS DECIMAL(18,4))) AS total_cost,
            SUM(CASE WHEN diagnosis_code IS NULL OR LTRIM(RTRIM(diagnosis_code)) = '' THEN 1 ELSE 0 END) AS null_diagnosis_count,
            CAST(SUM(CASE WHEN diagnosis_code IS NULL OR LTRIM(RTRIM(diagnosis_code)) = '' THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*),0) AS DECIMAL(18,4)) AS diagnosis_null_rate
        FROM dbo.patient_encounter
        WHERE CAST(encounter_date AS DATE) = @metric_date
        GROUP BY CAST(encounter_date AS DATE)
    ) src
    ON tgt.metric_date = src.metric_date
   AND ISNULL(tgt.source_file_name,'') = ISNULL(src.source_file_name,'')
    WHEN MATCHED THEN
        UPDATE SET
            tgt.total_records = src.total_records,
            tgt.distinct_patients = src.distinct_patients,
            tgt.distinct_practices = src.distinct_practices,
            tgt.distinct_clinicians = src.distinct_clinicians,
            tgt.avg_cost = src.avg_cost,
            tgt.min_cost = src.min_cost,
            tgt.max_cost = src.max_cost,
            tgt.total_cost = src.total_cost,
            tgt.null_diagnosis_count = src.null_diagnosis_count,
            tgt.diagnosis_null_rate = src.diagnosis_null_rate
    WHEN NOT MATCHED THEN
        INSERT
        (
            metric_date, source_file_name, total_records, distinct_patients, distinct_practices,
            distinct_clinicians, avg_cost, min_cost, max_cost, total_cost,
            null_diagnosis_count, diagnosis_null_rate
        )
        VALUES
        (
            src.metric_date, src.source_file_name, src.total_records, src.distinct_patients, src.distinct_practices,
            src.distinct_clinicians, src.avg_cost, src.min_cost, src.max_cost, src.total_cost,
            src.null_diagnosis_count, src.diagnosis_null_rate
        );
END;
GO

/* =========================================================
   12. DAILY VOLUME ANOMALY
   ========================================================= */
CREATE OR ALTER PROCEDURE dq.usp_check_daily_volume_anomaly
(
    @dq_run_id     BIGINT,
    @metric_date   DATE
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @rule_code VARCHAR(100) = 'VOL_DAILY_TOTAL';
    DECLARE @lookback_days INT;
    DECLARE @min_history INT;
    DECLARE @stddev_multiplier DECIMAL(18,4);

    SELECT
        @lookback_days = lookback_days,
        @min_history = minimum_history_days,
        @stddev_multiplier = stddev_multiplier
    FROM dq.dq_rule_catalog
    WHERE rule_code = @rule_code;

    ;WITH hist AS
    (
        SELECT metric_date, total_records
        FROM dq.daily_metric_snapshot
        WHERE metric_date < @metric_date
          AND metric_date >= DATEADD(DAY, -@lookback_days, @metric_date)
    ),
    cur AS
    (
        SELECT metric_date, total_records
        FROM dq.daily_metric_snapshot
        WHERE metric_date = @metric_date
    ),
    calc AS
    (
        SELECT
            c.metric_date,
            c.total_records,
            AVG(h.total_records * 1.0) AS baseline_avg,
            STDEV(h.total_records * 1.0) AS baseline_std,
            COUNT(*) AS hist_days
        FROM cur c
        CROSS JOIN hist h
        GROUP BY c.metric_date, c.total_records
    )
    SELECT *
    INTO #dq_vol
    FROM calc;

    IF NOT EXISTS (SELECT 1 FROM #dq_vol)
    BEGIN
        EXEC dq.usp_write_result
            @dq_run_id = @dq_run_id,
            @rule_code = @rule_code,
            @metric_date = @metric_date,
            @status = 'INFO',
            @message_text = 'No current metric snapshot found for requested date.';
        RETURN;
    END

    DECLARE @metric_value DECIMAL(18,4);
    DECLARE @baseline DECIMAL(18,4);
    DECLARE @baseline_std DECIMAL(18,4);
    DECLARE @hist_days INT;
    DECLARE @threshold_min DECIMAL(18,4);
    DECLARE @threshold_max DECIMAL(18,4);
    DECLARE @status VARCHAR(20);

    SELECT
        @metric_value = total_records,
        @baseline = baseline_avg,
        @baseline_std = baseline_std,
        @hist_days = hist_days
    FROM #dq_vol;

    IF @hist_days < ISNULL(@min_history, 1)
    BEGIN
        EXEC dq.usp_write_result
            @dq_run_id = @dq_run_id,
            @rule_code = @rule_code,
            @metric_date = @metric_date,
            @metric_value = @metric_value,
            @baseline_value = @baseline,
            @status = 'INFO',
            @message_text = 'Insufficient history to evaluate daily volume anomaly.';
        RETURN;
    END

    SET @threshold_min = @baseline - ISNULL(@stddev_multiplier, 2.0) * ISNULL(@baseline_std, 0);
    SET @threshold_max = @baseline + ISNULL(@stddev_multiplier, 2.0) * ISNULL(@baseline_std, 0);

    SET @status =
        CASE
            WHEN @metric_value BETWEEN @threshold_min AND @threshold_max THEN 'PASS'
            ELSE 'FAIL'
        END;

    EXEC dq.usp_write_result
        @dq_run_id = @dq_run_id,
        @rule_code = @rule_code,
        @metric_date = @metric_date,
        @metric_value = @metric_value,
        @baseline_value = @baseline,
        @threshold_min = @threshold_min,
        @threshold_max = @threshold_max,
        @status = @status,
        @rows_affected = CASE WHEN @status = 'FAIL' THEN @metric_value ELSE 0 END,
        @message_text = 'Daily total record count compared against rolling baseline.';
END;
GO

/* =========================================================
   13. COMPLETENESS CHECK
   ========================================================= */
CREATE OR ALTER PROCEDURE dq.usp_check_completeness
(
    @dq_run_id     BIGINT,
    @metric_date   DATE
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @rule_code VARCHAR(100) = 'COMP_DIAGNOSIS_NULL';
    DECLARE @threshold_max DECIMAL(18,4);

    SELECT @threshold_max = threshold_max
    FROM dq.dq_rule_catalog
    WHERE rule_code = @rule_code;

    DECLARE @metric_value DECIMAL(18,4);
    DECLARE @rows_affected INT;

    SELECT
        @metric_value = diagnosis_null_rate,
        @rows_affected = null_diagnosis_count
    FROM dq.daily_metric_snapshot
    WHERE metric_date = @metric_date;

    IF @metric_value IS NULL
    BEGIN
        EXEC dq.usp_write_result
            @dq_run_id = @dq_run_id,
            @rule_code = @rule_code,
            @metric_date = @metric_date,
            @status = 'INFO',
            @message_text = 'No metric snapshot available for completeness check.';
        RETURN;
    END

    EXEC dq.usp_write_result
        @dq_run_id = @dq_run_id,
        @rule_code = @rule_code,
        @metric_date = @metric_date,
        @metric_value = @metric_value,
        @threshold_max = @threshold_max,
        @status = CASE WHEN @metric_value <= @threshold_max THEN 'PASS' ELSE 'WARN' END,
        @rows_affected = @rows_affected,
        @message_text = 'Diagnosis code null rate evaluated for the day.';
END;
GO

/* =========================================================
   14. DUPLICATE BUSINESS KEY CHECK
   ========================================================= */
CREATE OR ALTER PROCEDURE dq.usp_check_duplicate_business_keys
(
    @dq_run_id     BIGINT,
    @metric_date   DATE
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @rule_code VARCHAR(100) = 'DUP_BUSINESS_KEY';

    ;WITH dupes AS
    (
        SELECT
            encounter_id,
            patient_id,
            practice_id,
            encounter_date,
            encounter_type_code,
            clinician_id,
            COUNT(*) AS cnt
        FROM dbo.patient_encounter
        WHERE encounter_date = @metric_date
        GROUP BY
            encounter_id,
            patient_id,
            practice_id,
            encounter_date,
            encounter_type_code,
            clinician_id
        HAVING COUNT(*) > 1
    )
    SELECT *
    INTO #dupes
    FROM dupes;

    DECLARE @dupe_count INT = (SELECT COUNT(*) FROM #dupes);

    EXEC dq.usp_write_result
        @dq_run_id = @dq_run_id,
        @rule_code = @rule_code,
        @metric_date = @metric_date,
        @metric_value = @dupe_count,
        @threshold_min = 0,
        @threshold_max = 0,
        @status = CASE WHEN @dupe_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        @rows_affected = @dupe_count,
        @message_text = 'Duplicate business key combinations in curated table.';

    IF @dupe_count > 0
    BEGIN
        DECLARE @dq_result_id BIGINT = SCOPE_IDENTITY();

        INSERT INTO dq.dq_issue_detail
        (
            dq_result_id, issue_type, encounter_id, patient_id, practice_id,
            encounter_date, encounter_type_code, clinician_id, issue_value, issue_notes
        )
        SELECT
            @dq_result_id,
            'DUPLICATE_BUSINESS_KEY',
            encounter_id,
            patient_id,
            practice_id,
            encounter_date,
            encounter_type_code,
            clinician_id,
            CAST(cnt AS VARCHAR(50)),
            'Duplicate business key found in dbo.patient_encounter'
        FROM #dupes;
    END
END;
GO

/* =========================================================
   15. REFERENTIAL INTEGRITY CHECKS
   ========================================================= */
CREATE OR ALTER PROCEDURE dq.usp_check_referential_integrity
(
    @dq_run_id     BIGINT,
    @metric_date   DATE
)
AS
BEGIN
    SET NOCOUNT ON;

    /* Practice check */
    ;WITH bad_practice AS
    (
        SELECT
            p.encounter_id, p.patient_id, p.practice_id, p.encounter_date,
            p.encounter_type_code, p.clinician_id
        FROM dbo.patient_encounter p
        LEFT JOIN dbo.dim_practice d
            ON p.practice_id = d.practice_id
        WHERE p.encounter_date = @metric_date
          AND d.practice_id IS NULL
    )
    SELECT * INTO #bad_practice FROM bad_practice;

    DECLARE @bad_practice_count INT = (SELECT COUNT(*) FROM #bad_practice);

    EXEC dq.usp_write_result
        @dq_run_id = @dq_run_id,
        @rule_code = 'REF_PRACTICE',
        @metric_date = @metric_date,
        @metric_value = @bad_practice_count,
        @threshold_min = 0,
        @threshold_max = 0,
        @status = CASE WHEN @bad_practice_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        @rows_affected = @bad_practice_count,
        @message_text = 'Practice reference integrity check against dbo.dim_practice.';

    IF @bad_practice_count > 0
    BEGIN
        DECLARE @dq_result_id_1 BIGINT = SCOPE_IDENTITY();

        INSERT INTO dq.dq_issue_detail
        (
            dq_result_id, issue_type, encounter_id, patient_id, practice_id,
            encounter_date, encounter_type_code, clinician_id, issue_value, issue_notes
        )
        SELECT
            @dq_result_id_1,
            'UNKNOWN_PRACTICE',
            encounter_id, patient_id, practice_id, encounter_date,
            encounter_type_code, clinician_id,
            CAST(practice_id AS VARCHAR(50)),
            'practice_id not found in dbo.dim_practice'
        FROM #bad_practice;
    END

    /* Encounter type check */
    ;WITH bad_type AS
    (
        SELECT
            p.encounter_id, p.patient_id, p.practice_id, p.encounter_date,
            p.encounter_type_code, p.clinician_id
        FROM dbo.patient_encounter p
        LEFT JOIN dbo.dim_encounter_type d
            ON p.encounter_type_code = d.encounter_type_code
        WHERE p.encounter_date = @metric_date
          AND d.encounter_type_code IS NULL
    )
    SELECT * INTO #bad_type FROM bad_type;

    DECLARE @bad_type_count INT = (SELECT COUNT(*) FROM #bad_type);

    EXEC dq.usp_write_result
        @dq_run_id = @dq_run_id,
        @rule_code = 'REF_ENCOUNTER_TYPE',
        @metric_date = @metric_date,
        @metric_value = @bad_type_count,
        @threshold_min = 0,
        @threshold_max = 0,
        @status = CASE WHEN @bad_type_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        @rows_affected = @bad_type_count,
        @message_text = 'Encounter type integrity check against dbo.dim_encounter_type.';

    IF @bad_type_count > 0
    BEGIN
        DECLARE @dq_result_id_2 BIGINT = SCOPE_IDENTITY();

        INSERT INTO dq.dq_issue_detail
        (
            dq_result_id, issue_type, encounter_id, patient_id, practice_id,
            encounter_date, encounter_type_code, clinician_id, issue_value, issue_notes
        )
        SELECT
            @dq_result_id_2,
            'UNKNOWN_ENCOUNTER_TYPE',
            encounter_id, patient_id, practice_id, encounter_date,
            encounter_type_code, clinician_id,
            encounter_type_code,
            'encounter_type_code not found in dbo.dim_encounter_type'
        FROM #bad_type;
    END
END;
GO

/* =========================================================
   16. DISTRIBUTION DRIFT CHECK
   Compare encounter type share against recent history
   ========================================================= */
CREATE OR ALTER PROCEDURE dq.usp_check_distribution_drift
(
    @dq_run_id     BIGINT,
    @metric_date   DATE
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @rule_code VARCHAR(100) = 'DIST_ENCOUNTER_TYPE';
    DECLARE @lookback_days INT;
    DECLARE @min_history INT;
    DECLARE @threshold_max DECIMAL(18,4);

    SELECT
        @lookback_days = lookback_days,
        @min_history = minimum_history_days,
        @threshold_max = threshold_max
    FROM dq.dq_rule_catalog
    WHERE rule_code = @rule_code;

    ;WITH cur AS
    (
        SELECT
            encounter_type_code,
            COUNT(*) * 1.0 / SUM(COUNT(*)) OVER() AS current_share
        FROM dbo.patient_encounter
        WHERE encounter_date = @metric_date
        GROUP BY encounter_type_code
    ),
    hist_daily AS
    (
        SELECT
            encounter_date,
            encounter_type_code,
            COUNT(*) * 1.0 / SUM(COUNT(*)) OVER(PARTITION BY encounter_date) AS day_share
        FROM dbo.patient_encounter
        WHERE encounter_date < @metric_date
          AND encounter_date >= DATEADD(DAY, -@lookback_days, @metric_date)
        GROUP BY encounter_date, encounter_type_code
    ),
    hist AS
    (
        SELECT
            encounter_type_code,
            AVG(day_share) AS baseline_share,
            COUNT(DISTINCT encounter_date) AS hist_days
        FROM hist_daily
        GROUP BY encounter_type_code
    ),
    joined AS
    (
        SELECT
            c.encounter_type_code,
            c.current_share,
            h.baseline_share,
            h.hist_days,
            ABS(c.current_share - h.baseline_share) AS abs_diff
        FROM cur c
        LEFT JOIN hist h
            ON c.encounter_type_code = h.encounter_type_code
    )
    INSERT INTO dq.dq_result
    (
        dq_run_id, rule_id, rule_code, rule_name, rule_category, severity,
        metric_date, dimension_1, metric_value, baseline_value,
        threshold_min, threshold_max, status, rows_affected, message_text
    )
    SELECT
        @dq_run_id,
        c.rule_id,
        c.rule_code,
        c.rule_name,
        c.rule_category,
        c.severity,
        @metric_date,
        j.encounter_type_code,
        CAST(j.current_share AS DECIMAL(18,4)),
        CAST(j.baseline_share AS DECIMAL(18,4)),
        0,
        @threshold_max,
        CASE
            WHEN j.hist_days IS NULL OR j.hist_days < @min_history THEN 'INFO'
            WHEN j.abs_diff <= @threshold_max THEN 'PASS'
            ELSE 'WARN'
        END,
        NULL,
        'Encounter type share compared with recent historical average.'
    FROM joined j
    CROSS JOIN dq.dq_rule_catalog c
    WHERE c.rule_code = @rule_code;
END;
GO

/* =========================================================
   17. COST OUTLIER CHECK
   ========================================================= */
CREATE OR ALTER PROCEDURE dq.usp_check_cost_outliers
(
    @dq_run_id     BIGINT,
    @metric_date   DATE
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @rule_code VARCHAR(100) = 'OUTLIER_COST';
    DECLARE @lookback_days INT;
    DECLARE @min_history INT;
    DECLARE @stddev_multiplier DECIMAL(18,4);

    SELECT
        @lookback_days = lookback_days,
        @min_history = minimum_history_days,
        @stddev_multiplier = stddev_multiplier
    FROM dq.dq_rule_catalog
    WHERE rule_code = @rule_code;

    ;WITH hist AS
    (
        SELECT
            AVG(cost * 1.0) AS baseline_avg,
            STDEV(cost * 1.0) AS baseline_std,
            COUNT(*) AS hist_rows
        FROM dbo.patient_encounter
        WHERE encounter_date < @metric_date
          AND encounter_date >= DATEADD(DAY, -@lookback_days, @metric_date)
    ),
    flagged AS
    (
        SELECT
            p.encounter_id, p.patient_id, p.practice_id, p.encounter_date,
            p.encounter_type_code, p.clinician_id, p.cost,
            h.baseline_avg,
            h.baseline_std,
            h.hist_rows
        FROM dbo.patient_encounter p
        CROSS JOIN hist h
        WHERE p.encounter_date = @metric_date
          AND h.hist_rows >= @min_history
          AND p.cost > h.baseline_avg + @stddev_multiplier * ISNULL(h.baseline_std, 0)
    )
    SELECT * INTO #flagged_cost FROM flagged;

    DECLARE @outlier_count INT = (SELECT COUNT(*) FROM #flagged_cost);
    DECLARE @baseline_avg DECIMAL(18,4) = (SELECT TOP 1 baseline_avg FROM #flagged_cost);
    DECLARE @threshold_max DECIMAL(18,4) = (
        SELECT TOP 1 baseline_avg + @stddev_multiplier * ISNULL(baseline_std,0)
        FROM #flagged_cost
    );

    EXEC dq.usp_write_result
        @dq_run_id = @dq_run_id,
        @rule_code = @rule_code,
        @metric_date = @metric_date,
        @metric_value = @outlier_count,
        @baseline_value = @baseline_avg,
        @threshold_max = @threshold_max,
        @status = CASE WHEN @outlier_count = 0 THEN 'PASS' ELSE 'WARN' END,
        @rows_affected = @outlier_count,
        @message_text = 'Cost outliers above rolling baseline + stddev threshold.';

    IF @outlier_count > 0
    BEGIN
        DECLARE @dq_result_id BIGINT = SCOPE_IDENTITY();

        INSERT INTO dq.dq_issue_detail
        (
            dq_result_id, issue_type, encounter_id, patient_id, practice_id,
            encounter_date, encounter_type_code, clinician_id, issue_value, issue_notes
        )
        SELECT
            @dq_result_id,
            'HIGH_COST_OUTLIER',
            encounter_id, patient_id, practice_id,
            encounter_date, encounter_type_code, clinician_id,
            CAST(cost AS VARCHAR(100)),
            CONCAT('Cost exceeds dynamic outlier threshold. Baseline avg=', baseline_avg)
        FROM #flagged_cost;
    END
END;
GO

/* =========================================================
   18. MASTER RUN PROCEDURE
   ========================================================= */
CREATE OR ALTER PROCEDURE dq.usp_run_data_quality
(
    @metric_date       DATE = NULL,
    @source_file_name  VARCHAR(260) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @metric_date IS NULL
        SET @metric_date = CAST(GETDATE() AS DATE);

    DECLARE @dq_run_id BIGINT;

    INSERT INTO dq.dq_run (run_date, source_file_name, status)
    VALUES (@metric_date, @source_file_name, 'STARTED');

    SET @dq_run_id = SCOPE_IDENTITY();

    BEGIN TRY
        EXEC dq.usp_capture_daily_metrics
            @metric_date = @metric_date,
            @source_file_name = @source_file_name;

        EXEC dq.usp_check_daily_volume_anomaly
            @dq_run_id = @dq_run_id,
            @metric_date = @metric_date;

        EXEC dq.usp_check_completeness
            @dq_run_id = @dq_run_id,
            @metric_date = @metric_date;

        EXEC dq.usp_check_duplicate_business_keys
            @dq_run_id = @dq_run_id,
            @metric_date = @metric_date;

        EXEC dq.usp_check_referential_integrity
            @dq_run_id = @dq_run_id,
            @metric_date = @metric_date;

        EXEC dq.usp_check_distribution_drift
            @dq_run_id = @dq_run_id,
            @metric_date = @metric_date;

        EXEC dq.usp_check_cost_outliers
            @dq_run_id = @dq_run_id,
            @metric_date = @metric_date;

        UPDATE dq.dq_run
        SET
            status = 'SUCCESS',
            checks_run = (SELECT COUNT(*) FROM dq.dq_result WHERE dq_run_id = @dq_run_id),
            checks_failed = (SELECT COUNT(*) FROM dq.dq_result WHERE dq_run_id = @dq_run_id AND status = 'FAIL'),
            checks_warned = (SELECT COUNT(*) FROM dq.dq_result WHERE dq_run_id = @dq_run_id AND status = 'WARN')
        WHERE dq_run_id = @dq_run_id;
    END TRY
    BEGIN CATCH
        UPDATE dq.dq_run
        SET
            status = 'FAILED',
            error_message = ERROR_MESSAGE()
        WHERE dq_run_id = @dq_run_id;

        THROW;
    END CATCH
END;
GO

/* =========================================================
   19. USEFUL CHECK QUERIES
   ========================================================= */

-- Run for a specific date
-- EXEC dq.usp_run_data_quality @metric_date = '2026-03-30';

-- Summary of runs
-- SELECT * FROM dq.dq_run ORDER BY dq_run_id DESC;

-- Rule results
-- SELECT * FROM dq.dq_result ORDER BY dq_result_id DESC;

-- Detailed issues
-- SELECT * FROM dq.dq_issue_detail ORDER BY dq_issue_detail_id DESC;

-- Daily metric history
-- SELECT * FROM dq.daily_metric_snapshot ORDER BY metric_date DESC;
