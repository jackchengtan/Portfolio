/*
    Project: Enhanced Healthcare Reporting & Insights Layer
    Database: PortfolioHealthcareETL
    Purpose:
        Build a richer reporting / BI layer on top of:
          - ETL curated data
          - Data quality outputs
        This script creates:
          - reporting schema
          - richer snapshot tables
          - executive and operational reporting views
          - trend / ranking / variance reporting
          - DQ-aware scorecard outputs
          - refresh procedures for reporting marts
*/

USE master;
GO

IF DB_ID('PortfolioHealthcareETL') IS NULL
BEGIN
    RAISERROR('Database PortfolioHealthcareETL does not exist. Run the ETL and DQ setup first.', 16, 1);
END
GO

USE PortfolioHealthcareETL;
GO

/* =========================================================
   1. PRE-CHECKS
   ========================================================= */
IF OBJECT_ID('dbo.patient_encounter', 'U') IS NULL
BEGIN
    RAISERROR('Required table dbo.patient_encounter was not found. Run the ETL project setup first.', 16, 1);
    RETURN;
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'rpt')
    EXEC('CREATE SCHEMA rpt');
GO

/* =========================================================
   2. DROP OBJECTS IF RE-RUNNING
   ========================================================= */
IF OBJECT_ID('rpt.usp_refresh_reporting_layer', 'P') IS NOT NULL DROP PROCEDURE rpt.usp_refresh_reporting_layer;
GO
IF OBJECT_ID('rpt.usp_refresh_monthly_snapshots', 'P') IS NOT NULL DROP PROCEDURE rpt.usp_refresh_monthly_snapshots;
GO
IF OBJECT_ID('rpt.usp_refresh_daily_snapshots', 'P') IS NOT NULL DROP PROCEDURE rpt.usp_refresh_daily_snapshots;
GO
IF OBJECT_ID('rpt.vw_reporting_scorecard', 'V') IS NOT NULL DROP VIEW rpt.vw_reporting_scorecard;
GO
IF OBJECT_ID('rpt.vw_dq_reporting_summary', 'V') IS NOT NULL DROP VIEW rpt.vw_dq_reporting_summary;
GO
IF OBJECT_ID('rpt.vw_clinician_productivity_trend', 'V') IS NOT NULL DROP VIEW rpt.vw_clinician_productivity_trend;
GO
IF OBJECT_ID('rpt.vw_diagnosis_trend', 'V') IS NOT NULL DROP VIEW rpt.vw_diagnosis_trend;
GO
IF OBJECT_ID('rpt.vw_monthly_practice_ranking', 'V') IS NOT NULL DROP VIEW rpt.vw_monthly_practice_ranking;
GO
IF OBJECT_ID('rpt.vw_monthly_executive_trend', 'V') IS NOT NULL DROP VIEW rpt.vw_monthly_executive_trend;
GO
IF OBJECT_ID('rpt.vw_daily_executive_dashboard', 'V') IS NOT NULL DROP VIEW rpt.vw_daily_executive_dashboard;
GO
IF OBJECT_ID('rpt.vw_encounter_type_mix_trend', 'V') IS NOT NULL DROP VIEW rpt.vw_encounter_type_mix_trend;
GO
IF OBJECT_ID('rpt.vw_practice_daily_variance', 'V') IS NOT NULL DROP VIEW rpt.vw_practice_daily_variance;
GO
IF OBJECT_ID('rpt.vw_daily_activity_enhanced', 'V') IS NOT NULL DROP VIEW rpt.vw_daily_activity_enhanced;
GO

IF OBJECT_ID('rpt.monthly_encounter_type_snapshot', 'U') IS NOT NULL DROP TABLE rpt.monthly_encounter_type_snapshot;
GO
IF OBJECT_ID('rpt.monthly_practice_snapshot', 'U') IS NOT NULL DROP TABLE rpt.monthly_practice_snapshot;
GO
IF OBJECT_ID('rpt.daily_diagnosis_snapshot', 'U') IS NOT NULL DROP TABLE rpt.daily_diagnosis_snapshot;
GO
IF OBJECT_ID('rpt.daily_clinician_snapshot', 'U') IS NOT NULL DROP TABLE rpt.daily_clinician_snapshot;
GO
IF OBJECT_ID('rpt.daily_encounter_type_snapshot', 'U') IS NOT NULL DROP TABLE rpt.daily_encounter_type_snapshot;
GO
IF OBJECT_ID('rpt.daily_practice_snapshot', 'U') IS NOT NULL DROP TABLE rpt.daily_practice_snapshot;
GO
IF OBJECT_ID('rpt.daily_kpi_snapshot', 'U') IS NOT NULL DROP TABLE rpt.daily_kpi_snapshot;
GO

/* =========================================================
   3. SNAPSHOT TABLES
   ========================================================= */

CREATE TABLE rpt.daily_kpi_snapshot
(
    snapshot_date              DATE           NOT NULL PRIMARY KEY,
    total_encounters           INT            NOT NULL,
    unique_patients            INT            NOT NULL,
    unique_practices           INT            NOT NULL,
    unique_clinicians          INT            NOT NULL,
    total_cost                 DECIMAL(18,2)  NOT NULL,
    average_cost               DECIMAL(18,2)  NOT NULL,
    min_cost                   DECIMAL(18,2)  NOT NULL,
    max_cost                   DECIMAL(18,2)  NOT NULL,
    average_cost_per_patient   DECIMAL(18,2)  NULL,
    average_cost_per_practice  DECIMAL(18,2)  NULL,
    diagnosis_null_count       INT            NOT NULL,
    diagnosis_null_rate        DECIMAL(18,4)  NOT NULL,
    rolling_7d_avg_encounters  DECIMAL(18,2)  NULL,
    rolling_30d_avg_encounters DECIMAL(18,2)  NULL,
    created_datetime           DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME()
);
GO

CREATE TABLE rpt.daily_practice_snapshot
(
    snapshot_date              DATE           NOT NULL,
    practice_id                INT            NOT NULL,
    total_encounters           INT            NOT NULL,
    unique_patients            INT            NOT NULL,
    unique_clinicians          INT            NOT NULL,
    total_cost                 DECIMAL(18,2)  NOT NULL,
    average_cost               DECIMAL(18,2)  NOT NULL,
    cost_per_patient           DECIMAL(18,2)  NULL,
    encounter_share_pct        DECIMAL(18,4)  NULL,
    practice_rank_by_volume    INT            NULL,
    practice_rank_by_cost      INT            NULL,
    created_datetime           DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_rpt_daily_practice PRIMARY KEY (snapshot_date, practice_id)
);
GO

CREATE TABLE rpt.daily_encounter_type_snapshot
(
    snapshot_date              DATE           NOT NULL,
    encounter_type_code        VARCHAR(20)    NOT NULL,
    total_encounters           INT            NOT NULL,
    unique_patients            INT            NOT NULL,
    total_cost                 DECIMAL(18,2)  NOT NULL,
    average_cost               DECIMAL(18,2)  NOT NULL,
    encounter_mix_pct          DECIMAL(18,4)  NULL,
    created_datetime           DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_rpt_daily_encounter_type PRIMARY KEY (snapshot_date, encounter_type_code)
);
GO

CREATE TABLE rpt.daily_clinician_snapshot
(
    snapshot_date              DATE           NOT NULL,
    clinician_id               INT            NOT NULL,
    total_encounters           INT            NOT NULL,
    unique_patients            INT            NOT NULL,
    unique_practices           INT            NOT NULL,
    total_cost                 DECIMAL(18,2)  NOT NULL,
    average_cost               DECIMAL(18,2)  NOT NULL,
    clinician_rank_by_volume   INT            NULL,
    created_datetime           DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_rpt_daily_clinician PRIMARY KEY (snapshot_date, clinician_id)
);
GO

CREATE TABLE rpt.daily_diagnosis_snapshot
(
    snapshot_date              DATE           NOT NULL,
    diagnosis_code             VARCHAR(50)    NOT NULL,
    total_encounters           INT            NOT NULL,
    unique_patients            INT            NOT NULL,
    total_cost                 DECIMAL(18,2)  NOT NULL,
    average_cost               DECIMAL(18,2)  NOT NULL,
    diagnosis_rank_by_volume   INT            NULL,
    created_datetime           DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_rpt_daily_diagnosis PRIMARY KEY (snapshot_date, diagnosis_code)
);
GO

CREATE TABLE rpt.monthly_practice_snapshot
(
    month_start_date           DATE           NOT NULL,
    practice_id                INT            NOT NULL,
    total_encounters           INT            NOT NULL,
    unique_patients            INT            NOT NULL,
    unique_clinicians          INT            NOT NULL,
    total_cost                 DECIMAL(18,2)  NOT NULL,
    average_cost               DECIMAL(18,2)  NOT NULL,
    practice_rank_by_volume    INT            NULL,
    practice_rank_by_cost      INT            NULL,
    created_datetime           DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_rpt_monthly_practice PRIMARY KEY (month_start_date, practice_id)
);
GO

CREATE TABLE rpt.monthly_encounter_type_snapshot
(
    month_start_date           DATE           NOT NULL,
    encounter_type_code        VARCHAR(20)    NOT NULL,
    total_encounters           INT            NOT NULL,
    unique_patients            INT            NOT NULL,
    total_cost                 DECIMAL(18,2)  NOT NULL,
    average_cost               DECIMAL(18,2)  NOT NULL,
    encounter_mix_pct          DECIMAL(18,4)  NULL,
    created_datetime           DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_rpt_monthly_encounter_type PRIMARY KEY (month_start_date, encounter_type_code)
);
GO

/* =========================================================
   4. ENHANCED DAILY SNAPSHOT REFRESH
   ========================================================= */
CREATE OR ALTER PROCEDURE rpt.usp_refresh_daily_snapshots
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE rpt.daily_kpi_snapshot;
    TRUNCATE TABLE rpt.daily_practice_snapshot;
    TRUNCATE TABLE rpt.daily_encounter_type_snapshot;
    TRUNCATE TABLE rpt.daily_clinician_snapshot;
    TRUNCATE TABLE rpt.daily_diagnosis_snapshot;

    /* Daily KPI snapshot */
    ;WITH base AS
    (
        SELECT
            encounter_date,
            COUNT(*) AS total_encounters,
            COUNT(DISTINCT patient_id) AS unique_patients,
            COUNT(DISTINCT practice_id) AS unique_practices,
            COUNT(DISTINCT clinician_id) AS unique_clinicians,
            CAST(SUM(cost) AS DECIMAL(18,2)) AS total_cost,
            CAST(AVG(cost) AS DECIMAL(18,2)) AS average_cost,
            CAST(MIN(cost) AS DECIMAL(18,2)) AS min_cost,
            CAST(MAX(cost) AS DECIMAL(18,2)) AS max_cost,
            SUM(CASE WHEN diagnosis_code IS NULL OR LTRIM(RTRIM(diagnosis_code)) = '' THEN 1 ELSE 0 END) AS diagnosis_null_count
        FROM dbo.patient_encounter
        GROUP BY encounter_date
    ),
    enriched AS
    (
        SELECT
            encounter_date AS snapshot_date,
            total_encounters,
            unique_patients,
            unique_practices,
            unique_clinicians,
            total_cost,
            average_cost,
            min_cost,
            max_cost,
            CAST(total_cost / NULLIF(unique_patients, 0) AS DECIMAL(18,2)) AS average_cost_per_patient,
            CAST(total_cost / NULLIF(unique_practices, 0) AS DECIMAL(18,2)) AS average_cost_per_practice,
            diagnosis_null_count,
            CAST(diagnosis_null_count * 1.0 / NULLIF(total_encounters, 0) AS DECIMAL(18,4)) AS diagnosis_null_rate,
            CAST(AVG(total_encounters * 1.0) OVER (
                ORDER BY encounter_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) AS DECIMAL(18,2)) AS rolling_7d_avg_encounters,
            CAST(AVG(total_encounters * 1.0) OVER (
                ORDER BY encounter_date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) AS DECIMAL(18,2)) AS rolling_30d_avg_encounters
        FROM base
    )
    INSERT INTO rpt.daily_kpi_snapshot
    (
        snapshot_date, total_encounters, unique_patients, unique_practices, unique_clinicians,
        total_cost, average_cost, min_cost, max_cost, average_cost_per_patient,
        average_cost_per_practice, diagnosis_null_count, diagnosis_null_rate,
        rolling_7d_avg_encounters, rolling_30d_avg_encounters
    )
    SELECT
        snapshot_date, total_encounters, unique_patients, unique_practices, unique_clinicians,
        total_cost, average_cost, min_cost, max_cost, average_cost_per_patient,
        average_cost_per_practice, diagnosis_null_count, diagnosis_null_rate,
        rolling_7d_avg_encounters, rolling_30d_avg_encounters
    FROM enriched;

    /* Daily practice snapshot */
    ;WITH base AS
    (
        SELECT
            encounter_date AS snapshot_date,
            practice_id,
            COUNT(*) AS total_encounters,
            COUNT(DISTINCT patient_id) AS unique_patients,
            COUNT(DISTINCT clinician_id) AS unique_clinicians,
            CAST(SUM(cost) AS DECIMAL(18,2)) AS total_cost,
            CAST(AVG(cost) AS DECIMAL(18,2)) AS average_cost
        FROM dbo.patient_encounter
        GROUP BY encounter_date, practice_id
    ),
    totals AS
    (
        SELECT snapshot_date, SUM(total_encounters) AS day_total
        FROM base
        GROUP BY snapshot_date
    )
    INSERT INTO rpt.daily_practice_snapshot
    (
        snapshot_date, practice_id, total_encounters, unique_patients, unique_clinicians,
        total_cost, average_cost, cost_per_patient, encounter_share_pct,
        practice_rank_by_volume, practice_rank_by_cost
    )
    SELECT
        b.snapshot_date,
        b.practice_id,
        b.total_encounters,
        b.unique_patients,
        b.unique_clinicians,
        b.total_cost,
        b.average_cost,
        CAST(b.total_cost / NULLIF(b.unique_patients,0) AS DECIMAL(18,2)) AS cost_per_patient,
        CAST(b.total_encounters * 1.0 / NULLIF(t.day_total,0) AS DECIMAL(18,4)) AS encounter_share_pct,
        DENSE_RANK() OVER (PARTITION BY b.snapshot_date ORDER BY b.total_encounters DESC) AS practice_rank_by_volume,
        DENSE_RANK() OVER (PARTITION BY b.snapshot_date ORDER BY b.total_cost DESC) AS practice_rank_by_cost
    FROM base b
    INNER JOIN totals t
        ON b.snapshot_date = t.snapshot_date;

    /* Daily encounter type snapshot */
    ;WITH base AS
    (
        SELECT
            encounter_date AS snapshot_date,
            encounter_type_code,
            COUNT(*) AS total_encounters,
            COUNT(DISTINCT patient_id) AS unique_patients,
            CAST(SUM(cost) AS DECIMAL(18,2)) AS total_cost,
            CAST(AVG(cost) AS DECIMAL(18,2)) AS average_cost
        FROM dbo.patient_encounter
        GROUP BY encounter_date, encounter_type_code
    ),
    totals AS
    (
        SELECT snapshot_date, SUM(total_encounters) AS day_total
        FROM base
        GROUP BY snapshot_date
    )
    INSERT INTO rpt.daily_encounter_type_snapshot
    (
        snapshot_date, encounter_type_code, total_encounters, unique_patients,
        total_cost, average_cost, encounter_mix_pct
    )
    SELECT
        b.snapshot_date,
        b.encounter_type_code,
        b.total_encounters,
        b.unique_patients,
        b.total_cost,
        b.average_cost,
        CAST(b.total_encounters * 1.0 / NULLIF(t.day_total,0) AS DECIMAL(18,4)) AS encounter_mix_pct
    FROM base b
    INNER JOIN totals t
        ON b.snapshot_date = t.snapshot_date;

    /* Daily clinician snapshot */
    INSERT INTO rpt.daily_clinician_snapshot
    (
        snapshot_date, clinician_id, total_encounters, unique_patients,
        unique_practices, total_cost, average_cost, clinician_rank_by_volume
    )
    SELECT
        encounter_date AS snapshot_date,
        clinician_id,
        COUNT(*) AS total_encounters,
        COUNT(DISTINCT patient_id) AS unique_patients,
        COUNT(DISTINCT practice_id) AS unique_practices,
        CAST(SUM(cost) AS DECIMAL(18,2)) AS total_cost,
        CAST(AVG(cost) AS DECIMAL(18,2)) AS average_cost,
        DENSE_RANK() OVER (
            PARTITION BY encounter_date
            ORDER BY COUNT(*) DESC
        ) AS clinician_rank_by_volume
    FROM dbo.patient_encounter
    GROUP BY encounter_date, clinician_id;

    /* Daily diagnosis snapshot */
    INSERT INTO rpt.daily_diagnosis_snapshot
    (
        snapshot_date, diagnosis_code, total_encounters,
        unique_patients, total_cost, average_cost, diagnosis_rank_by_volume
    )
    SELECT
        encounter_date AS snapshot_date,
        ISNULL(NULLIF(LTRIM(RTRIM(diagnosis_code)), ''), 'UNKNOWN') AS diagnosis_code,
        COUNT(*) AS total_encounters,
        COUNT(DISTINCT patient_id) AS unique_patients,
        CAST(SUM(cost) AS DECIMAL(18,2)) AS total_cost,
        CAST(AVG(cost) AS DECIMAL(18,2)) AS average_cost,
        DENSE_RANK() OVER (
            PARTITION BY encounter_date
            ORDER BY COUNT(*) DESC
        ) AS diagnosis_rank_by_volume
    FROM dbo.patient_encounter
    GROUP BY encounter_date, ISNULL(NULLIF(LTRIM(RTRIM(diagnosis_code)), ''), 'UNKNOWN');
END;
GO

/* =========================================================
   5. MONTHLY SNAPSHOT REFRESH
   ========================================================= */
CREATE OR ALTER PROCEDURE rpt.usp_refresh_monthly_snapshots
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE rpt.monthly_practice_snapshot;
    TRUNCATE TABLE rpt.monthly_encounter_type_snapshot;

    ;WITH base AS
    (
        SELECT
            DATEFROMPARTS(YEAR(encounter_date), MONTH(encounter_date), 1) AS month_start_date,
            practice_id,
            COUNT(*) AS total_encounters,
            COUNT(DISTINCT patient_id) AS unique_patients,
            COUNT(DISTINCT clinician_id) AS unique_clinicians,
            CAST(SUM(cost) AS DECIMAL(18,2)) AS total_cost,
            CAST(AVG(cost) AS DECIMAL(18,2)) AS average_cost
        FROM dbo.patient_encounter
        GROUP BY DATEFROMPARTS(YEAR(encounter_date), MONTH(encounter_date), 1), practice_id
    )
    INSERT INTO rpt.monthly_practice_snapshot
    (
        month_start_date, practice_id, total_encounters, unique_patients,
        unique_clinicians, total_cost, average_cost,
        practice_rank_by_volume, practice_rank_by_cost
    )
    SELECT
        month_start_date,
        practice_id,
        total_encounters,
        unique_patients,
        unique_clinicians,
        total_cost,
        average_cost,
        DENSE_RANK() OVER (PARTITION BY month_start_date ORDER BY total_encounters DESC) AS practice_rank_by_volume,
        DENSE_RANK() OVER (PARTITION BY month_start_date ORDER BY total_cost DESC) AS practice_rank_by_cost
    FROM base;

    ;WITH base AS
    (
        SELECT
            DATEFROMPARTS(YEAR(encounter_date), MONTH(encounter_date), 1) AS month_start_date,
            encounter_type_code,
            COUNT(*) AS total_encounters,
            COUNT(DISTINCT patient_id) AS unique_patients,
            CAST(SUM(cost) AS DECIMAL(18,2)) AS total_cost,
            CAST(AVG(cost) AS DECIMAL(18,2)) AS average_cost
        FROM dbo.patient_encounter
        GROUP BY DATEFROMPARTS(YEAR(encounter_date), MONTH(encounter_date), 1), encounter_type_code
    ),
    totals AS
    (
        SELECT month_start_date, SUM(total_encounters) AS month_total
        FROM base
        GROUP BY month_start_date
    )
    INSERT INTO rpt.monthly_encounter_type_snapshot
    (
        month_start_date, encounter_type_code, total_encounters,
        unique_patients, total_cost, average_cost, encounter_mix_pct
    )
    SELECT
        b.month_start_date,
        b.encounter_type_code,
        b.total_encounters,
        b.unique_patients,
        b.total_cost,
        b.average_cost,
        CAST(b.total_encounters * 1.0 / NULLIF(t.month_total,0) AS DECIMAL(18,4)) AS encounter_mix_pct
    FROM base b
    INNER JOIN totals t
        ON b.month_start_date = t.month_start_date;
END;
GO

/* =========================================================
   6. WRAPPER PROCEDURE
   ========================================================= */
CREATE OR ALTER PROCEDURE rpt.usp_refresh_reporting_layer
AS
BEGIN
    SET NOCOUNT ON;
    EXEC rpt.usp_refresh_daily_snapshots;
    EXEC rpt.usp_refresh_monthly_snapshots;
END;
GO

/* =========================================================
   7. ENHANCED REPORTING VIEWS
   ========================================================= */

CREATE OR ALTER VIEW rpt.vw_daily_activity_enhanced
AS
SELECT
    d.snapshot_date,
    d.total_encounters,
    d.unique_patients,
    d.unique_practices,
    d.unique_clinicians,
    d.total_cost,
    d.average_cost,
    d.min_cost,
    d.max_cost,
    d.average_cost_per_patient,
    d.average_cost_per_practice,
    d.diagnosis_null_count,
    d.diagnosis_null_rate,
    d.rolling_7d_avg_encounters,
    d.rolling_30d_avg_encounters,
    d.total_encounters
        - LAG(d.total_encounters) OVER (ORDER BY d.snapshot_date) AS day_on_day_volume_change,
    CAST(
        (d.total_encounters - LAG(d.total_encounters) OVER (ORDER BY d.snapshot_date)) * 1.0
        / NULLIF(LAG(d.total_encounters) OVER (ORDER BY d.snapshot_date), 0)
        AS DECIMAL(18,4)
    ) AS day_on_day_volume_pct_change,
    d.total_cost
        - LAG(d.total_cost) OVER (ORDER BY d.snapshot_date) AS day_on_day_cost_change
FROM rpt.daily_kpi_snapshot d;
GO

CREATE OR ALTER VIEW rpt.vw_practice_daily_variance
AS
SELECT
    p.snapshot_date,
    p.practice_id,
    p.total_encounters,
    p.unique_patients,
    p.unique_clinicians,
    p.total_cost,
    p.average_cost,
    p.cost_per_patient,
    p.encounter_share_pct,
    p.practice_rank_by_volume,
    p.practice_rank_by_cost,
    p.total_encounters
        - LAG(p.total_encounters) OVER (PARTITION BY p.practice_id ORDER BY p.snapshot_date) AS day_on_day_volume_change,
    CAST(
        (p.total_encounters - LAG(p.total_encounters) OVER (PARTITION BY p.practice_id ORDER BY p.snapshot_date)) * 1.0
        / NULLIF(LAG(p.total_encounters) OVER (PARTITION BY p.practice_id ORDER BY p.snapshot_date), 0)
        AS DECIMAL(18,4)
    ) AS day_on_day_volume_pct_change
FROM rpt.daily_practice_snapshot p;
GO

CREATE OR ALTER VIEW rpt.vw_encounter_type_mix_trend
AS
SELECT
    snapshot_date,
    encounter_type_code,
    total_encounters,
    unique_patients,
    total_cost,
    average_cost,
    encounter_mix_pct,
    CAST(
        encounter_mix_pct - LAG(encounter_mix_pct) OVER (
            PARTITION BY encounter_type_code ORDER BY snapshot_date
        )
        AS DECIMAL(18,4)
    ) AS mix_pct_change_vs_prior_day
FROM rpt.daily_encounter_type_snapshot;
GO

CREATE OR ALTER VIEW rpt.vw_daily_executive_dashboard
AS
SELECT
    d.snapshot_date,
    d.total_encounters,
    d.unique_patients,
    d.unique_practices,
    d.unique_clinicians,
    d.total_cost,
    d.average_cost,
    d.average_cost_per_patient,
    d.diagnosis_null_rate,
    d.rolling_7d_avg_encounters,
    d.rolling_30d_avg_encounters,
    CASE
        WHEN d.total_encounters > d.rolling_7d_avg_encounters THEN 'Above 7D Average'
        WHEN d.total_encounters < d.rolling_7d_avg_encounters THEN 'Below 7D Average'
        ELSE 'At 7D Average'
    END AS volume_vs_7d_flag
FROM rpt.daily_kpi_snapshot d;
GO

CREATE OR ALTER VIEW rpt.vw_monthly_executive_trend
AS
SELECT
    DATEFROMPARTS(YEAR(encounter_date), MONTH(encounter_date), 1) AS month_start_date,
    COUNT(*) AS total_encounters,
    COUNT(DISTINCT patient_id) AS unique_patients,
    COUNT(DISTINCT practice_id) AS unique_practices,
    COUNT(DISTINCT clinician_id) AS unique_clinicians,
    CAST(SUM(cost) AS DECIMAL(18,2)) AS total_cost,
    CAST(AVG(cost) AS DECIMAL(18,2)) AS average_cost,
    CAST(SUM(cost) / NULLIF(COUNT(DISTINCT patient_id),0) AS DECIMAL(18,2)) AS cost_per_patient
FROM dbo.patient_encounter
GROUP BY DATEFROMPARTS(YEAR(encounter_date), MONTH(encounter_date), 1);
GO

CREATE OR ALTER VIEW rpt.vw_monthly_practice_ranking
AS
SELECT
    month_start_date,
    practice_id,
    total_encounters,
    unique_patients,
    unique_clinicians,
    total_cost,
    average_cost,
    practice_rank_by_volume,
    practice_rank_by_cost
FROM rpt.monthly_practice_snapshot;
GO

CREATE OR ALTER VIEW rpt.vw_diagnosis_trend
AS
SELECT
    snapshot_date,
    diagnosis_code,
    total_encounters,
    unique_patients,
    total_cost,
    average_cost,
    diagnosis_rank_by_volume
FROM rpt.daily_diagnosis_snapshot;
GO

CREATE OR ALTER VIEW rpt.vw_clinician_productivity_trend
AS
SELECT
    snapshot_date,
    clinician_id,
    total_encounters,
    unique_patients,
    unique_practices,
    total_cost,
    average_cost,
    clinician_rank_by_volume
FROM rpt.daily_clinician_snapshot;
GO

/* =========================================================
   8. DQ-AWARE REPORTING
   Requires dq schema from DQ project
   ========================================================= */
CREATE OR ALTER VIEW rpt.vw_dq_reporting_summary
AS
SELECT
    r.run_date,
    r.status AS dq_run_status,
    r.checks_run,
    r.checks_failed,
    r.checks_warned,
    res.rule_category,
    res.rule_code,
    res.rule_name,
    res.severity,
    res.status AS rule_status,
    res.metric_value,
    res.baseline_value,
    res.threshold_min,
    res.threshold_max,
    res.rows_affected,
    res.message_text
FROM dq.dq_run r
LEFT JOIN dq.dq_result res
    ON r.dq_run_id = res.dq_run_id;
GO

CREATE OR ALTER VIEW rpt.vw_reporting_scorecard
AS
SELECT
    d.snapshot_date,
    d.total_encounters,
    d.unique_patients,
    d.unique_practices,
    d.total_cost,
    d.average_cost,
    d.average_cost_per_patient,
    d.diagnosis_null_rate,
    dq.dq_run_status,
    dq.checks_run,
    dq.checks_failed,
    dq.checks_warned
FROM rpt.daily_kpi_snapshot d
LEFT JOIN
(
    SELECT
        run_date,
        MAX(status) AS dq_run_status,
        MAX(checks_run) AS checks_run,
        MAX(checks_failed) AS checks_failed,
        MAX(checks_warned) AS checks_warned
    FROM dq.dq_run
    GROUP BY run_date
) dq
    ON d.snapshot_date = dq.run_date;
GO

/* =========================================================
   9. EXAMPLE VALIDATION / TEST QUERIES
   ========================================================= */

-- EXEC rpt.usp_refresh_reporting_layer;

-- SELECT * FROM rpt.vw_daily_activity_enhanced ORDER BY snapshot_date;
-- SELECT * FROM rpt.vw_practice_daily_variance ORDER BY snapshot_date, practice_id;
-- SELECT * FROM rpt.vw_encounter_type_mix_trend ORDER BY snapshot_date, encounter_type_code;
-- SELECT * FROM rpt.vw_daily_executive_dashboard ORDER BY snapshot_date;
-- SELECT * FROM rpt.vw_monthly_executive_trend ORDER BY month_start_date;
-- SELECT * FROM rpt.vw_monthly_practice_ranking ORDER BY month_start_date, practice_rank_by_volume;
-- SELECT * FROM rpt.vw_diagnosis_trend ORDER BY snapshot_date, diagnosis_rank_by_volume;
-- SELECT * FROM rpt.vw_clinician_productivity_trend ORDER BY snapshot_date, clinician_rank_by_volume;
-- SELECT * FROM rpt.vw_dq_reporting_summary ORDER BY run_date DESC;
-- SELECT * FROM rpt.vw_reporting_scorecard ORDER BY snapshot_date DESC;
