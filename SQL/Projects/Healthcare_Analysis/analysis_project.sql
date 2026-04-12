/* =========================================================
   FULL HEALTHCARE ANALYSIS PROJECT (DEMAND, STRAIN & UTILISATION)
   Platform: SQL Server
   Database: PortfolioHealthcareETL
   Purpose:
       Build an analytical layer on top of curated healthcare
       encounter data for demand, strain, utilisation, and
       operational insight generation.
   ========================================================= */

USE master;
GO

IF DB_ID('PortfolioHealthcareETL') IS NULL
BEGIN
    RAISERROR('Database PortfolioHealthcareETL does not exist. Run the ETL project first.', 16, 1);
END
GO

USE PortfolioHealthcareETL;
GO

/* =========================================================
   1. PRE-CHECKS
   ========================================================= */
IF OBJECT_ID('dbo.patient_encounter', 'U') IS NULL
BEGIN
    RAISERROR('Required table dbo.patient_encounter was not found. Run the ETL project first.', 16, 1);
    RETURN;
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ana')
    EXEC('CREATE SCHEMA ana');
GO

/* =========================================================
   2. DROP OBJECTS IF RE-RUNNING
   ========================================================= */
IF OBJECT_ID('ana.usp_refresh_analysis_layer', 'P') IS NOT NULL DROP PROCEDURE ana.usp_refresh_analysis_layer;
GO
IF OBJECT_ID('ana.usp_build_encounter_type_mix', 'P') IS NOT NULL DROP PROCEDURE ana.usp_build_encounter_type_mix;
GO
IF OBJECT_ID('ana.usp_build_diagnosis_demand', 'P') IS NOT NULL DROP PROCEDURE ana.usp_build_diagnosis_demand;
GO
IF OBJECT_ID('ana.usp_build_clinician_workload', 'P') IS NOT NULL DROP PROCEDURE ana.usp_build_clinician_workload;
GO
IF OBJECT_ID('ana.usp_build_practice_capacity', 'P') IS NOT NULL DROP PROCEDURE ana.usp_build_practice_capacity;
GO
IF OBJECT_ID('ana.usp_build_daily_demand_metrics', 'P') IS NOT NULL DROP PROCEDURE ana.usp_build_daily_demand_metrics;
GO

IF OBJECT_ID('ana.vw_demand_dashboard', 'V') IS NOT NULL DROP VIEW ana.vw_demand_dashboard;
GO
IF OBJECT_ID('ana.vw_practice_strain_ranking', 'V') IS NOT NULL DROP VIEW ana.vw_practice_strain_ranking;
GO
IF OBJECT_ID('ana.vw_clinician_strain_ranking', 'V') IS NOT NULL DROP VIEW ana.vw_clinician_strain_ranking;
GO
IF OBJECT_ID('ana.vw_diagnosis_pressure', 'V') IS NOT NULL DROP VIEW ana.vw_diagnosis_pressure;
GO
IF OBJECT_ID('ana.vw_encounter_type_mix_trend', 'V') IS NOT NULL DROP VIEW ana.vw_encounter_type_mix_trend;
GO
IF OBJECT_ID('ana.vw_weekday_demand_pattern', 'V') IS NOT NULL DROP VIEW ana.vw_weekday_demand_pattern;
GO
IF OBJECT_ID('ana.vw_monthly_demand_pattern', 'V') IS NOT NULL DROP VIEW ana.vw_monthly_demand_pattern;
GO
IF OBJECT_ID('ana.vw_encounter_enriched', 'V') IS NOT NULL DROP VIEW ana.vw_encounter_enriched;
GO

IF OBJECT_ID('ana.encounter_type_mix_analysis', 'U') IS NOT NULL DROP TABLE ana.encounter_type_mix_analysis;
GO
IF OBJECT_ID('ana.diagnosis_demand_analysis', 'U') IS NOT NULL DROP TABLE ana.diagnosis_demand_analysis;
GO
IF OBJECT_ID('ana.clinician_workload_analysis', 'U') IS NOT NULL DROP TABLE ana.clinician_workload_analysis;
GO
IF OBJECT_ID('ana.practice_capacity_analysis', 'U') IS NOT NULL DROP TABLE ana.practice_capacity_analysis;
GO
IF OBJECT_ID('ana.daily_demand_metrics', 'U') IS NOT NULL DROP TABLE ana.daily_demand_metrics;
GO

/* =========================================================
   3. ENRICHED ENCOUNTER VIEW
   ========================================================= */
CREATE OR ALTER VIEW ana.vw_encounter_enriched
AS
SELECT
    pe.encounter_id,
    pe.patient_id,
    pe.practice_id,
    pe.encounter_date,
    DATENAME(WEEKDAY, pe.encounter_date) AS weekday_name,
    DATEPART(WEEKDAY, pe.encounter_date) AS weekday_no,
    DATEPART(WEEK, pe.encounter_date) AS week_no,
    DATEPART(MONTH, pe.encounter_date) AS month_no,
    DATEPART(YEAR, pe.encounter_date) AS year_no,
    DATEFROMPARTS(YEAR(pe.encounter_date), MONTH(pe.encounter_date), 1) AS month_start_date,
    CASE
        WHEN DATEPART(WEEKDAY, pe.encounter_date) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    pe.encounter_type_code,
    pe.clinician_id,
    ISNULL(NULLIF(LTRIM(RTRIM(pe.diagnosis_code)), ''), 'UNKNOWN') AS diagnosis_code,
    CAST(pe.cost AS DECIMAL(18,2)) AS cost,
    CASE
        WHEN pe.cost >= 80 THEN 'High Cost'
        WHEN pe.cost >= 40 THEN 'Medium Cost'
        ELSE 'Low Cost'
    END AS cost_band
FROM dbo.patient_encounter pe;
GO

/* =========================================================
   4. DAILY DEMAND & STRAIN METRICS
   ========================================================= */
CREATE TABLE ana.daily_demand_metrics
(
    metric_date                 DATE PRIMARY KEY,
    total_encounters            INT NOT NULL,
    unique_patients             INT NOT NULL,
    unique_practices            INT NOT NULL,
    unique_clinicians           INT NOT NULL,
    total_cost                  DECIMAL(18,2) NOT NULL,
    average_cost                DECIMAL(18,2) NOT NULL,
    cost_per_patient            DECIMAL(18,2) NULL,
    rolling_7d_avg              DECIMAL(18,2) NULL,
    rolling_7d_std              DECIMAL(18,2) NULL,
    rolling_30d_avg             DECIMAL(18,2) NULL,
    prior_day_encounters        INT NULL,
    demand_delta                INT NULL,
    demand_pct_change           DECIMAL(18,4) NULL,
    demand_status               VARCHAR(50) NOT NULL,
    demand_strain_score         DECIMAL(18,4) NULL,
    created_datetime            DATETIME2(0) NOT NULL DEFAULT SYSDATETIME()
);
GO

/* =========================================================
   5. PRACTICE CAPACITY & STRAIN
   ========================================================= */
CREATE TABLE ana.practice_capacity_analysis
(
    practice_id                 INT NOT NULL PRIMARY KEY,
    total_encounters            INT NOT NULL,
    unique_patients             INT NOT NULL,
    unique_clinicians           INT NOT NULL,
    active_days                 INT NOT NULL,
    avg_daily_load              DECIMAL(18,2) NOT NULL,
    avg_daily_cost              DECIMAL(18,2) NOT NULL,
    avg_cost_per_encounter      DECIMAL(18,2) NOT NULL,
    avg_cost_per_patient        DECIMAL(18,2) NULL,
    peak_daily_load             INT NOT NULL,
    low_daily_load              INT NOT NULL,
    load_variability_std        DECIMAL(18,2) NULL,
    estimated_capacity_band     VARCHAR(50) NOT NULL,
    strain_risk_level           VARCHAR(50) NOT NULL,
    created_datetime            DATETIME2(0) NOT NULL DEFAULT SYSDATETIME()
);
GO

/* =========================================================
   6. CLINICIAN WORKLOAD ANALYSIS
   ========================================================= */
CREATE TABLE ana.clinician_workload_analysis
(
    clinician_id                INT NOT NULL PRIMARY KEY,
    total_encounters            INT NOT NULL,
    unique_patients             INT NOT NULL,
    unique_practices            INT NOT NULL,
    active_days                 INT NOT NULL,
    avg_daily_workload          DECIMAL(18,2) NOT NULL,
    avg_cost_per_encounter      DECIMAL(18,2) NOT NULL,
    peak_daily_workload         INT NOT NULL,
    workload_variability_std    DECIMAL(18,2) NULL,
    workload_risk_level         VARCHAR(50) NOT NULL,
    created_datetime            DATETIME2(0) NOT NULL DEFAULT SYSDATETIME()
);
GO

/* =========================================================
   7. DIAGNOSIS DEMAND ANALYSIS
   ========================================================= */
CREATE TABLE ana.diagnosis_demand_analysis
(
    diagnosis_code              VARCHAR(50) NOT NULL PRIMARY KEY,
    total_encounters            INT NOT NULL,
    unique_patients             INT NOT NULL,
    total_cost                  DECIMAL(18,2) NOT NULL,
    average_cost                DECIMAL(18,2) NOT NULL,
    active_days                 INT NOT NULL,
    avg_daily_demand            DECIMAL(18,2) NOT NULL,
    peak_daily_demand           INT NOT NULL,
    diagnosis_pressure_level    VARCHAR(50) NOT NULL,
    created_datetime            DATETIME2(0) NOT NULL DEFAULT SYSDATETIME()
);
GO

/* =========================================================
   8. ENCOUNTER TYPE MIX ANALYSIS
   ========================================================= */
CREATE TABLE ana.encounter_type_mix_analysis
(
    encounter_type_code         VARCHAR(20) NOT NULL PRIMARY KEY,
    total_encounters            INT NOT NULL,
    unique_patients             INT NOT NULL,
    total_cost                  DECIMAL(18,2) NOT NULL,
    average_cost                DECIMAL(18,2) NOT NULL,
    encounter_mix_pct           DECIMAL(18,4) NOT NULL,
    avg_daily_demand            DECIMAL(18,2) NOT NULL,
    peak_daily_demand           INT NOT NULL,
    mix_pressure_level          VARCHAR(50) NOT NULL,
    created_datetime            DATETIME2(0) NOT NULL DEFAULT SYSDATETIME()
);
GO

/* =========================================================
   9. BUILD DAILY DEMAND METRICS
   ========================================================= */
CREATE OR ALTER PROCEDURE ana.usp_build_daily_demand_metrics
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE ana.daily_demand_metrics;

    ;WITH daily AS
    (
        SELECT
            encounter_date AS metric_date,
            COUNT(*) AS total_encounters,
            COUNT(DISTINCT patient_id) AS unique_patients,
            COUNT(DISTINCT practice_id) AS unique_practices,
            COUNT(DISTINCT clinician_id) AS unique_clinicians,
            CAST(SUM(cost) AS DECIMAL(18,2)) AS total_cost,
            CAST(AVG(cost) AS DECIMAL(18,2)) AS average_cost
        FROM dbo.patient_encounter
        GROUP BY encounter_date
    ),
    calc AS
    (
        SELECT
            metric_date,
            total_encounters,
            unique_patients,
            unique_practices,
            unique_clinicians,
            total_cost,
            average_cost,
            CAST(total_cost / NULLIF(unique_patients, 0) AS DECIMAL(18,2)) AS cost_per_patient,
            CAST(AVG(total_encounters * 1.0) OVER (
                ORDER BY metric_date
                ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
            ) AS DECIMAL(18,2)) AS rolling_7d_avg,
            CAST(STDEV(total_encounters * 1.0) OVER (
                ORDER BY metric_date
                ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
            ) AS DECIMAL(18,2)) AS rolling_7d_std,
            CAST(AVG(total_encounters * 1.0) OVER (
                ORDER BY metric_date
                ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
            ) AS DECIMAL(18,2)) AS rolling_30d_avg,
            LAG(total_encounters) OVER (ORDER BY metric_date) AS prior_day_encounters
        FROM daily
    )
    INSERT INTO ana.daily_demand_metrics
    (
        metric_date,
        total_encounters,
        unique_patients,
        unique_practices,
        unique_clinicians,
        total_cost,
        average_cost,
        cost_per_patient,
        rolling_7d_avg,
        rolling_7d_std,
        rolling_30d_avg,
        prior_day_encounters,
        demand_delta,
        demand_pct_change,
        demand_status,
        demand_strain_score
    )
    SELECT
        metric_date,
        total_encounters,
        unique_patients,
        unique_practices,
        unique_clinicians,
        total_cost,
        average_cost,
        cost_per_patient,
        rolling_7d_avg,
        rolling_7d_std,
        rolling_30d_avg,
        prior_day_encounters,
        total_encounters - prior_day_encounters AS demand_delta,
        CAST(
            (total_encounters - prior_day_encounters) * 1.0 / NULLIF(prior_day_encounters, 0)
            AS DECIMAL(18,4)
        ) AS demand_pct_change,
        CASE
            WHEN rolling_7d_avg IS NULL THEN 'INSUFFICIENT_HISTORY'
            WHEN total_encounters > rolling_7d_avg + 2 * ISNULL(rolling_7d_std, 0) THEN 'OVERLOAD'
            WHEN total_encounters > rolling_7d_avg + 1 * ISNULL(rolling_7d_std, 0) THEN 'HIGH_PRESSURE'
            WHEN total_encounters < rolling_7d_avg - 2 * ISNULL(rolling_7d_std, 0) THEN 'UNDERUTILISED'
            ELSE 'NORMAL'
        END AS demand_status,
        CAST(
            (total_encounters - ISNULL(rolling_7d_avg, total_encounters)) * 1.0
            / NULLIF(ISNULL(rolling_7d_avg, total_encounters), 0)
            AS DECIMAL(18,4)
        ) AS demand_strain_score
    FROM calc;
END;
GO

/* =========================================================
   10. BUILD PRACTICE CAPACITY
   ========================================================= */
CREATE OR ALTER PROCEDURE ana.usp_build_practice_capacity
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE ana.practice_capacity_analysis;

    ;WITH daily AS
    (
        SELECT
            practice_id,
            encounter_date,
            COUNT(*) AS daily_load,
            CAST(SUM(cost) AS DECIMAL(18,2)) AS daily_cost
        FROM dbo.patient_encounter
        GROUP BY practice_id, encounter_date
    ),
    agg AS
    (
        SELECT
            pe.practice_id,
            COUNT(*) AS total_encounters,
            COUNT(DISTINCT pe.patient_id) AS unique_patients,
            COUNT(DISTINCT pe.clinician_id) AS unique_clinicians,
            COUNT(DISTINCT pe.encounter_date) AS active_days,
            CAST(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT pe.encounter_date), 0) AS DECIMAL(18,2)) AS avg_daily_load,
            CAST(SUM(pe.cost) * 1.0 / NULLIF(COUNT(DISTINCT pe.encounter_date), 0) AS DECIMAL(18,2)) AS avg_daily_cost,
            CAST(AVG(pe.cost) AS DECIMAL(18,2)) AS avg_cost_per_encounter,
            CAST(SUM(pe.cost) * 1.0 / NULLIF(COUNT(DISTINCT pe.patient_id), 0) AS DECIMAL(18,2)) AS avg_cost_per_patient
        FROM dbo.patient_encounter pe
        GROUP BY pe.practice_id
    ),
    peaks AS
    (
        SELECT
            practice_id,
            MAX(daily_load) AS peak_daily_load,
            MIN(daily_load) AS low_daily_load,
            CAST(STDEV(daily_load * 1.0) AS DECIMAL(18,2)) AS load_variability_std
        FROM daily
        GROUP BY practice_id
    )
    INSERT INTO ana.practice_capacity_analysis
    (
        practice_id,
        total_encounters,
        unique_patients,
        unique_clinicians,
        active_days,
        avg_daily_load,
        avg_daily_cost,
        avg_cost_per_encounter,
        avg_cost_per_patient,
        peak_daily_load,
        low_daily_load,
        load_variability_std,
        estimated_capacity_band,
        strain_risk_level
    )
    SELECT
        a.practice_id,
        a.total_encounters,
        a.unique_patients,
        a.unique_clinicians,
        a.active_days,
        a.avg_daily_load,
        a.avg_daily_cost,
        a.avg_cost_per_encounter,
        a.avg_cost_per_patient,
        p.peak_daily_load,
        p.low_daily_load,
        p.load_variability_std,
        CASE
            WHEN a.avg_daily_load >= 25 THEN 'VERY_HIGH_CAPACITY'
            WHEN a.avg_daily_load >= 15 THEN 'HIGH_CAPACITY'
            WHEN a.avg_daily_load >= 8  THEN 'MEDIUM_CAPACITY'
            ELSE 'LOW_CAPACITY'
        END AS estimated_capacity_band,
        CASE
            WHEN p.peak_daily_load >= a.avg_daily_load * 1.8 THEN 'HIGH_STRAIN_RISK'
            WHEN p.peak_daily_load >= a.avg_daily_load * 1.4 THEN 'MODERATE_STRAIN_RISK'
            ELSE 'LOW_STRAIN_RISK'
        END AS strain_risk_level
    FROM agg a
    INNER JOIN peaks p
        ON a.practice_id = p.practice_id;
END;
GO

/* =========================================================
   11. BUILD CLINICIAN WORKLOAD
   ========================================================= */
CREATE OR ALTER PROCEDURE ana.usp_build_clinician_workload
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE ana.clinician_workload_analysis;

    ;WITH daily AS
    (
        SELECT
            clinician_id,
            encounter_date,
            COUNT(*) AS daily_workload
        FROM dbo.patient_encounter
        GROUP BY clinician_id, encounter_date
    ),
    agg AS
    (
        SELECT
            pe.clinician_id,
            COUNT(*) AS total_encounters,
            COUNT(DISTINCT pe.patient_id) AS unique_patients,
            COUNT(DISTINCT pe.practice_id) AS unique_practices,
            COUNT(DISTINCT pe.encounter_date) AS active_days,
            CAST(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT pe.encounter_date), 0) AS DECIMAL(18,2)) AS avg_daily_workload,
            CAST(AVG(pe.cost) AS DECIMAL(18,2)) AS avg_cost_per_encounter
        FROM dbo.patient_encounter pe
        GROUP BY pe.clinician_id
    ),
    peaks AS
    (
        SELECT
            clinician_id,
            MAX(daily_workload) AS peak_daily_workload,
            CAST(STDEV(daily_workload * 1.0) AS DECIMAL(18,2)) AS workload_variability_std
        FROM daily
        GROUP BY clinician_id
    )
    INSERT INTO ana.clinician_workload_analysis
    (
        clinician_id,
        total_encounters,
        unique_patients,
        unique_practices,
        active_days,
        avg_daily_workload,
        avg_cost_per_encounter,
        peak_daily_workload,
        workload_variability_std,
        workload_risk_level
    )
    SELECT
        a.clinician_id,
        a.total_encounters,
        a.unique_patients,
        a.unique_practices,
        a.active_days,
        a.avg_daily_workload,
        a.avg_cost_per_encounter,
        p.peak_daily_workload,
        p.workload_variability_std,
        CASE
            WHEN p.peak_daily_workload >= a.avg_daily_workload * 2 THEN 'HIGH_WORKLOAD_RISK'
            WHEN p.peak_daily_workload >= a.avg_daily_workload * 1.5 THEN 'MODERATE_WORKLOAD_RISK'
            ELSE 'LOW_WORKLOAD_RISK'
        END
    FROM agg a
    INNER JOIN peaks p
        ON a.clinician_id = p.clinician_id;
END;
GO

/* =========================================================
   12. BUILD DIAGNOSIS DEMAND
   ========================================================= */
CREATE OR ALTER PROCEDURE ana.usp_build_diagnosis_demand
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE ana.diagnosis_demand_analysis;

    ;WITH daily AS
    (
        SELECT
            ISNULL(NULLIF(LTRIM(RTRIM(diagnosis_code)), ''), 'UNKNOWN') AS diagnosis_code,
            encounter_date,
            COUNT(*) AS daily_demand
        FROM dbo.patient_encounter
        GROUP BY ISNULL(NULLIF(LTRIM(RTRIM(diagnosis_code)), ''), 'UNKNOWN'), encounter_date
    ),
    agg AS
    (
        SELECT
            ISNULL(NULLIF(LTRIM(RTRIM(diagnosis_code)), ''), 'UNKNOWN') AS diagnosis_code,
            COUNT(*) AS total_encounters,
            COUNT(DISTINCT patient_id) AS unique_patients,
            CAST(SUM(cost) AS DECIMAL(18,2)) AS total_cost,
            CAST(AVG(cost) AS DECIMAL(18,2)) AS average_cost,
            COUNT(DISTINCT encounter_date) AS active_days,
            CAST(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT encounter_date), 0) AS DECIMAL(18,2)) AS avg_daily_demand
        FROM dbo.patient_encounter
        GROUP BY ISNULL(NULLIF(LTRIM(RTRIM(diagnosis_code)), ''), 'UNKNOWN')
    ),
    peaks AS
    (
        SELECT
            diagnosis_code,
            MAX(daily_demand) AS peak_daily_demand
        FROM daily
        GROUP BY diagnosis_code
    )
    INSERT INTO ana.diagnosis_demand_analysis
    (
        diagnosis_code,
        total_encounters,
        unique_patients,
        total_cost,
        average_cost,
        active_days,
        avg_daily_demand,
        peak_daily_demand,
        diagnosis_pressure_level
    )
    SELECT
        a.diagnosis_code,
        a.total_encounters,
        a.unique_patients,
        a.total_cost,
        a.average_cost,
        a.active_days,
        a.avg_daily_demand,
        p.peak_daily_demand,
        CASE
            WHEN a.total_encounters >= 200 THEN 'HIGH_PRESSURE'
            WHEN a.total_encounters >= 75 THEN 'MEDIUM_PRESSURE'
            ELSE 'LOW_PRESSURE'
        END
    FROM agg a
    INNER JOIN peaks p
        ON a.diagnosis_code = p.diagnosis_code;
END;
GO

/* =========================================================
   13. BUILD ENCOUNTER TYPE MIX
   ========================================================= */
CREATE OR ALTER PROCEDURE ana.usp_build_encounter_type_mix
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE ana.encounter_type_mix_analysis;

    ;WITH daily AS
    (
        SELECT
            encounter_type_code,
            encounter_date,
            COUNT(*) AS daily_demand
        FROM dbo.patient_encounter
        GROUP BY encounter_type_code, encounter_date
    ),
    agg AS
    (
        SELECT
            encounter_type_code,
            COUNT(*) AS total_encounters,
            COUNT(DISTINCT patient_id) AS unique_patients,
            CAST(SUM(cost) AS DECIMAL(18,2)) AS total_cost,
            CAST(AVG(cost) AS DECIMAL(18,2)) AS average_cost,
            CAST(COUNT(*) * 1.0 / NULLIF((SELECT COUNT(*) FROM dbo.patient_encounter), 0) AS DECIMAL(18,4)) AS encounter_mix_pct,
            CAST(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT encounter_date), 0) AS DECIMAL(18,2)) AS avg_daily_demand
        FROM dbo.patient_encounter
        GROUP BY encounter_type_code
    ),
    peaks AS
    (
        SELECT
            encounter_type_code,
            MAX(daily_demand) AS peak_daily_demand
        FROM daily
        GROUP BY encounter_type_code
    )
    INSERT INTO ana.encounter_type_mix_analysis
    (
        encounter_type_code,
        total_encounters,
        unique_patients,
        total_cost,
        average_cost,
        encounter_mix_pct,
        avg_daily_demand,
        peak_daily_demand,
        mix_pressure_level
    )
    SELECT
        a.encounter_type_code,
        a.total_encounters,
        a.unique_patients,
        a.total_cost,
        a.average_cost,
        a.encounter_mix_pct,
        a.avg_daily_demand,
        p.peak_daily_demand,
        CASE
            WHEN a.encounter_mix_pct >= 0.35 THEN 'DOMINANT_MIX'
            WHEN a.encounter_mix_pct >= 0.15 THEN 'SIGNIFICANT_MIX'
            ELSE 'MINOR_MIX'
        END
    FROM agg a
    INNER JOIN peaks p
        ON a.encounter_type_code = p.encounter_type_code;
END;
GO

/* =========================================================
   14. WRAPPER PROCEDURE
   ========================================================= */
CREATE OR ALTER PROCEDURE ana.usp_refresh_analysis_layer
AS
BEGIN
    SET NOCOUNT ON;

    EXEC ana.usp_build_daily_demand_metrics;
    EXEC ana.usp_build_practice_capacity;
    EXEC ana.usp_build_clinician_workload;
    EXEC ana.usp_build_diagnosis_demand;
    EXEC ana.usp_build_encounter_type_mix;
END;
GO

/* =========================================================
   15. ANALYTICAL VIEWS
   ========================================================= */

CREATE OR ALTER VIEW ana.vw_demand_dashboard
AS
SELECT
    metric_date,
    total_encounters,
    unique_patients,
    unique_practices,
    unique_clinicians,
    total_cost,
    average_cost,
    cost_per_patient,
    rolling_7d_avg,
    rolling_30d_avg,
    demand_delta,
    demand_pct_change,
    demand_status,
    demand_strain_score
FROM ana.daily_demand_metrics;
GO

CREATE OR ALTER VIEW ana.vw_practice_strain_ranking
AS
SELECT
    practice_id,
    total_encounters,
    unique_patients,
    unique_clinicians,
    active_days,
    avg_daily_load,
    avg_daily_cost,
    avg_cost_per_encounter,
    avg_cost_per_patient,
    peak_daily_load,
    load_variability_std,
    estimated_capacity_band,
    strain_risk_level,
    DENSE_RANK() OVER (ORDER BY avg_daily_load DESC) AS rank_by_avg_load,
    DENSE_RANK() OVER (ORDER BY peak_daily_load DESC) AS rank_by_peak_load
FROM ana.practice_capacity_analysis;
GO

CREATE OR ALTER VIEW ana.vw_clinician_strain_ranking
AS
SELECT
    clinician_id,
    total_encounters,
    unique_patients,
    unique_practices,
    active_days,
    avg_daily_workload,
    avg_cost_per_encounter,
    peak_daily_workload,
    workload_variability_std,
    workload_risk_level,
    DENSE_RANK() OVER (ORDER BY avg_daily_workload DESC) AS rank_by_avg_workload,
    DENSE_RANK() OVER (ORDER BY peak_daily_workload DESC) AS rank_by_peak_workload
FROM ana.clinician_workload_analysis;
GO

CREATE OR ALTER VIEW ana.vw_diagnosis_pressure
AS
SELECT
    diagnosis_code,
    total_encounters,
    unique_patients,
    total_cost,
    average_cost,
    active_days,
    avg_daily_demand,
    peak_daily_demand,
    diagnosis_pressure_level,
    DENSE_RANK() OVER (ORDER BY total_encounters DESC) AS rank_by_volume,
    DENSE_RANK() OVER (ORDER BY total_cost DESC) AS rank_by_cost
FROM ana.diagnosis_demand_analysis;
GO

CREATE OR ALTER VIEW ana.vw_encounter_type_mix_trend
AS
SELECT
    encounter_type_code,
    total_encounters,
    unique_patients,
    total_cost,
    average_cost,
    encounter_mix_pct,
    avg_daily_demand,
    peak_daily_demand,
    mix_pressure_level,
    DENSE_RANK() OVER (ORDER BY encounter_mix_pct DESC) AS rank_by_mix_share
FROM ana.encounter_type_mix_analysis;
GO

CREATE OR ALTER VIEW ana.vw_weekday_demand_pattern
AS
SELECT
    weekday_name,
    weekday_no,
    COUNT(*) AS total_encounters,
    COUNT(DISTINCT patient_id) AS unique_patients,
    CAST(SUM(cost) AS DECIMAL(18,2)) AS total_cost,
    CAST(AVG(cost) AS DECIMAL(18,2)) AS average_cost
FROM ana.vw_encounter_enriched
GROUP BY weekday_name, weekday_no;
GO

CREATE OR ALTER VIEW ana.vw_monthly_demand_pattern
AS
SELECT
    month_start_date,
    COUNT(*) AS total_encounters,
    COUNT(DISTINCT patient_id) AS unique_patients,
    COUNT(DISTINCT practice_id) AS unique_practices,
    CAST(SUM(cost) AS DECIMAL(18,2)) AS total_cost,
    CAST(AVG(cost) AS DECIMAL(18,2)) AS average_cost
FROM ana.vw_encounter_enriched
GROUP BY month_start_date;
GO

/* =========================================================
   16. TEST / VALIDATION QUERIES
   ========================================================= */

-- EXEC ana.usp_refresh_analysis_layer;

-- SELECT * FROM ana.vw_demand_dashboard ORDER BY metric_date;
-- SELECT * FROM ana.vw_practice_strain_ranking ORDER BY rank_by_avg_load;
-- SELECT * FROM ana.vw_clinician_strain_ranking ORDER BY rank_by_avg_workload;
-- SELECT * FROM ana.vw_diagnosis_pressure ORDER BY rank_by_volume;
-- SELECT * FROM ana.vw_encounter_type_mix_trend ORDER BY rank_by_mix_share;
-- SELECT * FROM ana.vw_weekday_demand_pattern ORDER BY weekday_no;
-- SELECT * FROM ana.vw_monthly_demand_pattern ORDER BY month_start_date;
