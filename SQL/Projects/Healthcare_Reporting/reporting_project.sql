/*
    Project: Healthcare Reporting & Insights Layer
    Database: PortfolioETL
    Purpose:
        Create a reporting layer on top of the healthcare ETL and data quality projects.
        This script creates:
          - database (if missing)
          - reporting schema
          - reusable SQL views for KPIs and business reporting
          - stored procedures to populate reporting snapshot tables
          - optional snapshot tables for point-in-time reporting
          - example queries for dashboard use

    Notes:
      1. This project assumes the core ETL table dbo.patient_encounter already exists.
      2. The table was created in the earlier ETL project.
      3. All data is intended to be synthetic / portfolio-safe.
      4. This script is written for SQL Server.
*/

/*==============================================================
  1. CREATE DATABASE IF NEEDED
==============================================================*/
IF DB_ID('PortfolioETL') IS NULL
BEGIN
    CREATE DATABASE PortfolioETL;
END
GO

USE PortfolioETL;
GO

/*==============================================================
  2. PRE-CHECK NOTES
==============================================================*/
-- This reporting layer depends on dbo.patient_encounter from the ETL project.
-- If the table does not exist yet, stop and run the ETL project first.
IF OBJECT_ID('dbo.patient_encounter', 'U') IS NULL
BEGIN
    RAISERROR('Required table dbo.patient_encounter was not found. Run the ETL project setup first.', 16, 1);
    RETURN;
END
GO

/*==============================================================
  3. CREATE REPORTING SCHEMA
==============================================================*/
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'rpt')
    EXEC('CREATE SCHEMA rpt');
GO

/*==============================================================
  4. SNAPSHOT TABLES
     These tables are optional but useful for dashboards, exports,
     and point-in-time KPI tracking.
==============================================================*/

-- Daily operational KPI snapshot.
IF OBJECT_ID('rpt.daily_kpi_snapshot', 'U') IS NOT NULL
    DROP TABLE rpt.daily_kpi_snapshot;
GO

CREATE TABLE rpt.daily_kpi_snapshot
(
    snapshot_date           DATE           NOT NULL PRIMARY KEY,
    total_encounters        INT            NOT NULL,
    unique_patients         INT            NOT NULL,
    unique_practices        INT            NOT NULL,
    total_cost              DECIMAL(18,2)  NOT NULL,
    average_cost            DECIMAL(18,2)  NOT NULL,
    average_cost_per_patient DECIMAL(18,2) NULL,
    created_datetime        DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME()
);
GO

-- Practice-level daily snapshot.
IF OBJECT_ID('rpt.daily_practice_snapshot', 'U') IS NOT NULL
    DROP TABLE rpt.daily_practice_snapshot;
GO

CREATE TABLE rpt.daily_practice_snapshot
(
    snapshot_date           DATE           NOT NULL,
    practice_id             INT            NOT NULL,
    total_encounters        INT            NOT NULL,
    unique_patients         INT            NOT NULL,
    total_cost              DECIMAL(18,2)  NOT NULL,
    average_cost            DECIMAL(18,2)  NOT NULL,
    created_datetime        DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_daily_practice_snapshot PRIMARY KEY (snapshot_date, practice_id)
);
GO

-- Encounter type daily snapshot.
IF OBJECT_ID('rpt.daily_encounter_type_snapshot', 'U') IS NOT NULL
    DROP TABLE rpt.daily_encounter_type_snapshot;
GO

CREATE TABLE rpt.daily_encounter_type_snapshot
(
    snapshot_date           DATE           NOT NULL,
    encounter_type          VARCHAR(100)   NOT NULL,
    total_encounters        INT            NOT NULL,
    total_cost              DECIMAL(18,2)  NOT NULL,
    average_cost            DECIMAL(18,2)  NOT NULL,
    created_datetime        DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_daily_encounter_type_snapshot PRIMARY KEY (snapshot_date, encounter_type)
);
GO

/*==============================================================
  5. REPORTING VIEWS
     These views are designed for Power BI, Excel, or ad hoc SQL use.
==============================================================*/

-- View 1: Daily operational activity.
CREATE OR ALTER VIEW rpt.vw_daily_activity
AS
SELECT
    pe.encounter_date,
    COUNT(*) AS total_encounters,
    COUNT(DISTINCT pe.patient_id) AS unique_patients,
    COUNT(DISTINCT pe.practice_id) AS unique_practices,
    CAST(SUM(pe.cost) AS DECIMAL(18,2)) AS total_cost,
    CAST(AVG(pe.cost) AS DECIMAL(18,2)) AS average_cost,
    CAST(SUM(pe.cost) / NULLIF(COUNT(DISTINCT pe.patient_id), 0) AS DECIMAL(18,2)) AS average_cost_per_patient
FROM dbo.patient_encounter pe
GROUP BY pe.encounter_date;
GO

-- View 2: Practice performance across the full dataset.
CREATE OR ALTER VIEW rpt.vw_practice_performance
AS
SELECT
    pe.practice_id,
    COUNT(*) AS total_encounters,
    COUNT(DISTINCT pe.patient_id) AS unique_patients,
    COUNT(DISTINCT pe.clinician_id) AS unique_clinicians,
    CAST(SUM(pe.cost) AS DECIMAL(18,2)) AS total_cost,
    CAST(AVG(pe.cost) AS DECIMAL(18,2)) AS average_cost,
    MIN(pe.encounter_date) AS first_encounter_date,
    MAX(pe.encounter_date) AS last_encounter_date
FROM dbo.patient_encounter pe
GROUP BY pe.practice_id;
GO

-- View 3: Encounter type analysis across the full dataset.
CREATE OR ALTER VIEW rpt.vw_encounter_type_analysis
AS
SELECT
    pe.encounter_type,
    COUNT(*) AS total_encounters,
    COUNT(DISTINCT pe.patient_id) AS unique_patients,
    CAST(SUM(pe.cost) AS DECIMAL(18,2)) AS total_cost,
    CAST(AVG(pe.cost) AS DECIMAL(18,2)) AS average_cost
FROM dbo.patient_encounter pe
GROUP BY pe.encounter_type;
GO

-- View 4: Monthly trend analysis.
CREATE OR ALTER VIEW rpt.vw_monthly_encounter_trend
AS
SELECT
    DATEFROMPARTS(YEAR(pe.encounter_date), MONTH(pe.encounter_date), 1) AS month_start_date,
    COUNT(*) AS total_encounters,
    COUNT(DISTINCT pe.patient_id) AS unique_patients,
    COUNT(DISTINCT pe.practice_id) AS unique_practices,
    CAST(SUM(pe.cost) AS DECIMAL(18,2)) AS total_cost,
    CAST(AVG(pe.cost) AS DECIMAL(18,2)) AS average_cost
FROM dbo.patient_encounter pe
GROUP BY DATEFROMPARTS(YEAR(pe.encounter_date), MONTH(pe.encounter_date), 1);
GO

-- View 5: Clinician workload summary.
CREATE OR ALTER VIEW rpt.vw_clinician_workload
AS
SELECT
    pe.clinician_id,
    COUNT(*) AS total_encounters,
    COUNT(DISTINCT pe.patient_id) AS unique_patients,
    COUNT(DISTINCT pe.practice_id) AS practices_covered,
    CAST(SUM(pe.cost) AS DECIMAL(18,2)) AS total_cost,
    CAST(AVG(pe.cost) AS DECIMAL(18,2)) AS average_cost,
    MIN(pe.encounter_date) AS first_encounter_date,
    MAX(pe.encounter_date) AS last_encounter_date
FROM dbo.patient_encounter pe
GROUP BY pe.clinician_id;
GO

-- View 6: Diagnosis code summary.
CREATE OR ALTER VIEW rpt.vw_diagnosis_summary
AS
SELECT
    ISNULL(NULLIF(LTRIM(RTRIM(pe.diagnosis_code)), ''), 'UNKNOWN') AS diagnosis_code,
    COUNT(*) AS total_encounters,
    COUNT(DISTINCT pe.patient_id) AS unique_patients,
    CAST(SUM(pe.cost) AS DECIMAL(18,2)) AS total_cost,
    CAST(AVG(pe.cost) AS DECIMAL(18,2)) AS average_cost
FROM dbo.patient_encounter pe
GROUP BY ISNULL(NULLIF(LTRIM(RTRIM(pe.diagnosis_code)), ''), 'UNKNOWN');
GO

-- View 7: High-level executive KPI view.
CREATE OR ALTER VIEW rpt.vw_executive_kpis
AS
SELECT
    COUNT(*) AS total_encounters,
    COUNT(DISTINCT patient_id) AS total_unique_patients,
    COUNT(DISTINCT practice_id) AS total_unique_practices,
    COUNT(DISTINCT clinician_id) AS total_unique_clinicians,
    CAST(SUM(cost) AS DECIMAL(18,2)) AS total_cost,
    CAST(AVG(cost) AS DECIMAL(18,2)) AS average_cost,
    MIN(encounter_date) AS data_start_date,
    MAX(encounter_date) AS data_end_date
FROM dbo.patient_encounter;
GO

/*==============================================================
  6. STORED PROCEDURES TO POPULATE SNAPSHOT TABLES
==============================================================*/

-- Procedure 1: refresh daily KPI snapshot for all dates.
CREATE OR ALTER PROCEDURE rpt.usp_refresh_daily_kpi_snapshot
AS
BEGIN
    SET NOCOUNT ON;

    -- Rebuild snapshot for simplicity and consistency.
    TRUNCATE TABLE rpt.daily_kpi_snapshot;

    INSERT INTO rpt.daily_kpi_snapshot
    (
        snapshot_date,
        total_encounters,
        unique_patients,
        unique_practices,
        total_cost,
        average_cost,
        average_cost_per_patient
    )
    SELECT
        encounter_date,
        COUNT(*) AS total_encounters,
        COUNT(DISTINCT patient_id) AS unique_patients,
        COUNT(DISTINCT practice_id) AS unique_practices,
        CAST(SUM(cost) AS DECIMAL(18,2)) AS total_cost,
        CAST(AVG(cost) AS DECIMAL(18,2)) AS average_cost,
        CAST(SUM(cost) / NULLIF(COUNT(DISTINCT patient_id), 0) AS DECIMAL(18,2)) AS average_cost_per_patient
    FROM dbo.patient_encounter
    GROUP BY encounter_date;
END;
GO

-- Procedure 2: refresh practice-level snapshot.
CREATE OR ALTER PROCEDURE rpt.usp_refresh_daily_practice_snapshot
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE rpt.daily_practice_snapshot;

    INSERT INTO rpt.daily_practice_snapshot
    (
        snapshot_date,
        practice_id,
        total_encounters,
        unique_patients,
        total_cost,
        average_cost
    )
    SELECT
        encounter_date AS snapshot_date,
        practice_id,
        COUNT(*) AS total_encounters,
        COUNT(DISTINCT patient_id) AS unique_patients,
        CAST(SUM(cost) AS DECIMAL(18,2)) AS total_cost,
        CAST(AVG(cost) AS DECIMAL(18,2)) AS average_cost
    FROM dbo.patient_encounter
    GROUP BY encounter_date, practice_id;
END;
GO

-- Procedure 3: refresh encounter type snapshot.
CREATE OR ALTER PROCEDURE rpt.usp_refresh_daily_encounter_type_snapshot
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE rpt.daily_encounter_type_snapshot;

    INSERT INTO rpt.daily_encounter_type_snapshot
    (
        snapshot_date,
        encounter_type,
        total_encounters,
        total_cost,
        average_cost
    )
    SELECT
        encounter_date AS snapshot_date,
        encounter_type,
        COUNT(*) AS total_encounters,
        CAST(SUM(cost) AS DECIMAL(18,2)) AS total_cost,
        CAST(AVG(cost) AS DECIMAL(18,2)) AS average_cost
    FROM dbo.patient_encounter
    GROUP BY encounter_date, encounter_type;
END;
GO

-- Procedure 4: wrapper to refresh the full reporting layer.
CREATE OR ALTER PROCEDURE rpt.usp_refresh_reporting_layer
AS
BEGIN
    SET NOCOUNT ON;

    EXEC rpt.usp_refresh_daily_kpi_snapshot;
    EXEC rpt.usp_refresh_daily_practice_snapshot;
    EXEC rpt.usp_refresh_daily_encounter_type_snapshot;
END;
GO

/*==============================================================
  7. OPTIONAL SQL AGENT JOB SCRIPT
     This refreshes the reporting layer daily after the ETL completes.
     Note: Run in msdb after reviewing job names and schedules.
==============================================================*/
--
-- USE msdb;
-- GO
--
-- EXEC dbo.sp_add_job
--     @job_name = 'Daily Reporting Layer Refresh',
--     @enabled = 1,
--     @description = 'Refreshes reporting snapshot tables for the healthcare portfolio project';
-- GO
--
-- EXEC dbo.sp_add_jobstep
--     @job_name = 'Daily Reporting Layer Refresh',
--     @step_name = 'Refresh reporting layer',
--     @subsystem = 'TSQL',
--     @database_name = 'PortfolioETL',
--     @command = 'EXEC rpt.usp_refresh_reporting_layer;';
-- GO
--
-- EXEC dbo.sp_add_schedule
--     @schedule_name = 'Daily 01_30 AM Reporting Refresh',
--     @freq_type = 4,
--     @freq_interval = 1,
--     @active_start_time = 013000;
-- GO
--
-- EXEC dbo.sp_attach_schedule
--     @job_name = 'Daily Reporting Layer Refresh',
--     @schedule_name = 'Daily 01_30 AM Reporting Refresh';
-- GO
--
-- EXEC dbo.sp_add_jobserver
--     @job_name = 'Daily Reporting Layer Refresh';
-- GO

/*==============================================================
  8. EXAMPLE VALIDATION / TEST QUERIES
==============================================================*/

-- Refresh snapshots.
-- EXEC rpt.usp_refresh_reporting_layer;

-- Review daily KPI view.
-- SELECT * FROM rpt.vw_daily_activity ORDER BY encounter_date;

-- Review practice performance.
-- SELECT * FROM rpt.vw_practice_performance ORDER BY total_encounters DESC;

-- Review encounter type performance.
-- SELECT * FROM rpt.vw_encounter_type_analysis ORDER BY total_encounters DESC;

-- Review monthly trend.
-- SELECT * FROM rpt.vw_monthly_encounter_trend ORDER BY month_start_date;

-- Review clinician workload.
-- SELECT * FROM rpt.vw_clinician_workload ORDER BY total_encounters DESC;

-- Review diagnosis summary.
-- SELECT * FROM rpt.vw_diagnosis_summary ORDER BY total_encounters DESC;

-- Review executive KPIs.
-- SELECT * FROM rpt.vw_executive_kpis;

-- Review snapshot tables.
-- SELECT * FROM rpt.daily_kpi_snapshot ORDER BY snapshot_date;
-- SELECT * FROM rpt.daily_practice_snapshot ORDER BY snapshot_date, practice_id;
-- SELECT * FROM rpt.daily_encounter_type_snapshot ORDER BY snapshot_date, encounter_type;

/*==============================================================
  9. DASHBOARD BUILD NOTES
==============================================================*/
-- Recommended visuals for Power BI or Excel:
--  1. KPI cards: Total encounters, unique patients, total cost, average cost
--  2. Line chart: Daily encounters over time
--  3. Column chart: Encounters by practice
--  4. Bar chart: Encounters by encounter type
--  5. Table: Top diagnosis codes by volume and cost
--  6. Table: Clinician workload summary
--  7. Trend chart: Monthly encounter trend
--
-- Suggested Power BI data sources:
--  - rpt.vw_executive_kpis
--  - rpt.vw_daily_activity
--  - rpt.vw_practice_performance
--  - rpt.vw_encounter_type_analysis
--  - rpt.vw_monthly_encounter_trend
--  - rpt.vw_clinician_workload
--  - rpt.vw_diagnosis_summary

/*==============================================================
  10. PROJECT SUMMARY
==============================================================*/
-- This reporting layer demonstrates:
--  - SQL view design for BI / dashboard use
--  - KPI modelling for operational reporting
--  - snapshot table refresh logic
--  - business-facing reporting on synthetic healthcare data
--  - portfolio-ready SQL development and analytics thinking
