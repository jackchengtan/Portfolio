-- FULL HEALTHCARE ANALYSIS PROJECT (DEMAND & STRAIN)

IF DB_ID('PortfolioETL') IS NULL
CREATE DATABASE PortfolioETL;
GO

USE PortfolioETL;
GO

-- Schema
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='ana')
EXEC('CREATE SCHEMA ana');
GO

-- Enriched view
CREATE OR ALTER VIEW ana.vw_encounter_enriched AS
SELECT
    encounter_id,
    patient_id,
    practice_id,
    encounter_date,
    DATENAME(WEEKDAY, encounter_date) AS weekday,
    DATEPART(WEEK, encounter_date) AS week_no,
    DATEPART(MONTH, encounter_date) AS month_no,
    encounter_type,
    clinician_id,
    cost
FROM dbo.patient_encounter;
GO

-- Daily demand metrics table
CREATE TABLE ana.daily_demand_metrics (
    metric_date DATE PRIMARY KEY,
    total_encounters INT,
    rolling_avg FLOAT,
    rolling_std FLOAT,
    demand_status VARCHAR(50)
);
GO

-- Populate demand metrics
CREATE OR ALTER PROCEDURE ana.usp_build_demand_metrics AS
BEGIN
    WITH daily AS (
        SELECT encounter_date, COUNT(*) total_encounters
        FROM dbo.patient_encounter
        GROUP BY encounter_date
    ),
    calc AS (
        SELECT *,
        AVG(total_encounters*1.0) OVER (ORDER BY encounter_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS rolling_avg,
        STDEV(total_encounters*1.0) OVER (ORDER BY encounter_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS rolling_std
        FROM daily
    )
    INSERT INTO ana.daily_demand_metrics
    SELECT
        encounter_date,
        total_encounters,
        rolling_avg,
        rolling_std,
        CASE
            WHEN total_encounters > rolling_avg + 2*rolling_std THEN 'OVERLOAD'
            WHEN total_encounters < rolling_avg - 2*rolling_std THEN 'UNDERUTILISED'
            ELSE 'NORMAL'
        END
    FROM calc
    WHERE rolling_avg IS NOT NULL;
END;
GO

-- Practice performance table
CREATE TABLE ana.practice_capacity (
    practice_id INT,
    avg_daily_load FLOAT,
    total_encounters INT,
    unique_patients INT
);
GO

CREATE OR ALTER PROCEDURE ana.usp_build_practice_capacity AS
BEGIN
    INSERT INTO ana.practice_capacity
    SELECT
        practice_id,
        COUNT(*) * 1.0 / COUNT(DISTINCT encounter_date),
        COUNT(*),
        COUNT(DISTINCT patient_id)
    FROM dbo.patient_encounter
    GROUP BY practice_id;
END;
GO
