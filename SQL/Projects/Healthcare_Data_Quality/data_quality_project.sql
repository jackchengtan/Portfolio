-- Healthcare Data Quality Project Script
-- Includes schemas, tables, procedures, and execution

IF DB_ID('PortfolioETL') IS NULL
BEGIN
    CREATE DATABASE PortfolioETL;
END
GO

USE PortfolioETL;
GO

-- Create schema
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dq')
EXEC('CREATE SCHEMA dq');
GO

-- Metrics table
CREATE TABLE dq.daily_encounter_metrics (
    metric_date DATE PRIMARY KEY,
    total_records INT,
    distinct_patients INT,
    distinct_practices INT,
    avg_cost DECIMAL(12,2),
    created_datetime DATETIME2 DEFAULT SYSDATETIME()
);
GO

-- Results table
CREATE TABLE dq.dq_results (
    dq_result_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    rule_name VARCHAR(200),
    run_date DATE,
    metric_value DECIMAL(18,2),
    threshold_min DECIMAL(18,2),
    threshold_max DECIMAL(18,2),
    status VARCHAR(20),
    created_datetime DATETIME2 DEFAULT SYSDATETIME()
);
GO

-- Procedure: Calculate metrics
CREATE OR ALTER PROCEDURE dq.usp_calculate_daily_metrics
AS
BEGIN
    INSERT INTO dq.daily_encounter_metrics
    SELECT
        CAST(encounter_date AS DATE),
        COUNT(*),
        COUNT(DISTINCT patient_id),
        COUNT(DISTINCT practice_id),
        AVG(cost),
        SYSDATETIME()
    FROM dbo.patient_encounter
    GROUP BY CAST(encounter_date AS DATE);
END;
GO

-- Procedure: Detect anomalies
CREATE OR ALTER PROCEDURE dq.usp_detect_volume_anomalies
AS
BEGIN
    ;WITH metrics AS (
        SELECT *,
        AVG(total_records*1.0) OVER (ORDER BY metric_date ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS rolling_avg,
        STDEV(total_records*1.0) OVER (ORDER BY metric_date ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS rolling_std
        FROM dq.daily_encounter_metrics
    )
    INSERT INTO dq.dq_results
    SELECT
        'Volume Check',
        metric_date,
        total_records,
        rolling_avg - 2*rolling_std,
        rolling_avg + 2*rolling_std,
        CASE WHEN total_records BETWEEN rolling_avg - 2*rolling_std AND rolling_avg + 2*rolling_std
             THEN 'PASS' ELSE 'FAIL' END,
        SYSDATETIME()
    FROM metrics
    WHERE rolling_avg IS NOT NULL;
END;
GO

-- Procedure: Run all checks
CREATE OR ALTER PROCEDURE dq.usp_run_data_quality
AS
BEGIN
    EXEC dq.usp_calculate_daily_metrics;
    EXEC dq.usp_detect_volume_anomalies;
END;
GO
