/* =========================================================
   PATIENT SEGMENTATION LAYER
   ========================================================= */

USE PortfolioHealthcareETL;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'seg')
    EXEC('CREATE SCHEMA seg');
GO

IF OBJECT_ID('seg.usp_refresh_patient_segmentation', 'P') IS NOT NULL DROP PROCEDURE seg.usp_refresh_patient_segmentation;
GO
IF OBJECT_ID('seg.vw_patient_segment_summary', 'V') IS NOT NULL DROP VIEW seg.vw_patient_segment_summary;
GO
IF OBJECT_ID('seg.vw_high_value_patients', 'V') IS NOT NULL DROP VIEW seg.vw_high_value_patients;
GO
IF OBJECT_ID('seg.vw_patient_segment_distribution', 'V') IS NOT NULL DROP VIEW seg.vw_patient_segment_distribution;
GO
IF OBJECT_ID('seg.patient_segmentation', 'U') IS NOT NULL DROP TABLE seg.patient_segmentation;
GO

CREATE TABLE seg.patient_segmentation
(
    patient_id                    INT NOT NULL PRIMARY KEY,
    first_encounter_date          DATE NULL,
    last_encounter_date           DATE NULL,
    active_days                   INT NOT NULL,
    total_encounters              INT NOT NULL,
    total_cost                    DECIMAL(18,2) NOT NULL,
    avg_cost_per_encounter        DECIMAL(18,2) NOT NULL,
    distinct_practices            INT NOT NULL,
    distinct_clinicians           INT NOT NULL,
    distinct_diagnoses            INT NOT NULL,
    distinct_encounter_types      INT NOT NULL,
    recency_days                  INT NULL,
    utilisation_segment           VARCHAR(50) NOT NULL,
    cost_segment                  VARCHAR(50) NOT NULL,
    complexity_segment            VARCHAR(50) NOT NULL,
    overall_patient_segment       VARCHAR(100) NOT NULL,
    created_datetime              DATETIME2(0) NOT NULL DEFAULT SYSDATETIME()
);
GO

CREATE OR ALTER PROCEDURE seg.usp_refresh_patient_segmentation
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE seg.patient_segmentation;

    DECLARE @max_date DATE;
    SELECT @max_date = MAX(encounter_date) FROM dbo.patient_encounter;

    ;WITH base AS
    (
        SELECT
            patient_id,
            MIN(encounter_date) AS first_encounter_date,
            MAX(encounter_date) AS last_encounter_date,
            COUNT(DISTINCT encounter_date) AS active_days,
            COUNT(*) AS total_encounters,
            CAST(SUM(cost) AS DECIMAL(18,2)) AS total_cost,
            CAST(AVG(cost) AS DECIMAL(18,2)) AS avg_cost_per_encounter,
            COUNT(DISTINCT practice_id) AS distinct_practices,
            COUNT(DISTINCT clinician_id) AS distinct_clinicians,
            COUNT(DISTINCT ISNULL(NULLIF(LTRIM(RTRIM(diagnosis_code)), ''), 'UNKNOWN')) AS distinct_diagnoses,
            COUNT(DISTINCT encounter_type_code) AS distinct_encounter_types
        FROM dbo.patient_encounter
        GROUP BY patient_id
    )
    INSERT INTO seg.patient_segmentation
    (
        patient_id,
        first_encounter_date,
        last_encounter_date,
        active_days,
        total_encounters,
        total_cost,
        avg_cost_per_encounter,
        distinct_practices,
        distinct_clinicians,
        distinct_diagnoses,
        distinct_encounter_types,
        recency_days,
        utilisation_segment,
        cost_segment,
        complexity_segment,
        overall_patient_segment
    )
    SELECT
        patient_id,
        first_encounter_date,
        last_encounter_date,
        active_days,
        total_encounters,
        total_cost,
        avg_cost_per_encounter,
        distinct_practices,
        distinct_clinicians,
        distinct_diagnoses,
        distinct_encounter_types,
        DATEDIFF(DAY, last_encounter_date, @max_date) AS recency_days,
        CASE
            WHEN total_encounters >= 12 THEN 'HIGH_UTILISATION'
            WHEN total_encounters >= 5 THEN 'MEDIUM_UTILISATION'
            ELSE 'LOW_UTILISATION'
        END AS utilisation_segment,
        CASE
            WHEN total_cost >= 700 THEN 'HIGH_COST'
            WHEN total_cost >= 250 THEN 'MEDIUM_COST'
            ELSE 'LOW_COST'
        END AS cost_segment,
        CASE
            WHEN distinct_diagnoses >= 4 OR distinct_encounter_types >= 3 OR distinct_practices >= 2 THEN 'HIGH_COMPLEXITY'
            WHEN distinct_diagnoses >= 2 OR distinct_encounter_types >= 2 THEN 'MEDIUM_COMPLEXITY'
            ELSE 'LOW_COMPLEXITY'
        END AS complexity_segment,
        CASE
            WHEN total_encounters >= 12 AND total_cost >= 700 THEN 'HIGH_NEEDS_HIGH_COST'
            WHEN total_encounters >= 12 THEN 'FREQUENT_ATTENDER'
            WHEN total_cost >= 700 THEN 'HIGH_COST_CASE'
            WHEN distinct_diagnoses >= 4 OR distinct_practices >= 2 THEN 'COMPLEX_MULTI_TOUCH'
            WHEN DATEDIFF(DAY, last_encounter_date, @max_date) <= 30 AND total_encounters <= 3 THEN 'RECENT_LOW_TOUCH'
            ELSE 'STABLE_STANDARD'
        END AS overall_patient_segment
    FROM base;
END;
GO

CREATE OR ALTER VIEW seg.vw_patient_segment_summary
AS
SELECT
    overall_patient_segment,
    COUNT(*) AS patient_count,
    CAST(AVG(total_encounters * 1.0) AS DECIMAL(18,2)) AS avg_encounters,
    CAST(AVG(total_cost) AS DECIMAL(18,2)) AS avg_total_cost,
    CAST(SUM(total_cost) AS DECIMAL(18,2)) AS total_segment_cost
FROM seg.patient_segmentation
GROUP BY overall_patient_segment;
GO

CREATE OR ALTER VIEW seg.vw_high_value_patients
AS
SELECT
    patient_id,
    total_encounters,
    total_cost,
    avg_cost_per_encounter,
    distinct_practices,
    distinct_clinicians,
    distinct_diagnoses,
    distinct_encounter_types,
    recency_days,
    overall_patient_segment,
    DENSE_RANK() OVER (ORDER BY total_cost DESC) AS cost_rank
FROM seg.patient_segmentation
WHERE overall_patient_segment IN ('HIGH_NEEDS_HIGH_COST', 'HIGH_COST_CASE', 'COMPLEX_MULTI_TOUCH');
GO

CREATE OR ALTER VIEW seg.vw_patient_segment_distribution
AS
SELECT
    utilisation_segment,
    cost_segment,
    complexity_segment,
    COUNT(*) AS patient_count
FROM seg.patient_segmentation
GROUP BY utilisation_segment, cost_segment, complexity_segment;
GO
