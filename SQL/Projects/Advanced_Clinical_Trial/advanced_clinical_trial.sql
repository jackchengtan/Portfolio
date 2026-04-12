-- ADVANCED CLINICAL TRIAL ANALYSIS (PRODUCTION-STYLE)

IF DB_ID('ClinicalTrialDB') IS NULL
CREATE DATABASE ClinicalTrialDB;
GO
USE ClinicalTrialDB;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='trial') EXEC('CREATE SCHEMA trial');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='ana') EXEC('CREATE SCHEMA ana');
GO

CREATE TABLE trial.patient_enrollment(
    patient_id INT PRIMARY KEY,
    trial_group VARCHAR(20),
    enrollment_date DATE
);

CREATE TABLE trial.patient_outcomes(
    outcome_id INT IDENTITY PRIMARY KEY,
    patient_id INT,
    outcome_date DATE,
    outcome_score INT
);

CREATE TABLE trial.patient_encounter(
    encounter_id INT IDENTITY PRIMARY KEY,
    patient_id INT,
    encounter_date DATE,
    cost DECIMAL(10,2)
);

CREATE OR ALTER VIEW ana.vw_outcome_trend AS
SELECT e.trial_group, o.outcome_date, AVG(o.outcome_score) avg_score
FROM trial.patient_outcomes o
JOIN trial.patient_enrollment e ON o.patient_id = e.patient_id
GROUP BY e.trial_group, o.outcome_date;

CREATE OR ALTER VIEW ana.vw_patient_improvement AS
SELECT patient_id,
MIN(outcome_score) best_score,
MAX(outcome_score) worst_score,
MAX(outcome_score)-MIN(outcome_score) improvement
FROM trial.patient_outcomes
GROUP BY patient_id;

CREATE OR ALTER VIEW ana.vw_treatment_effect AS
SELECT e.trial_group, AVG(i.improvement) avg_improvement, COUNT(*) patients
FROM ana.vw_patient_improvement i
JOIN trial.patient_enrollment e ON i.patient_id = e.patient_id
GROUP BY e.trial_group;

CREATE OR ALTER VIEW ana.vw_patient_segments AS
SELECT e.trial_group, i.patient_id,
CASE WHEN i.improvement>=3 THEN 'Strong'
     WHEN i.improvement>=1 THEN 'Moderate'
     ELSE 'None' END response_group
FROM ana.vw_patient_improvement i
JOIN trial.patient_enrollment e ON i.patient_id = e.patient_id;

CREATE OR ALTER VIEW ana.vw_cost_effectiveness AS
SELECT e.trial_group, AVG(enc.cost) avg_cost, SUM(enc.cost) total_cost
FROM trial.patient_enrollment e
JOIN trial.patient_encounter enc ON e.patient_id = enc.patient_id
GROUP BY e.trial_group;

SELECT * FROM ana.vw_treatment_effect;
