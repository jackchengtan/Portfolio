/* =========================================================
   FORECASTING LAYER
   Platform: SQL Server
   Database: PortfolioHealthcareETL
   Purpose:
       Demand forecasting outputs for healthcare encounter data
   ========================================================= */

USE master;
GO

IF DB_ID('PortfolioHealthcareETL') IS NULL
BEGIN
    RAISERROR('Database PortfolioHealthcareETL does not exist.', 16, 1);
END
GO

USE PortfolioHealthcareETL;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'fcst')
    EXEC('CREATE SCHEMA fcst');
GO

/* =========================================================
   DROP OBJECTS IF RE-RUNNING
   ========================================================= */
IF OBJECT_ID('fcst.usp_refresh_forecasting_layer', 'P') IS NOT NULL DROP PROCEDURE fcst.usp_refresh_forecasting_layer;
GO
IF OBJECT_ID('fcst.usp_build_practice_forecast', 'P') IS NOT NULL DROP PROCEDURE fcst.usp_build_practice_forecast;
GO
IF OBJECT_ID('fcst.usp_build_daily_forecast', 'P') IS NOT NULL DROP PROCEDURE fcst.usp_build_daily_forecast;
GO
IF OBJECT_ID('fcst.vw_forecast_dashboard', 'V') IS NOT NULL DROP VIEW fcst.vw_forecast_dashboard;
GO
IF OBJECT_ID('fcst.vw_practice_forecast', 'V') IS NOT NULL DROP VIEW fcst.vw_practice_forecast;
GO

IF OBJECT_ID('fcst.practice_demand_forecast', 'U') IS NOT NULL DROP TABLE fcst.practice_demand_forecast;
GO
IF OBJECT_ID('fcst.daily_demand_forecast', 'U') IS NOT NULL DROP TABLE fcst.daily_demand_forecast;
GO

/* =========================================================
   FORECAST TABLES
   ========================================================= */
CREATE TABLE fcst.daily_demand_forecast
(
    forecast_date                DATE         NOT NULL PRIMARY KEY,
    forecast_horizon_days        INT          NOT NULL,
    forecast_total_encounters    DECIMAL(18,2) NOT NULL,
    lower_bound_encounters       DECIMAL(18,2) NULL,
    upper_bound_encounters       DECIMAL(18,2) NULL,
    baseline_7d_avg              DECIMAL(18,2) NULL,
    baseline_30d_avg             DECIMAL(18,2) NULL,
    recent_trend_factor          DECIMAL(18,4) NULL,
    weekday_seasonality_factor   DECIMAL(18,4) NULL,
    forecast_method              VARCHAR(100) NOT NULL,
    created_datetime             DATETIME2(0) NOT NULL DEFAULT SYSDATETIME()
);
GO

CREATE TABLE fcst.practice_demand_forecast
(
    forecast_date                DATE          NOT NULL,
    practice_id                  INT           NOT NULL,
    forecast_total_encounters    DECIMAL(18,2) NOT NULL,
    practice_baseline_avg        DECIMAL(18,2) NULL,
    practice_trend_factor        DECIMAL(18,4) NULL,
    created_datetime             DATETIME2(0)  NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_fcst_practice_demand_forecast PRIMARY KEY (forecast_date, practice_id)
);
GO

/* =========================================================
   BUILD DAILY FORECAST
   ========================================================= */
CREATE OR ALTER PROCEDURE fcst.usp_build_daily_forecast
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE fcst.daily_demand_forecast;

    ;WITH daily AS
    (
        SELECT
            encounter_date,
            COUNT(*) AS total_encounters,
            DATEPART(WEEKDAY, encounter_date) AS weekday_no
        FROM dbo.patient_encounter
        GROUP BY encounter_date
    ),
    weekday_pattern AS
    (
        SELECT
            weekday_no,
            AVG(total_encounters * 1.0) AS weekday_avg
        FROM daily
        GROUP BY weekday_no
    ),
    overall_avg AS
    (
        SELECT AVG(total_encounters * 1.0) AS overall_avg
        FROM daily
    ),
    recent_base AS
    (
        SELECT
            AVG(CASE WHEN rn <= 7  THEN total_encounters * 1.0 END) AS avg_7d,
            AVG(CASE WHEN rn <= 30 THEN total_encounters * 1.0 END) AS avg_30d,
            STDEV(CASE WHEN rn <= 30 THEN total_encounters * 1.0 END) AS std_30d
        FROM
        (
            SELECT
                encounter_date,
                total_encounters,
                ROW_NUMBER() OVER (ORDER BY encounter_date DESC) AS rn
            FROM daily
        ) d
    ),
    last_date AS
    (
        SELECT MAX(encounter_date) AS max_date
        FROM daily
    ),
    horizons AS
    (
        SELECT 1 AS horizon_days
        UNION ALL SELECT 2
        UNION ALL SELECT 3
        UNION ALL SELECT 4
        UNION ALL SELECT 5
        UNION ALL SELECT 6
        UNION ALL SELECT 7
    ),
    future_dates AS
    (
        SELECT
            DATEADD(DAY, h.horizon_days, l.max_date) AS forecast_date,
            h.horizon_days,
            DATEPART(WEEKDAY, DATEADD(DAY, h.horizon_days, l.max_date)) AS weekday_no
        FROM horizons h
        CROSS JOIN last_date l
    )
    INSERT INTO fcst.daily_demand_forecast
    (
        forecast_date,
        forecast_horizon_days,
        forecast_total_encounters,
        lower_bound_encounters,
        upper_bound_encounters,
        baseline_7d_avg,
        baseline_30d_avg,
        recent_trend_factor,
        weekday_seasonality_factor,
        forecast_method
    )
    SELECT
        f.forecast_date,
        f.horizon_days,
        CAST(
            (
                rb.avg_7d * 0.6 +
                rb.avg_30d * 0.4
            )
            * CASE
                WHEN rb.avg_30d IS NULL OR rb.avg_30d = 0 THEN 1.0
                ELSE rb.avg_7d / rb.avg_30d
              END
            * CASE
                WHEN oa.overall_avg IS NULL OR oa.overall_avg = 0 THEN 1.0
                ELSE wp.weekday_avg / oa.overall_avg
              END
            AS DECIMAL(18,2)
        ) AS forecast_total_encounters,
        CAST(
            (
                (
                    (rb.avg_7d * 0.6 + rb.avg_30d * 0.4)
                    * CASE WHEN rb.avg_30d IS NULL OR rb.avg_30d = 0 THEN 1.0 ELSE rb.avg_7d / rb.avg_30d END
                    * CASE WHEN oa.overall_avg IS NULL OR oa.overall_avg = 0 THEN 1.0 ELSE wp.weekday_avg / oa.overall_avg END
                ) - ISNULL(rb.std_30d, 0)
            ) AS DECIMAL(18,2)
        ) AS lower_bound_encounters,
        CAST(
            (
                (
                    (rb.avg_7d * 0.6 + rb.avg_30d * 0.4)
                    * CASE WHEN rb.avg_30d IS NULL OR rb.avg_30d = 0 THEN 1.0 ELSE rb.avg_7d / rb.avg_30d END
                    * CASE WHEN oa.overall_avg IS NULL OR oa.overall_avg = 0 THEN 1.0 ELSE wp.weekday_avg / oa.overall_avg END
                ) + ISNULL(rb.std_30d, 0)
            ) AS DECIMAL(18,2)
        ) AS upper_bound_encounters,
        CAST(rb.avg_7d AS DECIMAL(18,2)),
        CAST(rb.avg_30d AS DECIMAL(18,2)),
        CAST(CASE WHEN rb.avg_30d IS NULL OR rb.avg_30d = 0 THEN 1.0 ELSE rb.avg_7d / rb.avg_30d END AS DECIMAL(18,4)),
        CAST(CASE WHEN oa.overall_avg IS NULL OR oa.overall_avg = 0 THEN 1.0 ELSE wp.weekday_avg / oa.overall_avg END AS DECIMAL(18,4)),
        'Weighted moving average + weekday seasonality'
    FROM future_dates f
    CROSS JOIN recent_base rb
    CROSS JOIN overall_avg oa
    LEFT JOIN weekday_pattern wp
        ON f.weekday_no = wp.weekday_no;
END;
GO

/* =========================================================
   BUILD PRACTICE FORECAST
   ========================================================= */
CREATE OR ALTER PROCEDURE fcst.usp_build_practice_forecast
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE fcst.practice_demand_forecast;

    ;WITH daily AS
    (
        SELECT
            practice_id,
            encounter_date,
            COUNT(*) AS daily_encounters
        FROM dbo.patient_encounter
        GROUP BY practice_id, encounter_date
    ),
    ranked AS
    (
        SELECT
            practice_id,
            encounter_date,
            daily_encounters,
            ROW_NUMBER() OVER (PARTITION BY practice_id ORDER BY encounter_date DESC) AS rn
        FROM daily
    ),
    agg AS
    (
        SELECT
            practice_id,
            AVG(CASE WHEN rn <= 7  THEN daily_encounters * 1.0 END) AS avg_7d,
            AVG(CASE WHEN rn <= 30 THEN daily_encounters * 1.0 END) AS avg_30d
        FROM ranked
        GROUP BY practice_id
    ),
    max_date AS
    (
        SELECT DATEADD(DAY, 1, MAX(encounter_date)) AS forecast_date
        FROM dbo.patient_encounter
    )
    INSERT INTO fcst.practice_demand_forecast
    (
        forecast_date,
        practice_id,
        forecast_total_encounters,
        practice_baseline_avg,
        practice_trend_factor
    )
    SELECT
        m.forecast_date,
        a.practice_id,
        CAST(
            (ISNULL(a.avg_7d, 0) * 0.7 + ISNULL(a.avg_30d, 0) * 0.3)
            * CASE
                WHEN a.avg_30d IS NULL OR a.avg_30d = 0 THEN 1.0
                ELSE a.avg_7d / a.avg_30d
              END
            AS DECIMAL(18,2)
        ),
        CAST(a.avg_30d AS DECIMAL(18,2)),
        CAST(CASE WHEN a.avg_30d IS NULL OR a.avg_30d = 0 THEN 1.0 ELSE a.avg_7d / a.avg_30d END AS DECIMAL(18,4))
    FROM agg a
    CROSS JOIN max_date m;
END;
GO

/* =========================================================
   WRAPPER
   ========================================================= */
CREATE OR ALTER PROCEDURE fcst.usp_refresh_forecasting_layer
AS
BEGIN
    SET NOCOUNT ON;

    EXEC fcst.usp_build_daily_forecast;
    EXEC fcst.usp_build_practice_forecast;
END;
GO

/* =========================================================
   VIEWS
   ========================================================= */
CREATE OR ALTER VIEW fcst.vw_forecast_dashboard
AS
SELECT
    forecast_date,
    forecast_horizon_days,
    forecast_total_encounters,
    lower_bound_encounters,
    upper_bound_encounters,
    baseline_7d_avg,
    baseline_30d_avg,
    recent_trend_factor,
    weekday_seasonality_factor,
    forecast_method
FROM fcst.daily_demand_forecast;
GO

CREATE OR ALTER VIEW fcst.vw_practice_forecast
AS
SELECT
    forecast_date,
    practice_id,
    forecast_total_encounters,
    practice_baseline_avg,
    practice_trend_factor,
    DENSE_RANK() OVER (PARTITION BY forecast_date ORDER BY forecast_total_encounters DESC) AS forecast_rank
FROM fcst.practice_demand_forecast;
GO

/* =========================================================
   SCENARIO MODELLING LAYER
   ========================================================= */

USE PortfolioHealthcareETL;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'scn')
    EXEC('CREATE SCHEMA scn');
GO

IF OBJECT_ID('scn.usp_refresh_scenarios', 'P') IS NOT NULL DROP PROCEDURE scn.usp_refresh_scenarios;
GO
IF OBJECT_ID('scn.vw_scenario_summary', 'V') IS NOT NULL DROP VIEW scn.vw_scenario_summary;
GO
IF OBJECT_ID('scn.vw_scenario_practice_pressure', 'V') IS NOT NULL DROP VIEW scn.vw_scenario_practice_pressure;
GO
IF OBJECT_ID('scn.practice_scenario_impact', 'U') IS NOT NULL DROP TABLE scn.practice_scenario_impact;
GO
IF OBJECT_ID('scn.scenario_summary', 'U') IS NOT NULL DROP TABLE scn.scenario_summary;
GO
IF OBJECT_ID('scn.scenario_config', 'U') IS NOT NULL DROP TABLE scn.scenario_config;
GO

CREATE TABLE scn.scenario_config
(
    scenario_id                    INT IDENTITY(1,1) PRIMARY KEY,
    scenario_name                  VARCHAR(200) NOT NULL,
    demand_multiplier              DECIMAL(18,4) NOT NULL DEFAULT 1.0000,
    cost_multiplier                DECIMAL(18,4) NOT NULL DEFAULT 1.0000,
    clinician_capacity_multiplier  DECIMAL(18,4) NOT NULL DEFAULT 1.0000,
    encounter_mix_multiplier       DECIMAL(18,4) NOT NULL DEFAULT 1.0000,
    is_active                      BIT NOT NULL DEFAULT 1,
    created_datetime               DATETIME2(0) NOT NULL DEFAULT SYSDATETIME()
);
GO

CREATE TABLE scn.scenario_summary
(
    scenario_id                    INT NOT NULL,
    scenario_name                  VARCHAR(200) NOT NULL,
    projected_total_encounters     DECIMAL(18,2) NOT NULL,
    projected_total_cost           DECIMAL(18,2) NOT NULL,
    projected_cost_per_encounter   DECIMAL(18,2) NOT NULL,
    projected_capacity_pressure    DECIMAL(18,4) NOT NULL,
    projected_high_strain_practices INT NOT NULL,
    created_datetime               DATETIME2(0) NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_scn_scenario_summary PRIMARY KEY (scenario_id)
);
GO

CREATE TABLE scn.practice_scenario_impact
(
    scenario_id                    INT NOT NULL,
    practice_id                    INT NOT NULL,
    projected_daily_load           DECIMAL(18,2) NOT NULL,
    projected_daily_cost           DECIMAL(18,2) NOT NULL,
    projected_load_vs_baseline_pct DECIMAL(18,4) NULL,
    projected_pressure_level       VARCHAR(50) NOT NULL,
    created_datetime               DATETIME2(0) NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_scn_practice_scenario_impact PRIMARY KEY (scenario_id, practice_id)
);
GO

/* Seed example scenarios */
INSERT INTO scn.scenario_config
(
    scenario_name,
    demand_multiplier,
    cost_multiplier,
    clinician_capacity_multiplier,
    encounter_mix_multiplier
)
VALUES
('Baseline Scenario',                  1.00, 1.00, 1.00, 1.00),
('Moderate Winter Pressure',           1.15, 1.05, 0.95, 1.05),
('Severe Demand Surge',                1.30, 1.10, 0.90, 1.10),
('Cost Inflation Scenario',            1.00, 1.20, 1.00, 1.00),
('Operational Recovery Scenario',      0.95, 0.98, 1.10, 0.98);
GO

CREATE OR ALTER PROCEDURE scn.usp_refresh_scenarios
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE scn.scenario_summary;
    TRUNCATE TABLE scn.practice_scenario_impact;

    ;WITH baseline AS
    (
        SELECT
            COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT encounter_date), 0) AS avg_daily_encounters,
            SUM(cost) * 1.0 / NULLIF(COUNT(DISTINCT encounter_date), 0) AS avg_daily_cost
        FROM dbo.patient_encounter
    ),
    practice_base AS
    (
        SELECT
            practice_id,
            COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT encounter_date), 0) AS avg_daily_load,
            SUM(cost) * 1.0 / NULLIF(COUNT(DISTINCT encounter_date), 0) AS avg_daily_cost
        FROM dbo.patient_encounter
        GROUP BY practice_id
    )
    INSERT INTO scn.practice_scenario_impact
    (
        scenario_id,
        practice_id,
        projected_daily_load,
        projected_daily_cost,
        projected_load_vs_baseline_pct,
        projected_pressure_level
    )
    SELECT
        s.scenario_id,
        p.practice_id,
        CAST(p.avg_daily_load * s.demand_multiplier / NULLIF(s.clinician_capacity_multiplier, 0) AS DECIMAL(18,2)),
        CAST(p.avg_daily_cost * s.demand_multiplier * s.cost_multiplier AS DECIMAL(18,2)),
        CAST(
            (
                (p.avg_daily_load * s.demand_multiplier / NULLIF(s.clinician_capacity_multiplier, 0))
                - p.avg_daily_load
            ) / NULLIF(p.avg_daily_load, 0)
            AS DECIMAL(18,4)
        ),
        CASE
            WHEN p.avg_daily_load * s.demand_multiplier / NULLIF(s.clinician_capacity_multiplier, 0) >= p.avg_daily_load * 1.30 THEN 'HIGH_PRESSURE'
            WHEN p.avg_daily_load * s.demand_multiplier / NULLIF(s.clinician_capacity_multiplier, 0) >= p.avg_daily_load * 1.10 THEN 'MODERATE_PRESSURE'
            ELSE 'STABLE'
        END
    FROM practice_base p
    CROSS JOIN scn.scenario_config s
    WHERE s.is_active = 1;

    INSERT INTO scn.scenario_summary
    (
        scenario_id,
        scenario_name,
        projected_total_encounters,
        projected_total_cost,
        projected_cost_per_encounter,
        projected_capacity_pressure,
        projected_high_strain_practices
    )
    SELECT
        s.scenario_id,
        s.scenario_name,
        CAST(b.avg_daily_encounters * s.demand_multiplier / NULLIF(s.clinician_capacity_multiplier, 0) AS DECIMAL(18,2)),
        CAST(b.avg_daily_cost * s.demand_multiplier * s.cost_multiplier AS DECIMAL(18,2)),
        CAST(
            (b.avg_daily_cost * s.demand_multiplier * s.cost_multiplier)
            / NULLIF((b.avg_daily_encounters * s.demand_multiplier / NULLIF(s.clinician_capacity_multiplier, 0)), 0)
            AS DECIMAL(18,2)
        ),
        CAST(s.demand_multiplier / NULLIF(s.clinician_capacity_multiplier, 0) AS DECIMAL(18,4)),
        (
            SELECT COUNT(*)
            FROM scn.practice_scenario_impact i
            WHERE i.scenario_id = s.scenario_id
              AND i.projected_pressure_level = 'HIGH_PRESSURE'
        )
    FROM baseline b
    CROSS JOIN scn.scenario_config s
    WHERE s.is_active = 1;
END;
GO

CREATE OR ALTER VIEW scn.vw_scenario_summary
AS
SELECT
    scenario_id,
    scenario_name,
    projected_total_encounters,
    projected_total_cost,
    projected_cost_per_encounter,
    projected_capacity_pressure,
    projected_high_strain_practices
FROM scn.scenario_summary;
GO

CREATE OR ALTER VIEW scn.vw_scenario_practice_pressure
AS
SELECT
    scenario_id,
    practice_id,
    projected_daily_load,
    projected_daily_cost,
    projected_load_vs_baseline_pct,
    projected_pressure_level,
    DENSE_RANK() OVER (PARTITION BY scenario_id ORDER BY projected_daily_load DESC) AS projected_rank_by_load
FROM scn.practice_scenario_impact;
GO
