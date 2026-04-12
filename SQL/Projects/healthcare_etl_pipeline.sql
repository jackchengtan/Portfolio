/*
Project: Daily Healthcare Encounter ETL Pipeline
Platform: Microsoft SQL Server
Purpose:
    This script creates a portfolio-safe healthcare-inspired ETL solution that:
    1) Creates a new database
    2) Creates schemas, tables, indexes, and helper objects
    3) Loads a daily CSV file into a raw staging table
    4) Validates and deduplicates rows
    5) Upserts valid rows into a curated reporting table
    6) Logs ETL activity and errors
    7) Provides a wrapper procedure for daily execution
    8) Includes SQL Agent job creation scripts

Important notes:
    - All data is synthetic and intended for portfolio/demo use only.
    - File paths should be changed to match your local machine.
    - BULK INSERT requires the SQL Server service account to have folder access.
    - xp_cmdshell is not required for this version.
*/

/* =========================================================
   1. CREATE DATABASE
   ========================================================= */

USE master;
GO

IF DB_ID('PortfolioHealthcareETL') IS NULL
BEGIN
    CREATE DATABASE PortfolioHealthcareETL;
END;
GO

USE PortfolioHealthcareETL;
GO

/* =========================================================
   2. CREATE SCHEMAS
   ========================================================= */

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'stg')
    EXEC('CREATE SCHEMA stg');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'etl')
    EXEC('CREATE SCHEMA etl');
GO

/* =========================================================
   3. DROP OBJECTS IF RE-RUNNING SCRIPT
   This makes development easier while testing.
   ========================================================= */

IF OBJECT_ID('etl.usp_run_today_patient_encounter_etl', 'P') IS NOT NULL
    DROP PROCEDURE etl.usp_run_today_patient_encounter_etl;
GO

IF OBJECT_ID('etl.usp_run_daily_patient_encounter_etl', 'P') IS NOT NULL
    DROP PROCEDURE etl.usp_run_daily_patient_encounter_etl;
GO

IF OBJECT_ID('etl.usp_upsert_patient_encounter', 'P') IS NOT NULL
    DROP PROCEDURE etl.usp_upsert_patient_encounter;
GO

IF OBJECT_ID('etl.usp_validate_patient_encounter', 'P') IS NOT NULL
    DROP PROCEDURE etl.usp_validate_patient_encounter;
GO

IF OBJECT_ID('etl.usp_load_patient_encounter_raw', 'P') IS NOT NULL
    DROP PROCEDURE etl.usp_load_patient_encounter_raw;
GO

IF OBJECT_ID('etl.usp_write_etl_log', 'P') IS NOT NULL
    DROP PROCEDURE etl.usp_write_etl_log;
GO

IF OBJECT_ID('etl.file_load_audit', 'U') IS NOT NULL
    DROP TABLE etl.file_load_audit;
GO

IF OBJECT_ID('etl.etl_run_log', 'U') IS NOT NULL
    DROP TABLE etl.etl_run_log;
GO

IF OBJECT_ID('etl.patient_encounter_error', 'U') IS NOT NULL
    DROP TABLE etl.patient_encounter_error;
GO

IF OBJECT_ID('stg.patient_encounter_valid', 'U') IS NOT NULL
    DROP TABLE stg.patient_encounter_valid;
GO

IF OBJECT_ID('stg.patient_encounter_raw', 'U') IS NOT NULL
    DROP TABLE stg.patient_encounter_raw;
GO

IF OBJECT_ID('dbo.patient_encounter', 'U') IS NOT NULL
    DROP TABLE dbo.patient_encounter;
GO

IF OBJECT_ID('dbo.dim_encounter_type', 'U') IS NOT NULL
    DROP TABLE dbo.dim_encounter_type;
GO

IF OBJECT_ID('dbo.dim_practice', 'U') IS NOT NULL
    DROP TABLE dbo.dim_practice;
GO

/* =========================================================
   4. DIMENSION TABLES
   These make the project look more warehouse/healthcare-focused.
   ========================================================= */

CREATE TABLE dbo.dim_practice
(
    practice_id           INT            NOT NULL PRIMARY KEY,
    practice_name         VARCHAR(200)   NOT NULL,
    region_name           VARCHAR(100)   NULL,
    is_active             BIT            NOT NULL DEFAULT (1),
    created_datetime      DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME()
);
GO

CREATE TABLE dbo.dim_encounter_type
(
    encounter_type_code   VARCHAR(20)    NOT NULL PRIMARY KEY,
    encounter_type_name   VARCHAR(100)   NOT NULL,
    created_datetime      DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME()
);
GO

INSERT INTO dbo.dim_practice
(
    practice_id,
    practice_name,
    region_name
)
VALUES
(301, 'Northside Medical Centre', 'North'),
(302, 'Riverbank Surgery', 'South'),
(303, 'Westfield Family Practice', 'Central');
GO

INSERT INTO dbo.dim_encounter_type
(
    encounter_type_code,
    encounter_type_name
)
VALUES
('GP', 'GP Consultation'),
('NURSE', 'Nurse Review'),
('MEDREV', 'Medication Review'),
('FOLLOWUP', 'Follow-up Consultation');
GO

/* =========================================================
   5. FINAL CURATED TABLE
   This is the reporting-ready target table.
   ========================================================= */

CREATE TABLE dbo.patient_encounter
(
    encounter_sk          INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    encounter_id          INT            NOT NULL,
    patient_id            INT            NOT NULL,
    practice_id           INT            NOT NULL,
    encounter_date        DATE           NOT NULL,
    encounter_type_code   VARCHAR(20)    NOT NULL,
    encounter_type_name   VARCHAR(100)   NOT NULL,
    clinician_id          INT            NOT NULL,
    diagnosis_code        VARCHAR(20)    NULL,
    cost                  DECIMAL(12,2)  NOT NULL,
    source_file_date      DATE           NOT NULL,
    source_file_name      VARCHAR(260)   NOT NULL,
    created_datetime      DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
    updated_datetime      DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME()
);
GO

/*
Business key:
    We assume a single encounter should be uniquely identifiable by:
    - encounter_id
    - patient_id
    - practice_id
    - encounter_date
    - encounter_type_code
    - clinician_id
*/
CREATE UNIQUE INDEX UX_patient_encounter_business_key
ON dbo.patient_encounter
(
    encounter_id,
    patient_id,
    practice_id,
    encounter_date,
    encounter_type_code,
    clinician_id
);
GO

/* =========================================================
   6. RAW STAGING TABLE
   Store CSV as strings first. This is safer and realistic.
   ========================================================= */

CREATE TABLE stg.patient_encounter_raw
(
    raw_load_id           BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    encounter_id          VARCHAR(100)  NULL,
    patient_id            VARCHAR(100)  NULL,
    practice_id           VARCHAR(100)  NULL,
    encounter_date        VARCHAR(100)  NULL,
    encounter_type_code   VARCHAR(20)   NULL,
    clinician_id          VARCHAR(100)  NULL,
    diagnosis_code        VARCHAR(20)   NULL,
    cost                  VARCHAR(100)  NULL,
    source_file_date      VARCHAR(100)  NULL,
    source_file_name      VARCHAR(260)  NOT NULL,
    load_datetime         DATETIME2(0)  NOT NULL DEFAULT SYSDATETIME()
);
GO

/* =========================================================
   7. VALID STAGING TABLE
   Clean, typed, deduplicated records land here before upsert.
   ========================================================= */

CREATE TABLE stg.patient_encounter_valid
(
    valid_load_id         BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    encounter_id          INT            NOT NULL,
    patient_id            INT            NOT NULL,
    practice_id           INT            NOT NULL,
    encounter_date        DATE           NOT NULL,
    encounter_type_code   VARCHAR(20)    NOT NULL,
    encounter_type_name   VARCHAR(100)   NOT NULL,
    clinician_id          INT            NOT NULL,
    diagnosis_code        VARCHAR(20)    NULL,
    cost                  DECIMAL(12,2)  NOT NULL,
    source_file_date      DATE           NOT NULL,
    source_file_name      VARCHAR(260)   NOT NULL,
    load_datetime         DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME()
);
GO

/* =========================================================
   8. ERROR TABLE
   Invalid rows are stored here for investigation.
   ========================================================= */

CREATE TABLE etl.patient_encounter_error
(
    error_id              BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    encounter_id          VARCHAR(100)  NULL,
    patient_id            VARCHAR(100)  NULL,
    practice_id           VARCHAR(100)  NULL,
    encounter_date        VARCHAR(100)  NULL,
    encounter_type_code   VARCHAR(20)   NULL,
    clinician_id          VARCHAR(100)  NULL,
    diagnosis_code        VARCHAR(20)   NULL,
    cost                  VARCHAR(100)  NULL,
    source_file_date      VARCHAR(100)  NULL,
    source_file_name      VARCHAR(260)  NOT NULL,
    error_reason          VARCHAR(500)  NOT NULL,
    error_datetime        DATETIME2(0)  NOT NULL DEFAULT SYSDATETIME()
);
GO

/* =========================================================
   9. ETL RUN LOG
   Tracks ETL progress by step and outcome.
   ========================================================= */

CREATE TABLE etl.etl_run_log
(
    etl_run_id            BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    process_name          VARCHAR(200)   NOT NULL,
    source_file_name      VARCHAR(260)   NULL,
    step_name             VARCHAR(200)   NOT NULL,
    start_datetime        DATETIME2(0)   NOT NULL,
    end_datetime          DATETIME2(0)   NULL,
    rows_processed        INT            NULL,
    rows_inserted         INT            NULL,
    rows_updated          INT            NULL,
    rows_rejected         INT            NULL,
    status                VARCHAR(20)    NOT NULL,
    error_message         VARCHAR(MAX)   NULL
);
GO

/* =========================================================
   10. FILE LOAD AUDIT
   Prevents accidental duplicate file loading.
   ========================================================= */

CREATE TABLE etl.file_load_audit
(
    file_audit_id         BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    source_file_name      VARCHAR(260)   NOT NULL,
    file_path             VARCHAR(4000)  NOT NULL,
    load_status           VARCHAR(20)    NOT NULL,
    load_datetime         DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
    notes                 VARCHAR(1000)  NULL
);
GO

/* =========================================================
   11. HELPER LOGGING PROCEDURE
   ========================================================= */

CREATE PROCEDURE etl.usp_write_etl_log
(
    @process_name      VARCHAR(200),
    @source_file_name  VARCHAR(260) = NULL,
    @step_name         VARCHAR(200),
    @start_datetime    DATETIME2(0),
    @end_datetime      DATETIME2(0) = NULL,
    @rows_processed    INT = NULL,
    @rows_inserted     INT = NULL,
    @rows_updated      INT = NULL,
    @rows_rejected     INT = NULL,
    @status            VARCHAR(20),
    @error_message     VARCHAR(MAX) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO etl.etl_run_log
    (
        process_name,
        source_file_name,
        step_name,
        start_datetime,
        end_datetime,
        rows_processed,
        rows_inserted,
        rows_updated,
        rows_rejected,
        status,
        error_message
    )
    VALUES
    (
        @process_name,
        @source_file_name,
        @step_name,
        @start_datetime,
        @end_datetime,
        @rows_processed,
        @rows_inserted,
        @rows_updated,
        @rows_rejected,
        @status,
        @error_message
    );
END;
GO

/* =========================================================
   12. RAW LOAD PROCEDURE
   Loads a CSV file into a temp table with BULK INSERT,
   then appends the source file name before writing to staging.

   Expected CSV structure:
   encounter_id,patient_id,practice_id,encounter_date,encounter_type_code,
   clinician_id,diagnosis_code,cost,source_file_date
   ========================================================= */

CREATE PROCEDURE etl.usp_load_patient_encounter_raw
(
    @file_path        VARCHAR(4000),
    @source_file_name VARCHAR(260)
)
AS
BEGIN
    SET NOCOUNT ON;

    CREATE TABLE #raw_import
    (
        encounter_id          VARCHAR(100)  NULL,
        patient_id            VARCHAR(100)  NULL,
        practice_id           VARCHAR(100)  NULL,
        encounter_date        VARCHAR(100)  NULL,
        encounter_type_code   VARCHAR(20)   NULL,
        clinician_id          VARCHAR(100)  NULL,
        diagnosis_code        VARCHAR(20)   NULL,
        cost                  VARCHAR(100)  NULL,
        source_file_date      VARCHAR(100)  NULL
    );

    DECLARE @sql NVARCHAR(MAX);

    /*
    Notes:
        - FIRSTROW = 2 skips the header row.
        - FIELDTERMINATOR assumes comma-delimited CSV.
        - ROWTERMINATOR may need changing depending on file format:
            * Windows CSV often works with ''0x0d0a''
            * Unix/Linux often works with ''0x0a''
        - CODEPAGE 65001 supports UTF-8.
    */
    SET @sql = N'
        BULK INSERT #raw_import
        FROM ''' + REPLACE(@file_path, '''', '''''') + '''
        WITH
        (
            FIRSTROW = 2,
            FIELDTERMINATOR = '','',
            ROWTERMINATOR = ''0x0a'',
            TABLOCK,
            CODEPAGE = ''65001''
        );';

    EXEC sys.sp_executesql @sql;

    INSERT INTO stg.patient_encounter_raw
    (
        encounter_id,
        patient_id,
        practice_id,
        encounter_date,
        encounter_type_code,
        clinician_id,
        diagnosis_code,
        cost,
        source_file_date,
        source_file_name
    )
    SELECT
        encounter_id,
        patient_id,
        practice_id,
        encounter_date,
        encounter_type_code,
        clinician_id,
        diagnosis_code,
        cost,
        source_file_date,
        @source_file_name
    FROM #raw_import;
END;
GO

/* =========================================================
   13. VALIDATION PROCEDURE
   Rules:
       - required numeric identifiers must parse correctly
       - encounter date must be valid
       - source file date must be valid
       - encounter type code cannot be blank
       - encounter type code must exist in dim_encounter_type
       - cost must be numeric and >= 0
       - encounter_date cannot be after source_file_date
   Also performs within-file deduplication using ROW_NUMBER().
   ========================================================= */

CREATE PROCEDURE etl.usp_validate_patient_encounter
(
    @source_file_name VARCHAR(260)
)
AS
BEGIN
    SET NOCOUNT ON;

    ;WITH base AS
    (
        SELECT
            r.raw_load_id,
            r.encounter_id,
            r.patient_id,
            r.practice_id,
            r.encounter_date,
            r.encounter_type_code,
            r.clinician_id,
            r.diagnosis_code,
            r.cost,
            r.source_file_date,
            r.source_file_name,
            CASE
                WHEN TRY_CAST(r.encounter_id AS INT) IS NULL THEN 'Invalid encounter_id'
                WHEN TRY_CAST(r.patient_id AS INT) IS NULL THEN 'Invalid patient_id'
                WHEN TRY_CAST(r.practice_id AS INT) IS NULL THEN 'Invalid practice_id'
                WHEN TRY_CAST(r.encounter_date AS DATE) IS NULL THEN 'Invalid encounter_date'
                WHEN ISNULL(LTRIM(RTRIM(r.encounter_type_code)), '') = '' THEN 'Missing encounter_type_code'
                WHEN NOT EXISTS
                (
                    SELECT 1
                    FROM dbo.dim_encounter_type d
                    WHERE d.encounter_type_code = LTRIM(RTRIM(r.encounter_type_code))
                ) THEN 'Unknown encounter_type_code'
                WHEN TRY_CAST(r.clinician_id AS INT) IS NULL THEN 'Invalid clinician_id'
                WHEN TRY_CAST(r.cost AS DECIMAL(12,2)) IS NULL THEN 'Invalid cost'
                WHEN TRY_CAST(r.cost AS DECIMAL(12,2)) < 0 THEN 'Negative cost not allowed'
                WHEN TRY_CAST(r.source_file_date AS DATE) IS NULL THEN 'Invalid source_file_date'
                WHEN TRY_CAST(r.encounter_date AS DATE) > TRY_CAST(r.source_file_date AS DATE) THEN 'Encounter date after source file date'
                ELSE NULL
            END AS error_reason
        FROM stg.patient_encounter_raw r
        WHERE r.source_file_name = @source_file_name
    )
    INSERT INTO etl.patient_encounter_error
    (
        encounter_id,
        patient_id,
        practice_id,
        encounter_date,
        encounter_type_code,
        clinician_id,
        diagnosis_code,
        cost,
        source_file_date,
        source_file_name,
        error_reason
    )
    SELECT
        encounter_id,
        patient_id,
        practice_id,
        encounter_date,
        encounter_type_code,
        clinician_id,
        diagnosis_code,
        cost,
        source_file_date,
        source_file_name,
        error_reason
    FROM base
    WHERE error_reason IS NOT NULL;

    ;WITH good_rows AS
    (
        SELECT
            TRY_CAST(r.encounter_id AS INT) AS encounter_id,
            TRY_CAST(r.patient_id AS INT) AS patient_id,
            TRY_CAST(r.practice_id AS INT) AS practice_id,
            TRY_CAST(r.encounter_date AS DATE) AS encounter_date,
            LTRIM(RTRIM(r.encounter_type_code)) AS encounter_type_code,
            d.encounter_type_name,
            TRY_CAST(r.clinician_id AS INT) AS clinician_id,
            NULLIF(LTRIM(RTRIM(r.diagnosis_code)), '') AS diagnosis_code,
            TRY_CAST(r.cost AS DECIMAL(12,2)) AS cost,
            TRY_CAST(r.source_file_date AS DATE) AS source_file_date,
            r.source_file_name,
            ROW_NUMBER() OVER
            (
                PARTITION BY
                    TRY_CAST(r.encounter_id AS INT),
                    TRY_CAST(r.patient_id AS INT),
                    TRY_CAST(r.practice_id AS INT),
                    TRY_CAST(r.encounter_date AS DATE),
                    LTRIM(RTRIM(r.encounter_type_code)),
                    TRY_CAST(r.clinician_id AS INT)
                ORDER BY r.raw_load_id
            ) AS rn
        FROM stg.patient_encounter_raw r
        INNER JOIN dbo.dim_encounter_type d
            ON d.encounter_type_code = LTRIM(RTRIM(r.encounter_type_code))
        WHERE r.source_file_name = @source_file_name
          AND TRY_CAST(r.encounter_id AS INT) IS NOT NULL
          AND TRY_CAST(r.patient_id AS INT) IS NOT NULL
          AND TRY_CAST(r.practice_id AS INT) IS NOT NULL
          AND TRY_CAST(r.encounter_date AS DATE) IS NOT NULL
          AND ISNULL(LTRIM(RTRIM(r.encounter_type_code)), '') <> ''
          AND TRY_CAST(r.clinician_id AS INT) IS NOT NULL
          AND TRY_CAST(r.cost AS DECIMAL(12,2)) IS NOT NULL
          AND TRY_CAST(r.cost AS DECIMAL(12,2)) >= 0
          AND TRY_CAST(r.source_file_date AS DATE) IS NOT NULL
          AND TRY_CAST(r.encounter_date AS DATE) <= TRY_CAST(r.source_file_date AS DATE)
    )
    INSERT INTO stg.patient_encounter_valid
    (
        encounter_id,
        patient_id,
        practice_id,
        encounter_date,
        encounter_type_code,
        encounter_type_name,
        clinician_id,
        diagnosis_code,
        cost,
        source_file_date,
        source_file_name
    )
    SELECT
        encounter_id,
        patient_id,
        practice_id,
        encounter_date,
        encounter_type_code,
        encounter_type_name,
        clinician_id,
        diagnosis_code,
        cost,
        source_file_date,
        source_file_name
    FROM good_rows
    WHERE rn = 1;
END;
GO

/* =========================================================
   14. UPSERT PROCEDURE
   Updates existing records when non-key details differ.
   Inserts records that do not already exist.
   ========================================================= */

CREATE PROCEDURE etl.usp_upsert_patient_encounter
(
    @source_file_name VARCHAR(260)
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @rows_inserted INT = 0;
    DECLARE @rows_updated  INT = 0;

    /*
    Update existing target rows if any non-key attribute changed.
    */
    UPDATE tgt
    SET
        tgt.encounter_type_name = src.encounter_type_name,
        tgt.diagnosis_code = src.diagnosis_code,
        tgt.cost = src.cost,
        tgt.source_file_date = src.source_file_date,
        tgt.source_file_name = src.source_file_name,
        tgt.updated_datetime = SYSDATETIME()
    FROM dbo.patient_encounter tgt
    INNER JOIN stg.patient_encounter_valid src
        ON tgt.encounter_id = src.encounter_id
       AND tgt.patient_id = src.patient_id
       AND tgt.practice_id = src.practice_id
       AND tgt.encounter_date = src.encounter_date
       AND tgt.encounter_type_code = src.encounter_type_code
       AND tgt.clinician_id = src.clinician_id
    WHERE src.source_file_name = @source_file_name
      AND
      (
           ISNULL(tgt.encounter_type_name, '') <> ISNULL(src.encounter_type_name, '')
        OR ISNULL(tgt.diagnosis_code, '') <> ISNULL(src.diagnosis_code, '')
        OR tgt.cost <> src.cost
        OR tgt.source_file_date <> src.source_file_date
        OR tgt.source_file_name <> src.source_file_name
      );

    SET @rows_updated = @@ROWCOUNT;

    /*
    Insert new business-key combinations.
    */
    INSERT INTO dbo.patient_encounter
    (
        encounter_id,
        patient_id,
        practice_id,
        encounter_date,
        encounter_type_code,
        encounter_type_name,
        clinician_id,
        diagnosis_code,
        cost,
        source_file_date,
        source_file_name
    )
    SELECT
        src.encounter_id,
        src.patient_id,
        src.practice_id,
        src.encounter_date,
        src.encounter_type_code,
        src.encounter_type_name,
        src.clinician_id,
        src.diagnosis_code,
        src.cost,
        src.source_file_date,
        src.source_file_name
    FROM stg.patient_encounter_valid src
    WHERE src.source_file_name = @source_file_name
      AND NOT EXISTS
      (
          SELECT 1
          FROM dbo.patient_encounter tgt
          WHERE tgt.encounter_id = src.encounter_id
            AND tgt.patient_id = src.patient_id
            AND tgt.practice_id = src.practice_id
            AND tgt.encounter_date = src.encounter_date
            AND tgt.encounter_type_code = src.encounter_type_code
            AND tgt.clinician_id = src.clinician_id
      );

    SET @rows_inserted = @@ROWCOUNT;

    SELECT
        @rows_inserted AS rows_inserted,
        @rows_updated AS rows_updated;
END;
GO

/* =========================================================
   15. MAIN DAILY ETL PROCEDURE
   This is the core procedure that:
       - checks whether the file has already been loaded
       - clears prior staging rows for this file
       - loads raw CSV
       - validates rows
       - upserts target
       - writes audit and log entries
   ========================================================= */

CREATE PROCEDURE etl.usp_run_daily_patient_encounter_etl
(
    @file_path        VARCHAR(4000),
    @source_file_name VARCHAR(260)
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @process_name   VARCHAR(200) = 'patient_encounter_daily_etl';
    DECLARE @start_datetime DATETIME2(0) = SYSDATETIME();
    DECLARE @step_start     DATETIME2(0);
    DECLARE @rows_raw       INT = 0;
    DECLARE @rows_valid     INT = 0;
    DECLARE @rows_rejected  INT = 0;
    DECLARE @rows_inserted  INT = 0;
    DECLARE @rows_updated   INT = 0;

    BEGIN TRY
        /*
        Prevent duplicate successful processing of the same file.
        */
        IF EXISTS
        (
            SELECT 1
            FROM etl.file_load_audit
            WHERE source_file_name = @source_file_name
              AND load_status = 'SUCCESS'
        )
        BEGIN
            RAISERROR('This source file has already been loaded successfully.', 16, 1);
        END;

        INSERT INTO etl.file_load_audit
        (
            source_file_name,
            file_path,
            load_status,
            notes
        )
        VALUES
        (
            @source_file_name,
            @file_path,
            'STARTED',
            'ETL run started'
        );

        /*
        Clear any prior data for this same file name from staging/error tables.
        */
        DELETE FROM stg.patient_encounter_raw
        WHERE source_file_name = @source_file_name;

        DELETE FROM stg.patient_encounter_valid
        WHERE source_file_name = @source_file_name;

        DELETE FROM etl.patient_encounter_error
        WHERE source_file_name = @source_file_name;

        /* -------------------------
           Step 1: Load raw CSV
           ------------------------- */
        SET @step_start = SYSDATETIME();

        EXEC etl.usp_load_patient_encounter_raw
            @file_path = @file_path,
            @source_file_name = @source_file_name;

        SELECT @rows_raw = COUNT(*)
        FROM stg.patient_encounter_raw
        WHERE source_file_name = @source_file_name;

        EXEC etl.usp_write_etl_log
            @process_name = @process_name,
            @source_file_name = @source_file_name,
            @step_name = 'Load raw CSV',
            @start_datetime = @step_start,
            @end_datetime = SYSDATETIME(),
            @rows_processed = @rows_raw,
            @status = 'SUCCESS';

        /* -------------------------
           Step 2: Validate rows
           ------------------------- */
        SET @step_start = SYSDATETIME();

        EXEC etl.usp_validate_patient_encounter
            @source_file_name = @source_file_name;

        SELECT @rows_valid = COUNT(*)
        FROM stg.patient_encounter_valid
        WHERE source_file_name = @source_file_name;

        SELECT @rows_rejected = COUNT(*)
        FROM etl.patient_encounter_error
        WHERE source_file_name = @source_file_name;

        EXEC etl.usp_write_etl_log
            @process_name = @process_name,
            @source_file_name = @source_file_name,
            @step_name = 'Validate and deduplicate',
            @start_datetime = @step_start,
            @end_datetime = SYSDATETIME(),
            @rows_processed = @rows_raw,
            @rows_rejected = @rows_rejected,
            @status = 'SUCCESS';

        /* -------------------------
           Step 3: Upsert target
           ------------------------- */
        SET @step_start = SYSDATETIME();

        DECLARE @upsert_result TABLE
        (
            rows_inserted INT,
            rows_updated  INT
        );

        INSERT INTO @upsert_result
        EXEC etl.usp_upsert_patient_encounter
            @source_file_name = @source_file_name;

        SELECT
            @rows_inserted = rows_inserted,
            @rows_updated = rows_updated
        FROM @upsert_result;

        EXEC etl.usp_write_etl_log
            @process_name = @process_name,
            @source_file_name = @source_file_name,
            @step_name = 'Upsert curated table',
            @start_datetime = @step_start,
            @end_datetime = SYSDATETIME(),
            @rows_processed = @rows_valid,
            @rows_inserted = @rows_inserted,
            @rows_updated = @rows_updated,
            @status = 'SUCCESS';

        /* -------------------------
           Step 4: Mark overall success
           ------------------------- */
        EXEC etl.usp_write_etl_log
            @process_name = @process_name,
            @source_file_name = @source_file_name,
            @step_name = 'ETL complete',
            @start_datetime = @start_datetime,
            @end_datetime = SYSDATETIME(),
            @rows_processed = @rows_raw,
            @rows_inserted = @rows_inserted,
            @rows_updated = @rows_updated,
            @rows_rejected = @rows_rejected,
            @status = 'SUCCESS';

        UPDATE etl.file_load_audit
        SET
            load_status = 'SUCCESS',
            notes = CONCAT('ETL completed. Raw=', @rows_raw, ', Valid=', @rows_valid,
                           ', Inserted=', @rows_inserted, ', Updated=', @rows_updated,
                           ', Rejected=', @rows_rejected)
        WHERE source_file_name = @source_file_name
          AND load_status = 'STARTED';
    END TRY
    BEGIN CATCH
        EXEC etl.usp_write_etl_log
            @process_name = @process_name,
            @source_file_name = @source_file_name,
            @step_name = 'ETL failed',
            @start_datetime = @start_datetime,
            @end_datetime = SYSDATETIME(),
            @status = 'FAILED',
            @error_message = ERROR_MESSAGE();

        UPDATE etl.file_load_audit
        SET
            load_status = 'FAILED',
            notes = ERROR_MESSAGE()
        WHERE source_file_name = @source_file_name
          AND load_status = 'STARTED';

        THROW;
    END CATCH
END;
GO

/* =========================================================
   16. TODAY WRAPPER PROCEDURE
   Builds a file name in the format:
       patient_encounter_YYYYMMDD.csv
   Example:
       patient_encounter_20260330.csv
   ========================================================= */

CREATE PROCEDURE etl.usp_run_today_patient_encounter_etl
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @yyyymmdd CHAR(8) = CONVERT(CHAR(8), GETDATE(), 112);
    DECLARE @source_file_name VARCHAR(260) = 'patient_encounter_' + @yyyymmdd + '.csv';
    DECLARE @file_path VARCHAR(4000) = 'C:\ETL\Inbound\' + @source_file_name;

    EXEC etl.usp_run_daily_patient_encounter_etl
        @file_path = @file_path,
        @source_file_name = @source_file_name;
END;
GO

/* =========================================================
   17. SAMPLE MANUAL EXECUTION
   Use this to test the pipeline before scheduling.
   Change the path to match your machine.
   ========================================================= */

-- Example manual run:
-- EXEC etl.usp_run_daily_patient_encounter_etl
--     @file_path = 'C:\ETL\Inbound\patient_encounter_20260330.csv',
--     @source_file_name = 'patient_encounter_20260330.csv';
-- GO

/* =========================================================
   18. SAMPLE CHECK QUERIES
   Run these after a test load.
   ========================================================= */

-- SELECT * FROM dbo.patient_encounter ORDER BY encounter_sk;
-- SELECT * FROM stg.patient_encounter_raw ORDER BY raw_load_id;
-- SELECT * FROM stg.patient_encounter_valid ORDER BY valid_load_id;
-- SELECT * FROM etl.patient_encounter_error ORDER BY error_id;
-- SELECT * FROM etl.etl_run_log ORDER BY etl_run_id;
-- SELECT * FROM etl.file_load_audit ORDER BY file_audit_id;
-- GO

/* =========================================================
   19. SAMPLE CSV CONTENT
   Save a file like this as:
       C:\ETL\Inbound\patient_encounter_20260330.csv

   encounter_id,patient_id,practice_id,encounter_date,encounter_type_code,clinician_id,diagnosis_code,cost,source_file_date
   10001,500001,301,2026-03-29,GP,9001,J20.9,45.00,2026-03-30
   10002,500002,301,2026-03-29,NURSE,9002,E11.9,30.00,2026-03-30
   10003,500003,302,2026-03-29,MEDREV,9003,I10,25.00,2026-03-30
   10003,500003,302,2026-03-29,MEDREV,9003,I10,25.00,2026-03-30
   10004,,302,2026-03-29,GP,9004,J45.9,50.00,2026-03-30
   10005,500005,302,invalid_date,GP,9004,R07.9,60.00,2026-03-30

   Expected result:
       - duplicate row is deduplicated
       - missing patient_id goes to error table
       - invalid encounter_date goes to error table
   ========================================================= */

/* =========================================================
   20. SQL AGENT JOB SCRIPT
   Run this in msdb after testing the ETL manually.
   This creates a job that runs every day at 1:00 AM.
   ========================================================= */

-- USE msdb;
-- GO
--
-- EXEC dbo.sp_add_job
--     @job_name = 'Daily Patient Encounter ETL',
--     @enabled = 1,
--     @description = 'Loads daily synthetic healthcare encounter CSV into PortfolioHealthcareETL';
-- GO
--
-- EXEC dbo.sp_add_jobstep
--     @job_name = 'Daily Patient Encounter ETL',
--     @step_name = 'Run Patient Encounter ETL',
--     @subsystem = 'TSQL',
--     @database_name = 'PortfolioHealthcareETL',
--     @command = 'EXEC etl.usp_run_today_patient_encounter_etl;';
-- GO
--
-- EXEC dbo.sp_add_schedule
--     @schedule_name = 'Daily 1AM Patient Encounter ETL',
--     @freq_type = 4,
--     @freq_interval = 1,
--     @active_start_time = 010000;
-- GO
--
-- EXEC dbo.sp_attach_schedule
--     @job_name = 'Daily Patient Encounter ETL',
--     @schedule_name = 'Daily 1AM Patient Encounter ETL';
-- GO
--
-- EXEC dbo.sp_add_jobserver
--     @job_name = 'Daily Patient Encounter ETL';
-- GO

/* =========================================================
   21. PORTFOLIO NOTES
   How to describe this project:
   "Built a healthcare-inspired SQL Server ETL pipeline that ingests
   synthetic daily patient encounter CSV files into staging and curated
   tables, with validation, deduplication, error handling, logging,
   and SQL Agent job scheduling."
   ========================================================= */