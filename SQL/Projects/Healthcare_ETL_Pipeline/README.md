# Daily Healthcare Encounter ETL Pipeline (SQL Server)

## Overview
This project is a **healthcare-inspired SQL Server ETL pipeline** built with **synthetic patient encounter data**. It simulates a realistic batch-processing workflow where a clinic or healthcare organisation receives a **daily CSV extract** from an operational system and loads it into a reporting database.

The solution was designed to demonstrate practical SQL Server skills that are commonly used in real-world data engineering and reporting environments:

- database and schema design
- CSV ingestion with staging tables
- data validation and cleansing
- deduplication using a business key
- upsert logic into a curated target table
- ETL error logging and run logging
- daily scheduling with SQL Server Agent

This project is intended for portfolio use and uses **fully synthetic data only**.

---

## Business Scenario
A healthcare provider receives a daily CSV file containing patient encounter records. The data must be loaded into SQL Server for downstream reporting and operational monitoring.

The ETL pipeline is responsible for:

1. loading the CSV into a raw staging table
2. validating required columns and date formats
3. rejecting invalid records into an error table
4. removing duplicate encounter rows
5. inserting new records and updating existing records in the final reporting table
6. logging ETL activity for monitoring and troubleshooting
7. running automatically each day

---

## Example Data Fields
Each daily file includes fields such as:

- `encounter_id`
- `patient_id`
- `practice_id`
- `encounter_date`
- `encounter_type`
- `clinician_id`
- `diagnosis_code`
- `cost`
- `source_file_date`

A small number of bad rows are intentionally included in the sample generator so the ETL can demonstrate validation and rejection logic.

---

## Technical Design
### Database Layers
The database is split into clear ETL layers:

- **dbo**: curated target tables and dimensions
- **stg**: raw and validated staging tables
- **etl**: error logging, run logging, file audit, and ETL procedures

### Main Tables
- `dbo.patient_encounter` – final curated reporting table
- `stg.patient_encounter_raw` – raw CSV load table with text-based columns
- `stg.patient_encounter_valid` – validated and typed staging table
- `etl.patient_encounter_error` – invalid rows rejected during validation
- `etl.etl_run_log` – ETL execution log by process step
- `etl.file_load_audit` – optional file-level audit tracking

### Dimension Tables
- `dbo.dim_practice`
- `dbo.dim_encounter_type`

---

## ETL Flow
### 1. Raw File Load
The CSV file is loaded into `stg.patient_encounter_raw` using `BULK INSERT`. The raw table stores most values as text so parsing and validation can happen in a controlled way.

### 2. Validation
The validation procedure checks:

- numeric identifiers
- valid encounter dates
- non-blank encounter type
- valid and non-negative cost
- valid source file date
- business rule: `encounter_date` cannot be later than `source_file_date`

Invalid rows are written to `etl.patient_encounter_error`.

### 3. Deduplication
Valid rows are deduplicated using a business key based on:

- `encounter_id`
- `patient_id`
- `practice_id`
- `encounter_date`
- `encounter_type`
- `clinician_id`

### 4. Upsert to Target
The ETL then updates matching rows in `dbo.patient_encounter` and inserts rows that do not already exist.

### 5. Logging
Each ETL stage writes a record into `etl.etl_run_log`, including row counts and status.

### 6. Daily Scheduling
A SQL Server Agent job runs the ETL once per day and loads the latest inbound CSV file.

---

## Files Included
### Main SQL script
- `healthcare_etl_pipeline.sql`

This contains:
- database creation script
- schema creation
- dimension and target tables
- staging, error, log, and audit tables
- stored procedures
- wrapper procedure for daily dated files
- SQL Agent job script

### Sample CSV generator
- `sample_csv_generator.py`

This creates a synthetic file like:
- `patient_encounter_YYYYMMDD.csv`

It also intentionally adds:
- one duplicate record
- missing patient identifier
- invalid date
- negative cost
- encounter date after source file date

### Archive script
- `archive_after_load.ps1`

This PowerShell script moves a processed file from the inbound folder to an archive folder after a successful ETL run. It can also compress the archived file into a ZIP.

---

## How to Run the Project
### 1. Create the database objects
Run the SQL script in SQL Server Management Studio:

- `healthcare_etl_pipeline.sql`

### 2. Generate a sample CSV file
Run the Python generator:

```bash
python sample_csv_generator.py
```

This creates a dated CSV file in the inbound folder.

### 3. Run the ETL manually
Execute the wrapper procedure in SQL Server:

```sql
EXEC etl.usp_run_today_patient_encounter_etl;
```

### 4. Check ETL results
Review:

- `dbo.patient_encounter`
- `etl.patient_encounter_error`
- `etl.etl_run_log`

### 5. Archive the file after load
Run the PowerShell script:

```powershell
.\archive_after_load.ps1 -ZipAfterMove
```

---

## Key Skills Demonstrated
- SQL Server database development
- ETL design with staging layers
- stored procedures and procedural T-SQL
- CSV ingestion with `BULK INSERT`
- data quality validation
- deduplication and upsert logic
- ETL monitoring and logging
- SQL Server Agent job scheduling
- supporting automation with PowerShell

---

## Portfolio / CV Description
Built a healthcare-inspired SQL Server ETL pipeline to ingest synthetic daily patient encounter CSV data into staging and reporting tables. Implemented raw load, validation, deduplication, upsert logic, ETL logging, and automated daily scheduling. Added a supporting PowerShell archive workflow to move processed files after successful load.

---

## Notes
- This project uses **synthetic data only** and is designed for portfolio demonstration.
- File paths in the scripts may need to be updated depending on the local machine.
- `BULK INSERT` requires the SQL Server service account to have access to the inbound folder.
- If CSV line endings differ, `ROWTERMINATOR` may need to be adjusted.

---

## Future Improvements
Possible extensions include:

- email alerting on ETL failure
- file duplicate protection
- archive audit table updates
- Power BI dashboard on ETL run logs
- additional healthcare entities such as referrals, medications, or immunisations
- multi-file ingestion pattern
