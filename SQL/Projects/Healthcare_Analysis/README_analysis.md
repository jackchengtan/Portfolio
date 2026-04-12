# Advanced Healthcare Analytics Layer (Forecasting, Scenario Modelling & Patient Segmentation)

## Overview
This project extends a healthcare data platform with an **advanced analytics layer** built in **SQL Server**, designed to go beyond reporting and provide:

- demand forecasting
- scenario simulation
- patient segmentation and profiling

It sits on top of the ETL, data quality, reporting, and alerting layers, transforming curated encounter data into **forward-looking insights and strategic decision support**.

This layer demonstrates real-world analytics capabilities commonly used in healthcare, operations, and business intelligence environments:

- time-series forecasting using SQL
- scenario-based planning and simulation
- behavioural and cost-based segmentation
- demand and capacity modelling
- decision-support analytics design

This project uses **fully synthetic data** and is designed for portfolio demonstration.

---

## Business Context
A healthcare organisation has:

- an ETL pipeline to ingest encounter data
- a data quality framework to ensure data integrity
- a reporting layer to track KPIs
- an alerting system to monitor failures

However, stakeholders also need **advanced analytics capabilities** to answer higher-value questions:

### Forecasting
- How many patients will we see next week?
- Which days will experience peak demand?
- Which practices will be under pressure tomorrow?

### Scenario Modelling
- What happens if demand increases by 20%?
- What if clinician capacity drops?
- What if costs increase significantly?

### Patient Segmentation
- Who are the high-cost patients?
- Who are frequent attenders?
- Which patients are complex and multi-touch?

This project addresses these needs.

---

## Project Goal
The goal is to simulate a **production-style advanced analytics layer** that supports:

- predictive planning
- operational readiness
- resource allocation
- patient-level insight generation
- strategic decision-making

It demonstrates how a data platform evolves from **descriptive → diagnostic → predictive → prescriptive analytics**.

---

## Architecture

### Schemas
The advanced analytics layer introduces three new schemas:

- **fcst** → forecasting outputs
- **scn** → scenario modelling
- **seg** → patient segmentation

These work alongside:

- **ana** → core analysis layer
- **rpt** → reporting layer
- **dq** → data quality layer
- **etl** → ingestion layer
- **ops** → alerting layer

---

# 1. Forecasting Layer (`fcst`)

## Purpose
Provides short-term demand forecasts for:

- total daily encounters
- practice-level demand

## Key Features

### Time-Series Forecasting Logic
Forecasts are based on:

- rolling 7-day average (recent trend)
- rolling 30-day average (baseline)
- trend factor (short-term vs long-term comparison)
- weekday seasonality adjustment

### Outputs

#### `fcst.daily_demand_forecast`
- next 7 days of demand
- forecast confidence range (upper/lower bound)
- trend and seasonality factors

#### `fcst.practice_demand_forecast`
- next-day forecast per practice
- practice-level trend analysis
- ranking by expected demand

---

## Example Business Use Cases

- identify upcoming high-demand days
- plan staffing levels for next week
- detect rising demand trends early
- prepare for peak operational periods

---

# 2. Scenario Modelling Layer (`scn`)

## Purpose
Simulates **what-if scenarios** to understand how changes impact demand, cost, and capacity.

## Scenario Variables

Each scenario can adjust:

- demand multiplier (e.g. +20% demand)
- cost multiplier (e.g. inflation)
- clinician capacity multiplier (e.g. staff shortage)
- encounter mix multiplier

---

## Key Tables

### `scn.scenario_config`
Defines scenario assumptions:

- baseline
- moderate pressure
- severe demand surge
- cost inflation
- recovery scenarios

---

### `scn.practice_scenario_impact`
Practice-level simulation results:

- projected daily load
- projected cost
- load increase vs baseline
- pressure classification

---

### `scn.scenario_summary`
High-level impact:

- projected total demand
- projected cost
- cost per encounter
- capacity pressure ratio
- number of high-strain practices

---

## Example Business Use Cases

- capacity planning under demand surge
- workforce planning (staff shortages)
- cost impact modelling
- risk assessment for operational overload
- strategic decision support

---

# 3. Patient Segmentation Layer (`seg`)

## Purpose
Segments patients into meaningful groups based on:

- utilisation
- cost
- complexity
- recency

---

## Key Features

### Metrics Calculated
For each patient:

- total encounters
- total cost
- average cost
- number of active days
- number of practices visited
- number of clinicians seen
- diagnosis diversity
- encounter type diversity
- recency (days since last visit)

---

### Segmentation Dimensions

#### Utilisation Segment
- High utilisation
- Medium utilisation
- Low utilisation

#### Cost Segment
- High cost
- Medium cost
- Low cost

#### Complexity Segment
- High complexity
- Medium complexity
- Low complexity

---

### Final Patient Segments
Examples:

- `HIGH_NEEDS_HIGH_COST`
- `FREQUENT_ATTENDER`
- `HIGH_COST_CASE`
- `COMPLEX_MULTI_TOUCH`
- `RECENT_LOW_TOUCH`
- `STABLE_STANDARD`

---

## Key Outputs

### `seg.patient_segmentation`
Full patient-level segmentation dataset

### `seg.vw_patient_segment_summary`
Aggregated view of segments

### `seg.vw_high_value_patients`
Top high-cost / high-complexity patients

### `seg.vw_patient_segment_distribution`
Distribution across utilisation, cost, and complexity

---

## Example Business Use Cases

- identify high-cost patient cohorts
- target frequent attenders for intervention
- prioritise complex patients
- improve care pathway planning
- optimise resource allocation

---

# End-to-End Flow

1. ETL loads encounter data  
2. DQ validates data quality  
3. Reporting produces KPIs  
4. Alerting monitors issues  
5. Analysis explains demand patterns  
6. Forecasting predicts future demand  
7. Scenario modelling simulates changes  
8. Segmentation identifies patient groups  

---

# How to Run

### Step 1: Ensure prerequisites
Make sure these layers exist:

- ETL (`dbo.patient_encounter`)
- DQ (`dq`)
- Reporting (`rpt`)
- Analysis (`ana`)

---

### Step 2: Run scripts
Execute:

- forecasting script
- scenario modelling script
- segmentation script

---

### Step 3: Refresh analytics

```sql
EXEC ana.usp_refresh_advanced_analytics_stack;
