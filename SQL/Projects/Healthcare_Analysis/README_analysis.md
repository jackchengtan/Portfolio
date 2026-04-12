# Advanced Healthcare Analytics Layer (Demand Analysis, Forecasting, Scenario Modelling & Patient Segmentation)

## Overview
This project extends a healthcare data platform with an **advanced analytics layer** built in **SQL Server**, designed to go beyond reporting and provide:

- demand and operational strain analysis
- demand forecasting
- scenario simulation
- patient segmentation and profiling

It sits on top of the ETL, data quality, reporting, and alerting layers, transforming curated encounter data into **both current-state insights and forward-looking decision support**.

This layer demonstrates real-world analytics capabilities used in healthcare, operations, and business intelligence environments:

- demand and utilisation analysis
- time-series forecasting using SQL
- scenario-based planning and simulation
- behavioural and cost-based segmentation
- capacity and workload modelling

This project uses **fully synthetic data** and is designed for portfolio demonstration.

---

## Business Context
A healthcare organisation has:

- an ETL pipeline to ingest encounter data
- a data quality framework to ensure data integrity
- a reporting layer to track KPIs
- an alerting system to monitor failures

However, stakeholders also need an **advanced analytics layer** to answer higher-value questions across four areas:

### Demand Analysis
- What is current daily demand?
- Which days show overload, high pressure, or underutilisation?
- Which practices are under the most strain?
- Which clinicians have the highest workload?
- Which diagnoses and encounter types are driving demand?
- How do weekday and monthly demand patterns change over time?

### Predictive Analysis
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

- demand and strain analysis
- predictive planning
- operational readiness
- resource allocation
- patient-level insight generation
- strategic decision-making

It demonstrates how a data platform evolves from:

**descriptive → diagnostic → predictive → prescriptive analytics**

---

## Architecture

### Schemas
The advanced analytics layer brings together four analytical areas:

- **ana** → demand analysis and operational strain  
- **fcst** → forecasting outputs  
- **scn** → scenario modelling  
- **seg** → patient segmentation  

These work alongside:

- **etl** → ingestion layer  
- **dq** → data quality layer  
- **rpt** → reporting layer  
- **ops** → alerting layer  

---

# 1. Demand Analysis Layer (`ana`)

## Purpose
Provides a structured analytical view of current healthcare demand, utilisation, and operational strain.

This layer explains:

- how much demand is occurring today
- where operational pressure is building
- which practices and clinicians are under strain
- which diagnoses and encounter types drive workload
- how demand varies across time

---

## Key Features

### Daily Demand Monitoring
Tracks:

- total encounters
- unique patients
- unique practices
- unique clinicians
- total and average cost
- rolling 7-day and 30-day baselines
- day-on-day changes
- demand classification:
  - OVERLOAD
  - HIGH_PRESSURE
  - NORMAL
  - UNDERUTILISED

---

### Practice Strain Analysis
Measures:

- average daily load
- peak daily load
- load variability
- capacity band (low → very high)
- strain risk level

---

### Clinician Workload Analysis
Measures:

- average daily workload
- peak workload
- workload variability
- workload risk level
- cross-practice coverage

---

### Diagnosis Pressure Analysis
Identifies:

- diagnoses driving highest volume
- diagnoses driving highest cost
- peak daily demand by diagnosis
- pressure level by condition

---

### Encounter Type Mix Analysis
Tracks:

- demand by encounter type
- share of total workload
- dominant vs minor encounter types
- mix-driven pressure

---

### Time-Based Demand Patterns
Analyses:

- weekday demand behaviour
- monthly demand trends
- temporal variation in utilisation

---

## Example Business Use Cases

- identify overloaded days
- detect early signs of operational strain
- compare performance across practices
- balance clinician workload
- identify demand-driving conditions
- support workforce planning

---

# 2. Forecasting Layer (`fcst`)

## Purpose
Provides short-term demand forecasts for:

- overall healthcare demand
- practice-level activity

---

## Key Features

### Forecasting Logic
Based on:

- rolling 7-day average (recent trend)
- rolling 30-day average (baseline)
- trend factor (short vs long-term comparison)
- weekday seasonality adjustment

---

## Outputs

### `fcst.daily_demand_forecast`
- next 7 days demand forecast
- confidence range (upper/lower bounds)
- trend and seasonality indicators

### `fcst.practice_demand_forecast`
- next-day demand per practice
- ranking by expected load
- trend factor per practice

---

## Example Use Cases

- plan staffing levels
- anticipate peak days
- detect rising demand trends
- prepare for operational surges

---

# 3. Scenario Modelling Layer (`scn`)

## Purpose
Simulates **what-if scenarios** to evaluate the impact of operational changes.

---

## Scenario Variables

Each scenario can adjust:

- demand multiplier
- cost multiplier
- clinician capacity multiplier
- encounter mix

---

## Key Tables

### `scn.scenario_config`
Defines scenarios such as:

- baseline
- moderate pressure
- severe demand surge
- cost inflation
- recovery scenarios

---

### `scn.practice_scenario_impact`
Practice-level simulation:

- projected demand
- projected cost
- % change vs baseline
- pressure classification

---

### `scn.scenario_summary`
High-level outputs:

- projected demand
- projected cost
- cost per encounter
- capacity pressure ratio
- number of high-strain practices

---

## Example Use Cases

- workforce planning
- surge preparedness
- financial impact modelling
- operational risk assessment
- strategic planning

---

# 4. Patient Segmentation Layer (`seg`)

## Purpose
Segments patients into meaningful groups based on:

- utilisation
- cost
- complexity
- recency

---

## Key Features

### Metrics Calculated
- total encounters
- total cost
- average cost
- active days
- practices visited
- clinicians seen
- diagnosis diversity
- encounter type diversity
- recency

---

### Segmentation Dimensions

#### Utilisation
- High
- Medium
- Low

#### Cost
- High
- Medium
- Low

#### Complexity
- High
- Medium
- Low

---

### Final Segments

- HIGH_NEEDS_HIGH_COST  
- FREQUENT_ATTENDER  
- HIGH_COST_CASE  
- COMPLEX_MULTI_TOUCH  
- RECENT_LOW_TOUCH  
- STABLE_STANDARD  

---

## Outputs

- `seg.patient_segmentation`
- `seg.vw_patient_segment_summary`
- `seg.vw_high_value_patients`
- `seg.vw_patient_segment_distribution`

---

## Example Use Cases

- identify high-cost patients
- target frequent attenders
- prioritise complex cases
- optimise care pathways
- improve resource allocation

---

# End-to-End Flow

1. ETL loads encounter data  
2. DQ validates data quality  
3. Reporting produces KPIs  
4. Alerting monitors issues  
5. Demand analysis explains current utilisation and strain  
6. Forecasting predicts future demand  
7. Scenario modelling simulates operational change  
8. Segmentation identifies patient groups  

---

# How to Run

### Step 1: Ensure prerequisites
- ETL layer (`dbo.patient_encounter`)
- DQ layer (`dq`)
- Reporting layer (`rpt`)
- Analysis layer (`ana`)

---

### Step 2: Run scripts
Execute:

- analysis layer
- forecasting layer
- scenario modelling layer
- segmentation layer

---

### Step 3: Refresh analytics

```sql
EXEC ana.usp_refresh_advanced_analytics_stack;
