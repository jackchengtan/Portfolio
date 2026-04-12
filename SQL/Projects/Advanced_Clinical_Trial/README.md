# Clinical Trial Analytics Project (SQL Server)

## Overview
This project is a **healthcare-inspired clinical trial analytics layer** built in **SQL Server**, designed to simulate a more realistic trial analysis workflow on top of synthetic study data.

It moves beyond simple summary reporting and includes:

- enrollment and cohort structure
- longitudinal outcome analysis
- baseline vs follow-up improvement logic
- responder analysis
- retention and completion analysis
- subgroup treatment effect analysis
- resource utilisation and cost analysis
- cost-effectiveness outputs

The solution is intended for portfolio use and uses **fully synthetic data only**.

---

## Business Scenario
A clinical study tracks patients across multiple trial groups and collects outcome measurements over time. Analysts need more than a simple average score by treatment arm. They also need to understand:

- whether patients improved from baseline
- how outcomes changed over time
- how many patients completed follow-up
- which patients responded to treatment
- whether some subgroups benefited more than others
- how costs and utilisation differed by treatment arm
- whether a treatment appears more cost-effective than alternatives

This project simulates a structured SQL Server analytics layer to answer those questions.

---

## Project Goal
The goal of this project is to simulate a **production-style clinical trial analytics framework** that supports:

- longitudinal patient outcome analysis
- treatment effect measurement
- responder segmentation
- retention and follow-up tracking
- subgroup analysis
- cost and utilisation analysis
- cost-effectiveness reporting

It demonstrates how clinical trial data can be transformed into reusable analytical outputs for reporting, operational review, and study insight generation.

---

## Technical Design

### Database
- **ClinicalTrialDB**

### Schemas
- **trial**: core study data tables
- **ana**: analytics tables, procedures, and views

---

## Core Trial Data Model

### `trial.patient_enrollment`
Stores trial subject and cohort details, including:

- patient ID
- trial group
- enrollment date
- site
- age at enrollment
- sex
- baseline severity
- disease subtype
- withdrawal information

This acts as the study subject master table.

---

### `trial.patient_outcomes`
Stores longitudinal outcome measurements for each patient, including:

- outcome date
- visit label
- outcome score
- outcome type
- primary endpoint flag

This supports repeated measures across the study timeline.

---

### `trial.patient_encounter`
Stores encounter and utilisation records that act as a proxy for healthcare resource use, including:

- encounter date
- encounter type
- cost

This allows the project to connect outcomes to cost and resource consumption.

---

### `trial.study_visit_schedule`
Defines planned study visits such as:

- Baseline
- Week 4
- Week 8
- Week 12

This provides structure for follow-up analysis.

---

## Analytical Architecture

The analytics layer is designed around a set of reusable snapshots and views.

### Snapshot Tables
- `ana.patient_change_from_baseline`
- `ana.retention_analysis`
- `ana.responder_analysis`
- `ana.cost_effectiveness_summary`

### Views
- `ana.vw_enrollment_summary`
- `ana.vw_patient_trial_profile`
- `ana.vw_outcome_trajectory`
- `ana.vw_patient_change_from_baseline`
- `ana.vw_patient_response_segments`
- `ana.vw_treatment_effect_summary`
- `ana.vw_retention_summary`
- `ana.vw_responder_summary`
- `ana.vw_cost_effectiveness`
- `ana.vw_subgroup_treatment_effect`

---

## Key Analytical Features

# 1. Enrollment & Cohort Analysis

## Purpose
Provides an overview of study population structure by treatment arm.

## Example Outputs
- enrolled patients by trial group
- sites contributing data
- average age
- average baseline severity
- withdrawn patients

## Example Business Questions
- Are groups balanced at a high level?
- Which arm has the highest withdrawal count?
- Are there baseline differences between groups?

---

# 2. Longitudinal Outcome Analysis

## Purpose
Tracks how patient outcomes change over time across study visits.

## Key Logic
The project analyses repeated measurements by:

- trial group
- visit label
- outcome date

## Example Outputs
- average score over time
- min and max outcome score by visit
- number of observations at each timepoint

## Example Business Questions
- Does the treatment arm improve faster than control?
- At which visit do outcomes begin to separate?
- Are trajectories stable or variable over time?

---

# 3. Change from Baseline Analysis

## Purpose
Measures patient improvement more realistically than a simple max-minus-min approach.

## Logic
The project calculates:

- **baseline score** = earliest primary endpoint score
- **latest follow-up score** = latest score after baseline
- **best follow-up score** = highest follow-up score after baseline
- **change from baseline** = follow-up minus baseline

This is more realistic than simply using the overall maximum and minimum score.

## Example Outputs
- latest change vs baseline
- best change vs baseline
- percent change from baseline
- number of follow-up visits

## Example Business Questions
- How much did each patient improve?
- Which group shows the largest average change?
- How many follow-up observations are available?

---

# 4. Responder Analysis

## Purpose
Segments patients based on how strongly they responded to treatment.

## Example Categories
- Strong Responder
- Responder
- Non-Responder
- No Follow-up

## Logic
Example thresholds:

- responder: latest change from baseline >= 1
- strong responder: latest change from baseline >= 3

## Example Outputs
- responder count by group
- strong responder count by group
- responder rate
- patient-level response segments

## Example Business Questions
- Which treatment has the highest responder rate?
- How many strong responders are there?
- Which patients showed no improvement?

---

# 5. Retention & Follow-up Analysis

## Purpose
Measures study completion and follow-up quality.

## Example Outputs
- enrolled patients
- patients with baseline
- patients with any follow-up
- patients with Week 12 follow-up
- withdrawn patients
- follow-up completion rate
- Week 12 completion rate
- withdrawal rate

## Example Business Questions
- Which group retained patients best?
- How many subjects reached Week 12?
- Is attrition affecting interpretation of treatment effect?

---

# 6. Cost & Utilisation Analysis

## Purpose
Connects outcome data with encounter and cost data to support efficiency and value analysis.

## Example Outputs
- total encounters by group
- total cost by group
- average cost per patient
- average cost per encounter

## Example Business Questions
- Which treatment group generated the highest resource use?
- Is one arm more expensive to manage?
- Do cost patterns differ by group?

---

# 7. Cost-Effectiveness Analysis

## Purpose
Combines response outcomes with cost data to create value-oriented metrics.

## Example Outputs
- responder count
- average improvement
- cost per responder
- cost per unit improvement

## Example Business Questions
- Which treatment appears most cost-effective?
- Does higher cost correspond to better improvement?
- Which arm delivers the best balance of cost and response?

---

# 8. Subgroup Treatment Effect Analysis

## Purpose
Examines whether treatment effect differs across patient subgroups.

## Example Subgroups
- sex
- age band
- disease subtype

## Example Outputs
- average latest change by subgroup
- average best change by subgroup
- responder rate by subgroup

## Example Business Questions
- Does treatment work better in younger patients?
- Are outcomes different by disease subtype?
- Do response patterns vary by sex?

---

## Main Stored Procedures

### `ana.usp_build_patient_change_from_baseline`
Builds the patient-level baseline vs follow-up analysis table.

### `ana.usp_build_retention_analysis`
Builds trial-group retention and completion metrics.

### `ana.usp_build_responder_analysis`
Builds treatment-arm responder summaries.

### `ana.usp_build_cost_effectiveness`
Builds cost-effectiveness outputs by trial group.

### `ana.usp_refresh_trial_analytics`
Wrapper procedure that refreshes the full analytics layer.

---

## How to Run the Project

### 1. Create database objects
Run the SQL script in SQL Server Management Studio to create:

- database
- schemas
- core trial tables
- analytic snapshot tables
- stored procedures
- reporting views

### 2. Load or generate synthetic study data
Insert synthetic data into:

- `trial.patient_enrollment`
- `trial.patient_outcomes`
- `trial.patient_encounter`

### 3. Refresh analytics
Run:

```sql
EXEC ana.usp_refresh_trial_analytics;
