import csv
import random
from datetime import date, timedelta
from pathlib import Path

# ------------------------------------------------------------
# Synthetic Healthcare CSV Generator
# ------------------------------------------------------------
# Purpose:
#   Create a realistic sample daily CSV file for the SQL Server
#   healthcare ETL project.
#
# Output columns:
#   encounter_id, patient_id, practice_id, encounter_date,
#   encounter_type, clinician_id, diagnosis_code, cost,
#   source_file_date
#
# Notes:
#   - Data is fully synthetic.
#   - A few bad rows and duplicate rows are added intentionally
#     so the ETL validation and error logging can be demonstrated.
# ------------------------------------------------------------

OUTPUT_DIR = Path(r"C:\ETL\Inbound")
# Change this if you want to write somewhere else on your machine, e.g.:
# OUTPUT_DIR = Path(r"C:\Users\YourName\Documents\ETL\Inbound")

NUM_GOOD_ROWS = 100
FILE_DATE = date.today()  # file date used in the filename and source_file_date column
SEED = 42

random.seed(SEED)

PRACTICES = [301, 302, 303, 304, 305]
CLINICIANS = [9001, 9002, 9003, 9004, 9005, 9006]
ENCOUNTER_TYPES = [
    ("GP Consultation", 45.00, 95.00),
    ("Nurse Review", 25.00, 55.00),
    ("Medication Review", 20.00, 45.00),
    ("Asthma Review", 30.00, 60.00),
    ("Diabetes Review", 35.00, 70.00),
]
DIAGNOSIS_CODES = [
    "J20.9",  # acute bronchitis
    "J45.9",  # asthma
    "E11.9",  # type 2 diabetes
    "I10",    # hypertension
    "R07.9",  # chest pain
    "M54.5",  # low back pain
    "N39.0",  # UTI
    "K21.9",  # reflux
]


def rand_patient_id() -> int:
    return random.randint(500001, 500999)


def rand_encounter_date(file_date: date) -> date:
    # Use a recent encounter date within the last 7 days.
    return file_date - timedelta(days=random.randint(0, 7))


def rand_cost(low: float, high: float) -> float:
    return round(random.uniform(low, high), 2)


def make_good_rows(n: int, file_date: date):
    rows = []
    encounter_id_start = 10001

    for i in range(n):
        encounter_type, low_cost, high_cost = random.choice(ENCOUNTER_TYPES)
        row = {
            "encounter_id": encounter_id_start + i,
            "patient_id": rand_patient_id(),
            "practice_id": random.choice(PRACTICES),
            "encounter_date": rand_encounter_date(file_date).isoformat(),
            "encounter_type": encounter_type,
            "clinician_id": random.choice(CLINICIANS),
            "diagnosis_code": random.choice(DIAGNOSIS_CODES),
            "cost": f"{rand_cost(low_cost, high_cost):.2f}",
            "source_file_date": file_date.isoformat(),
        }
        rows.append(row)

    return rows


def add_demo_issues(rows, file_date: date):
    """Add duplicate and invalid rows so ETL behaviour can be shown."""
    if not rows:
        return rows

    # Duplicate first valid row.
    rows.append(dict(rows[0]))

    # Missing patient_id.
    bad_missing_patient = dict(rows[1])
    bad_missing_patient["encounter_id"] = 99901
    bad_missing_patient["patient_id"] = ""
    rows.append(bad_missing_patient)

    # Invalid encounter_date.
    bad_date = dict(rows[2])
    bad_date["encounter_id"] = 99902
    bad_date["encounter_date"] = "invalid_date"
    rows.append(bad_date)

    # Negative cost.
    bad_cost = dict(rows[3])
    bad_cost["encounter_id"] = 99903
    bad_cost["cost"] = "-10.00"
    rows.append(bad_cost)

    # Encounter date later than file date.
    future_row = dict(rows[4])
    future_row["encounter_id"] = 99904
    future_row["encounter_date"] = (file_date + timedelta(days=1)).isoformat()
    rows.append(future_row)

    return rows


def write_csv(rows, output_path: Path):
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "encounter_id",
        "patient_id",
        "practice_id",
        "encounter_date",
        "encounter_type",
        "clinician_id",
        "diagnosis_code",
        "cost",
        "source_file_date",
    ]

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    rows = make_good_rows(NUM_GOOD_ROWS, FILE_DATE)
    rows = add_demo_issues(rows, FILE_DATE)

    file_name = f"patient_encounter_{FILE_DATE.strftime('%Y%m%d')}.csv"
    full_path = OUTPUT_DIR / file_name
    write_csv(rows, full_path)

    print(f"Created synthetic CSV file: {full_path}")
    print(f"Rows written: {len(rows)}")
    print("Includes demo duplicate and invalid rows for ETL testing.")
