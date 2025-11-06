import pyodbc
import random
from datetime import datetime, timedelta
import uuid

# Connection details - update as needed
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 18 for SQL Server};'
    'SERVER=ASUS_MANJUNATHA\\SQLEXPRESS;'
    'DATABASE=NHS_A_E_Warehouse;'
    'Trusted_Connection=yes;'
    'TrustServerCertificate=yes;'
)
cursor = conn.cursor()

def random_datetime(base, min_offset=0, max_offset=120):
    """Generate a random datetime offset from a base datetime in minutes."""
    return base + timedelta(minutes=random.randint(min_offset, max_offset))

def generate_patient_ae_record():
    """Generate a single, unique AE patient record."""
    arrival = datetime.now() - timedelta(hours=1)
    triage = random_datetime(arrival, 0, 10)
    seen_doc = random_datetime(triage, 5, 20)
    admit_dec = random_datetime(seen_doc, 5, 40)
    ward_admit = random_datetime(admit_dec, 5, 20)
    discharge = random_datetime(ward_admit, 60, 300)
    reasons = ['Treated', 'Admitted', 'Transferred', 'Left', 'Other']
    # Guarantee patient ID uniqueness using UUID4
    patient_id = str(uuid.uuid4())
    record = (
        patient_id,
        arrival,
        triage,
        seen_doc,
        admit_dec,
        ward_admit,
        discharge,
        random.choice(reasons),
        f"Ward-{random.randint(1,5)}",
        datetime.now()
    )
    return record

def insert_patient_ae(n=10):
    """Insert n generated patient journey records into the database."""
    count = 0
    for _ in range(n):
        rec = generate_patient_ae_record()
        cursor.execute("""
        INSERT INTO bronze.Patient_AE_Journeys
        (PatientID, Arrival_Timestamp, Triage_Timestamp, SeenByDoctor_Timestamp, DecisionToAdmit_Timestamp,
         WardAdmission_Timestamp, Discharge_Timestamp, Discharge_Reason, CurrentLocation, IngestionTimestamp)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rec)
        count += 1
    conn.commit()
    print(f"Inserted {count} records.")

insert_patient_ae(20)

cursor.close()
conn.close()
