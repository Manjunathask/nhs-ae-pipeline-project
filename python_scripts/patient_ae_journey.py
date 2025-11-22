import random
from datetime import datetime, timedelta
import uuid
from airflow.providers.odbc.hooks.odbc import OdbcHook
import sys

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
    """Insert n generated patient journey records into the database using Airflow Hook."""
    
    # ---------------------------------------------------------
    # Explicitly pass the driver here.
    # This bypasses the security restriction on the 'Extra' field.
    # ---------------------------------------------------------
    hook = OdbcHook(
        odbc_conn_id='mssql_local', 
        driver='ODBC Driver 18 for SQL Server'
    )
    
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    count = 0
    print(f"Starting insertion of {n} records...")

    try:
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
        print(f"Successfully inserted {count} records.")
        
    except Exception as e:
        print(f"Error inserting data: {e}")
        conn.rollback()
        raise
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    insert_patient_ae(20)