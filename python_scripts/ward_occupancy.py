import pyodbc
import random
from datetime import datetime

conn = pyodbc.connect(
    'DRIVER={ODBC Driver 18 for SQL Server};'
    'SERVER=ASUS_MANJUNATHA\\SQLEXPRESS;'
    'DATABASE=NHS_A_E_Warehouse;'
    'Trusted_Connection=yes;'
    'TrustServerCertificate=yes;'
)
cursor = conn.cursor()

wards = [
    ('W1', 'Acute Medical', 'Medicine'),
    ('W2', 'Surgical', 'Surgery'),
    ('W3', 'Paediatric', 'Children'),
    ('W4', 'Orthopaedic', 'Bones'),
    ('W5', 'Maternity', 'Women'),
]

def insert_ward_occupancy():
    for ward_id, ward_name, specialty in wards:
        total_beds = random.randint(15, 40)
        occupied = random.randint(0, total_beds)
        last_updated = datetime.now()
        cursor.execute("""
        INSERT INTO bronze.Ward_Occupancy
         (WardID, WardName, Specialty, TotalBeds, OccupiedBeds, LastUpdatedTimestamp)
         VALUES (?, ?, ?, ?, ?, ?)
        """, (ward_id, ward_name, specialty, total_beds, occupied, last_updated))
    conn.commit()

insert_ward_occupancy()
cursor.close()
conn.close()