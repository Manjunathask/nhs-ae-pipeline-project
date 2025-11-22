import random
from datetime import datetime
from airflow.providers.odbc.hooks.odbc import OdbcHook

# Static list of wards
wards = [
    ('W1', 'Acute Medical', 'Medicine'),
    ('W2', 'Surgical', 'Surgery'),
    ('W3', 'Paediatric', 'Children'),
    ('W4', 'Orthopaedic', 'Bones'),
    ('W5', 'Maternity', 'Women'),
]

def insert_ward_occupancy():
    """Generate random ward occupancy records."""
    # Ensure your Airflow Connection ID is 'mssql_local' and type is 'ODBC'
    hook = OdbcHook(
        odbc_conn_id='mssql_local', 
        driver='ODBC Driver 18 for SQL Server'
    )
    
    conn = hook.get_conn()
    cursor = conn.cursor()

    print("Starting Ward Occupancy update...")
    
    try:
        for ward_id, ward_name, specialty in wards:
            total_beds = random.randint(15, 40)
            occupied = random.randint(0, total_beds)
            last_updated = datetime.now()

            # Execute Insert
            # Note: pyodbc uses '?' as placeholders, just like pymssql.
            cursor.execute("""
            INSERT INTO bronze.Ward_Occupancy
            (WardID, WardName, Specialty, TotalBeds, OccupiedBeds, LastUpdatedTimestamp)
            VALUES (?, ?, ?, ?, ?, ?)
            """, (ward_id, ward_name, specialty, total_beds, occupied, last_updated))
            
        conn.commit()
        print(f"Successfully updated status for {len(wards)} wards.")

    except Exception as e:
        print(f"Error updating wards: {e}")
        conn.rollback()
        raise
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    insert_ward_occupancy()