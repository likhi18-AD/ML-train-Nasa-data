import os
import sqlite3
import pandas as pd
import numpy as np
DB_PATH = "./database/ev_health_data.db"
DATASET_PATH = "./data"
BATTERIES = ["B0005", "B0006", "B0007"]
def extract_hi_features(cycle_df):
    """Extract 9 Health Indicators (HIs) from a single cycle."""
    features = {}
    temperature = cycle_df['Temperature_measured'].values
    time_steps = cycle_df['relative_time'].values
    # HI1: Time to peak temperature
    peak_temp_idx = np.argmax(temperature)
    features['HI1_peak_temp_time'] = time_steps[peak_temp_idx]
    # HI2: Peak temperature
    features['HI2_peak_temp'] = np.max(temperature)
    # HI3: Avg temperature
    features['HI3_avg_temp'] = np.mean(temperature)
    # Voltage Indicators
    voltage = cycle_df['Voltage_measured'].values
    features['HI4_initial_voltage'] = voltage[0]
    features['HI5_final_voltage'] = voltage[-1]
    features['HI6_voltage_drop'] = voltage[0] - voltage[-1]
    features['HI7_avg_voltage'] = np.mean(voltage)
    # Current Indicators
    current = cycle_df['Current_measured'].values
    features['HI8_avg_current'] = np.mean(current)
    features['HI9_peak_current'] = np.max(np.abs(current))
    return features
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS battery_cycles")
    cursor.execute("DROP TABLE IF EXISTS cycle_plot_data")
    cursor.execute("DROP TABLE IF EXISTS batteries")

    cursor.execute("""
    CREATE TABLE battery_cycles (
        battery_id TEXT,
        cycle_id INTEGER,
        SOH REAL,
        HI1_peak_temp_time REAL,
        HI2_peak_temp REAL,
        HI3_avg_temp REAL,
        HI4_initial_voltage REAL,
        HI5_final_voltage REAL,
        HI6_voltage_drop REAL,
        HI7_avg_voltage REAL,
        HI8_avg_current REAL,
        HI9_peak_current REAL
    )
    """)

    cursor.execute("""
    CREATE TABLE cycle_plot_data (
        battery_id TEXT,
        cycle_id INTEGER,
        time_step INTEGER,
        voltage REAL,
        current REAL,
        temperature REAL,
        type TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE batteries (
        battery_id TEXT PRIMARY KEY,
        total_cycles INTEGER,
        description TEXT
    )
    """)

    conn.commit()
    conn.close()

def process_battery(battery_id):
    print(f"🔋 Processing {battery_id}...")
    charge_file = os.path.join(DATASET_PATH, battery_id, f"{battery_id}_charge_data.csv")
    discharge_file = os.path.join(DATASET_PATH, battery_id, f"{battery_id}_discharge_data.csv")

    charge_df = pd.read_csv(charge_file)
    discharge_df = pd.read_csv(discharge_file)

    charge_df['relative_time'] = charge_df.groupby('id_cycle').cumcount()
    discharge_df['relative_time'] = discharge_df.groupby('id_cycle').cumcount()

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    hi_records = []
    for cycle_id, cycle_df in discharge_df.groupby('id_cycle'):
        hi = extract_hi_features(cycle_df)
        hi['battery_id'] = battery_id
        hi['cycle_id'] = int(cycle_id)
        hi['SOH'] = cycle_df['Capacity'].max()  # SOH 
        hi_records.append(hi)

    hi_df = pd.DataFrame(hi_records)
    hi_df.to_sql("battery_cycles", conn, if_exists="append", index=False)

    for df, ctype in [(charge_df, "charge"), (discharge_df, "discharge")]:
        plot_records = []
        for _, row in df.iterrows():
            plot_records.append((
                battery_id,
                int(row['id_cycle']),
                int(row['relative_time']),
                row['Voltage_measured'],
                row['Current_measured'],
                row['Temperature_measured'],
                ctype
            ))
        cursor.executemany("""
            INSERT INTO cycle_plot_data (battery_id, cycle_id, time_step, voltage, current, temperature, type)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, plot_records)

    total_cycles = discharge_df['id_cycle'].nunique()
    cursor.execute("INSERT OR REPLACE INTO batteries (battery_id, total_cycles, description) VALUES (?, ?, ?)",
                   (battery_id, total_cycles, f"{battery_id} NASA Battery Dataset"))

    conn.commit()
    conn.close()
    print(f"{battery_id} processed successfully!")

# Main Script
if __name__ == "__main__":
    if not os.path.exists("./database"):
        os.makedirs("./database")

    init_db()
    for batt in BATTERIES:
        process_battery(batt)

    print(" Database ev_health_data.db built successfully!")
