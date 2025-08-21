import sqlite3
import pandas as pd

DB_PATH = './database/ev_health_data.db'

# Connect to DB
conn = sqlite3.connect(DB_PATH)

# 1️⃣ Preview battery_cycles (HIs + SOH)
print("\n🔋 Battery Cycles Table (HIs + SOH):")
df_cycles = pd.read_sql_query("SELECT * FROM battery_cycles LIMIT 10", conn)
print(df_cycles)

# 2️⃣ Preview cycle_plot_data (raw plots)
print("\n📈 Cycle Plot Data (Voltage/Current/Temp):")
df_plot = pd.read_sql_query("SELECT * FROM cycle_plot_data LIMIT 10", conn)
print(df_plot)

# 3️⃣ Preview batteries metadata
print("\nℹ️ Battery Metadata:")
df_meta = pd.read_sql_query("SELECT * FROM batteries", conn)
print(df_meta)

# 4️⃣ Check number of records per table
print("\n📊 Record Counts:")
for table in ["battery_cycles", "cycle_plot_data", "batteries"]:
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"{table}: {count} records")

conn.close()
