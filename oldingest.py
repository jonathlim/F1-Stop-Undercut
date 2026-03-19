import fastf1
import pandas as pd
import datetime
import os
from dotenv import load_dotenv
from databricks import sql

# -------------------------------------------------------
# LOAD ENVIRONMENT VARIABLES
# -------------------------------------------------------
load_dotenv()

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_CATALOG = os.getenv("DATABRICKS_CATALOG")
DATABRICKS_SCHEMA = os.getenv("DATABRICKS_SCHEMA")
TABLE_NAME = "raw_race_results"

# -------------------------------------------------------
# ENABLE FASTF1 CACHE
# Saves downloaded session data locally so you don't
# re-download the same data every time you run the script
# -------------------------------------------------------
fastf1.Cache.enable_cache('cache/')

# -------------------------------------------------------
# DEFINE WHAT DATA TO PULL
# Adjust year and rounds list as needed
# -------------------------------------------------------
YEAR = 2025
ROUNDS = range(1, 2)  # Round 1 only for now, expand as more races happen

all_records = []

print(f"Starting FastF1 data pull for {YEAR} season...")

# -------------------------------------------------------
# LOOP THROUGH EACH ROUND AND LOAD SESSION DATA
# 'R' = Race session. Other options: 'Q' = Qualifying,
# 'FP1', 'FP2', 'FP3' = Practice sessions
# -------------------------------------------------------
try:
    for round_number in ROUNDS:
        print(f"Loading round {round_number}...")

        # Load the session - this fetches from FastF1 API / cache
        session = fastf1.get_session(YEAR, round_number, 'R')
        session.load()

        # session.laps returns a DataFrame of every lap in the race
        df_laps = session.laps.copy()

        # Add context columns so we know which race this data belongs to
        df_laps['year'] = YEAR
        df_laps['round_number'] = round_number
        df_laps['event_name'] = session.event['EventName']
        df_laps['session_type'] = 'R'

        all_records.append(df_laps)
        print(f"  -> {len(df_laps)} laps pulled for {session.event['EventName']}")

except fastf1.core.DataNotLoadedError:
    print(f"Session data not available for round {round_number}")
    exit(1)
except ConnectionError:
    print(f"Failed to connect to FastF1 — check your internet connection")
    exit(1)
except Exception as e:
    print(f"Failed to pull data from FastF1: {e}")
    exit(1)

# -------------------------------------------------------
# COMBINE ALL ROUNDS INTO A SINGLE DATAFRAME
# -------------------------------------------------------
print("Combining all records into a single DataFrame...")
df = pd.concat(all_records, ignore_index=True)

# -------------------------------------------------------
# CONVERT TIMEDELTA COLUMNS TO SECONDS (float)
# FastF1 stores all time-based columns as timedelta64
# which Databricks cannot store directly
# NaT values (no pit stop, safety car laps etc.)
# will become NaN floats which is fine for the raw layer
# -------------------------------------------------------
time_cols = [
    'Time', 'LapTime', 'PitOutTime', 'PitInTime',
    'Sector1Time', 'Sector2Time', 'Sector3Time',
    'Sector1SessionTime', 'Sector2SessionTime', 'Sector3SessionTime',
    'LapStartTime'
]
for col in time_cols:
    if col in df.columns:
        df[col] = df[col].dt.total_seconds()

# -------------------------------------------------------
# CONVERT DATETIME COLUMN TO STRING
# LapStartDate is datetime64 — convert to string so
# Databricks can store it cleanly at the raw layer
# -------------------------------------------------------
if 'LapStartDate' in df.columns:
    df['LapStartDate'] = df['LapStartDate'].astype(str)

# -------------------------------------------------------
# ADD LOAD TIMESTAMP
# Tracks when this data was ingested into Databricks
# -------------------------------------------------------
df['loaded_at'] = datetime.datetime.utcnow()

print(f"Total records to load: {len(df)}")
print(df.head())

# -------------------------------------------------------
# CONNECT TO DATABRICKS
# Uses the SQL connector with credentials from .env
# -------------------------------------------------------
print("Connecting to Databricks...")
try:
    connection = sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    )
    cursor = connection.cursor()
    print("Connected to Databricks successfully!")

except Exception as e:
    print(f"Failed to connect to Databricks: {e}")
    exit(1)

# -------------------------------------------------------
# WRITE DATA TO DATABRICKS
# Drops and recreates the table on each run (replace behavior)
# All columns stored as STRING at the raw layer —
# type casting is handled later in dbt
# -------------------------------------------------------
print(f"Loading data into {DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{TABLE_NAME}...")

try:
    # Drop table if it exists so we start fresh each run
    cursor.execute(f"DROP TABLE IF EXISTS {DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{TABLE_NAME}")

    # Dynamically build CREATE TABLE based on DataFrame columns
    # Everything stored as STRING for simplicity at raw/ingestion layer
    col_definitions = ", ".join([f"`{col}` STRING" for col in df.columns])
    cursor.execute(f"""
        CREATE TABLE {DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{TABLE_NAME} ({col_definitions})
    """)

    # Insert rows in batches of 500 to avoid overwhelming the connection
    BATCH_SIZE = 250

    # Converts df values to str > extract just the value > numpy array to python list of lists
    rows = df.astype(str).values.tolist()  # Convert all values to string to match schema

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]

        # Creates list of "?" size of amount of columns in df 
        # ['?','?','?'] > '(? ,? ,? )' > repeats for every row in batch
        placeholders = ", ".join([f"({', '.join(['?' for _ in df.columns])})" for _ in batch])
        
        flat_values = [val for row in batch for val in row]
        cursor.execute(
            f"INSERT INTO {DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{TABLE_NAME} VALUES {placeholders}",
            flat_values
        )
        print(f"  -> Inserted rows {i} to {i + len(batch)}")

    connection.commit()
    print(f"-- {TABLE_NAME} loaded successfully!")

except Exception as e:
    print(f"Failed to write to Databricks: {e}")
    exit(1)

finally:
    # Always close the cursor and connection when done
    cursor.close()
    connection.close()
    print("Databricks connection closed.")