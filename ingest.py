import fastf1
import pandas as pd
import datetime
import os
from dotenv import load_dotenv
from databricks import sql

# LOAD ENV VARIABLES
load_dotenv()

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_CATALOG = os.getenv("DATABRICKS_CATALOG")
DATABRICKS_SCHEMA = os.getenv("DATABRICKS_SCHEMA")
TABLE_NAME = "raw_race_results"

# ENABLE CACHE
fastf1.Cache.enable_cache('cache/')

# DEFINE PARAMS
YEAR = 2025
ROUNDS = range(1, 13)  # (25 for full season in 2025) Full season, script will skip already loaded rounds

all_records = []

print(f"Starting FastF1 data pull for {YEAR} season...")

# CONNECT TO DATABRICKS
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

# CHECK WHAT ROUNDS ALREADY EXIST IN DATABRICKS
# Query the raw table for existing year + round combinations
try:
    cursor.execute(f"""
        SELECT DISTINCT year, round_number 
        FROM {DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{TABLE_NAME}
    """)

    # Store as a list of tuples
    existing = cursor.fetchall()
    
    # Store as a set of tuples for fast lookup
    # existing_rounds = set(existing)
    existing_rounds = {(row[0], row[1]) for row in existing}
    print(f"Found {len(existing_rounds)} existing round(s) in Databricks — will skip these")

except Exception:
    # Table doesn't exist yet
    existing_rounds = set()
    print("No existing table found — full load will run")

# LOOP THROUGH ROUNDS, SKIPPING ALREADY LOADED ONES
try:
    for round_number in ROUNDS:
        # Check if this year + round combo already exists
        # Values stored as STRING in Databricks so we compare as strings
        if (str(YEAR), str(round_number)) in existing_rounds:
            print(f"Round {round_number} already loaded — skipping")
            continue

        print(f"Loading round {round_number}...")

        try:
            session = fastf1.get_session(YEAR, round_number, 'R')
            session.load()
        except Exception as e:
            # Log and skip rather than exiting the whole script
            print(f"  -> Round {round_number} not available yet, skipping: {e}")
            continue

        df_laps = session.laps.copy()

        df_laps['year'] = YEAR
        df_laps['round_number'] = round_number
        df_laps['event_name'] = session.event['EventName']
        df_laps['session_type'] = 'R'

        all_records.append(df_laps)
        print(f"  -> {len(df_laps)} laps pulled for {session.event['EventName']}")

except Exception as e:
    print(f"Unexpected error during data pull: {e}")
    exit(1)

# IF NO NEW RECORDS, EXIT EARLY
if not all_records:
    print("No new rounds to load — exiting")
    cursor.close()
    connection.close()
    exit(0)

# COMBINE ALL NEW ROUNDS INTO A SINGLE DATAFRAME
print("Combining all new records into a single DataFrame...")
df = pd.concat(all_records, ignore_index=True)

# PRE Cleaning
# CONVERT TIMEDELTA COLUMNS TO SECONDS (float)
time_cols = [
    'Time', 'LapTime', 'PitOutTime', 'PitInTime',
    'Sector1Time', 'Sector2Time', 'Sector3Time',
    'Sector1SessionTime', 'Sector2SessionTime', 'Sector3SessionTime',
    'LapStartTime'
]
for col in time_cols:
    if col in df.columns:
        df[col] = df[col].dt.total_seconds()

# CONVERT DATETIME COLUMN TO STRING
if 'LapStartDate' in df.columns:
    df['LapStartDate'] = df['LapStartDate'].astype(str)


# ADD LOAD TIMESTAMP
df['loaded_at'] = datetime.datetime.now(datetime.UTC)

print(f"Total new records to load: {len(df)}")
print(df.head())

# WRITE NEW RECORDS TO DATABRICKS
# On first run creates the table
# On next runs appends only new rounds
print(f"Loading data into {DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{TABLE_NAME}...")

try:
    # Attempt to insert directly first
    # If table doesn't exist, create it then insert
    BATCH_SIZE = 250
    rows = df.astype(str).values.tolist()

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        placeholders = ", ".join([f"({', '.join(['?' for _ in df.columns])})" for _ in batch])
        flat_values = [val for row in batch for val in row]
        try:
            cursor.execute(
                f"INSERT INTO {DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{TABLE_NAME} VALUES {placeholders}",
                flat_values
            )
        except Exception as insert_error:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(insert_error):
                # Table doesn't exist yet — create it and retry
                print("Table does not exist — creating...")
                col_definitions = ", ".join([f"`{col}` STRING" for col in df.columns])
                cursor.execute(f"""
                    CREATE TABLE {DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{TABLE_NAME} ({col_definitions})
                """)
                cursor.execute(
                    f"INSERT INTO {DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{TABLE_NAME} VALUES {placeholders}",
                    flat_values
                )
            else:
                raise insert_error
        print(f"  -> Inserted rows {i} to {i + len(batch)}")

    connection.commit()
    print(f"-- {TABLE_NAME} loaded successfully!")

except Exception as e:
    print(f"Failed to write to Databricks: {e}")
    exit(1)

finally:
    cursor.close()
    connection.close()
    print("Databricks connection closed.")