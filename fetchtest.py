import fastf1
import pandas as pd

# Enable cache
fastf1.Cache.enable_cache('cache/')

# Load a single session to explore
YEAR = 2025
ROUND = 1  # Bahrain GP
SESSION_TYPE = 'R'  # Race

print(f"Loading {YEAR} Round {ROUND} - {SESSION_TYPE}...")

session = fastf1.get_session(YEAR, ROUND, SESSION_TYPE)
session.load()

print(f"\nEvent: {session.event['EventName']}")

# Look at the laps DataFrame
df = session.laps.copy()

print(f"\nShape: {df.shape}")
print(f"\nColumns:\n{df.columns.tolist()}")
print(f"\nSample data:\n{df.head()}")
print(f"\nData types:\n{df.dtypes}")