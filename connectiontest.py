from dotenv import load_dotenv
from databricks import sql
import os

load_dotenv()

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

print("Attempting to connect to Databricks...")

try:
    connection = sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    )
    cursor = connection.cursor()
    cursor.execute("SELECT 1")
    result = cursor.fetchone()
    print(f"Connection successful! Test query returned: {result}")

except Exception as e:
    print(f"Connection failed: {e}")

finally:
    cursor.close()
    connection.close()