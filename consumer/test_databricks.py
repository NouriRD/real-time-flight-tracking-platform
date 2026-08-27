import os

from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient


load_dotenv()

host = os.getenv("DATABRICKS_HOST")
token = os.getenv("DATABRICKS_TOKEN")

if not host or not token:
    raise ValueError("DATABRICKS_HOST or DATABRICKS_TOKEN is missing")

w = WorkspaceClient(
    host=host,
    token=token,
)

print("Testing Databricks connection...")

files = list(
    w.files.list_directory_contents(
        "/Volumes/flight_streaming/landing/raw_events"
    )
)

print("Connection successful!")
print(f"Files found: {len(files)}")

for file in files[:5]:
    print(file.path)