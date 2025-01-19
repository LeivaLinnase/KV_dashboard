from google.cloud import bigquery
from google.oauth2 import service_account

# Load credentials from the service account JSON file
credentials_path = "/Users/riccardokiho/PycharmProjects/REAL_ESTATE/service_account.json"
credentials = service_account.Credentials.from_service_account_file(credentials_path)

# Initialize the BigQuery client with explicit credentials
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Updated query to reference the correct dataset
query = """
SELECT
  COUNT(*)
FROM
  `testingdaata.kv_real_estate.INFORMATION_SCHEMA.TABLES`
"""

try:
    query_job = client.query(query)
    results = query_job.result()
    print(f"Query succeeded! Row count: {list(results)}")
except Exception as e:
    print(f"Error: {e}")

