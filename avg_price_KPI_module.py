import os
import json
from dash import html
from google.cloud import bigquery
from google.oauth2 import service_account

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/riccardokiho/PycharmProjects/REAL_ESTATE/service_account.json"

credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if not credentials_path:
    raise ValueError("The GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")

if not os.path.exists(credentials_path):
    raise FileNotFoundError(f"Credentials file not found at {credentials_path}")

credentials = service_account.Credentials.from_service_account_file(credentials_path)

# Set up the BigQuery client
project_id = credentials.project_id  # Extract project ID from credentials
client = bigquery.Client(credentials=credentials, project=project_id)



def fetch_average_price(table_name, province=None):
    query = f"SELECT ROUND(AVG(Price)) AS avg_price FROM `testingdaata.kv_real_estate.{table_name}` WHERE Price > 2700"
    if province:
        query += f" AND Province = '{province}'"
    query_job = client.query(query)
    result = query_job.result()
    return int(next(result).avg_price) if result.total_rows > 0 else 0


def get_latest_tables(dataset_id):
    dataset_ref = client.dataset(dataset_id)
    tables = list(client.list_tables(dataset_ref))
    weeks = sorted(
        (int(table.table_id.split('_week_')[-1]), table.table_id)
        for table in tables if '_week_' in table.table_id
    )
    latest_table = weeks[-1][1] if weeks else None
    previous_table = weeks[-2][1] if len(weeks) > 1 else None
    return latest_table, previous_table


latest_table, previous_table = get_latest_tables("kv_real_estate")


def average_price_kpi(selected_region=None):
    avg_price_current = fetch_average_price(latest_table, selected_region)
    avg_price_previous = fetch_average_price(previous_table, selected_region) if previous_table else 0
    change = avg_price_current - avg_price_previous
    sign = "+" if change > 0 else ("-" if change < 0 else "")

    return html.Div(
        children=[
            html.Div(
                f"Average Unit Price in {selected_region if selected_region else 'Estonia'}:",
                style={
                    "textAlign": "center",
                    "fontFamily": "Orbitron",
                    "fontSize": "24px",
                    "color": "#7E60BF",
                    "position": "absolute",
                    "top": "10px",
                    "left": "50%",
                    "transform": "translateX(-50%)",
                    "width": "100%",
                }
            ),
            html.Div(
                f"{avg_price_current} €",
                style={
                    "textAlign": "center",
                    "fontSize": "48px",
                    "fontWeight": "900",
                    "color": "#433878",
                    "fontFamily": "Orbitron",
                    "position": "absolute",
                    "top": "50%",
                    "left": "50%",
                    "transform": "translate(-50%, -50%)",
                    "whiteSpace": "nowrap",
                }
            ),
            html.Div(
                f"{sign}{abs(change)} € since last week" if change != 0 else "No change since last week",
                style={
                    "textAlign": "center",
                    "fontSize": "20px",
                    "fontWeight": "400",
                    "color": "green" if change >= 0 else "red",
                    "fontFamily": "Orbitron",
                    "position": "absolute",
                    "top": "75%",
                    "left": "50%",
                    "transform": "translate(-50%, -50%)",
                    "opacity": 0.3,
                    "whiteSpace": "nowrap"
                }
            ),
        ],
        style={
            "width": "454px",
            "height": "210px",
            "border": "5px solid #7E60BF",
            "borderRadius": "10px",
            "backgroundColor": "#FFE1FF",
            "position": "relative",
            "display": "flex",
            "justifyContent": "center",
            "alignItems": "center",
            "overflow": "hidden",
        }
    )


__all__ = ["average_price_kpi"]

