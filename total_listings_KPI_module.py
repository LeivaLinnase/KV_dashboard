import os
import json
from dotenv import load_dotenv
from dash import html
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()
credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
if not credentials_json:
    raise ValueError("The GOOGLE_APPLICATION_CREDENTIALS_JSON environment variable is not set.")

credentials = service_account.Credentials.from_service_account_info(
    json.loads(credentials_json)
)

project_id = credentials.project_id
client = bigquery.Client(credentials=credentials, project=project_id)


def fetch_total_listings(table_name, province=None):
    query = f"SELECT COUNT(*) AS total_listings FROM `testingdaata.kv_real_estate.{table_name}`"
    if province:
        query += f" WHERE Province = '{province}'"
    query_job = client.query(query)
    result = query_job.result()
    return next(result).total_listings


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


def total_listings_kpi(selected_region=None):
    total_listings = fetch_total_listings(latest_table, selected_region)
    total_listings_previous = fetch_total_listings(previous_table, selected_region) if previous_table else 0
    change = total_listings - total_listings_previous
    sign = "+" if change >= 0 else "-"
    difference_text = f"{sign}{abs(change)} since last week"

    return html.Div(
        children=[
            html.Div(
                f"Total Listings in {selected_region if selected_region else 'Estonia'}:",
                style={
                    "textAlign": "center",
                    "fontFamily": "Orbitron",
                    "fontSize": "24px",
                    "color": "#7E60BF",
                    "position": "absolute",
                    "top": "10px",
                    "left": "50%",
                    "transform": "translateX(-50%)",
                    "margin": "0",
                    "padding": "0",
                    "width": "100%",
                }
            ),
            html.Div(
                f"{total_listings}",
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
                    "margin": "0",
                    "padding": "0",
                    "whiteSpace": "nowrap",
                }
            ),
            html.Div(
                difference_text,
                style={
                    "textAlign": "center",
                    "fontFamily": "Orbitron",
                    "fontSize": "20px",
                    "position": "absolute",
                    "top": "75%",
                    "left": "50%",
                    "transform": "translate(-50%, -50%)",
                    "color": "green" if change >= 0 else "red",
                    "fontWeight": "400",
                    "opacity": 0.3,
                    "whiteSpace": "nowrap"
                }
            )
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
            "margin": "0",
            "padding": "0",
            "overflow": "hidden",
        }
    )


__all__ = ["total_listings_kpi"]



