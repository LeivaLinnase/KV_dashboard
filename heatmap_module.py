import os
import json
from dotenv import load_dotenv
import pandas as pd
from dash import dcc
import plotly.graph_objects as go
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


def get_latest_table_name(dataset_id):
    dataset_ref = client.dataset(dataset_id)
    tables = list(client.list_tables(dataset_ref))
    weeks = sorted(
        (int(table.table_id.split('_week_')[-1]), table.table_id)
        for table in tables if '_week_' in table.table_id
    )
    return weeks[-1][1] if weeks else None


def fetch_data_from_bq():
    latest_table_name = get_latest_table_name("kv_real_estate")
    full_table_name = f"{project_id}.kv_real_estate.{latest_table_name}"
    query = f"""
    SELECT 
        Province, 
        COUNT(*) as listing_count 
    FROM 
        `{full_table_name}`  # Using the fully qualified table name
    GROUP BY 
        Province
    ORDER BY 
        listing_count DESC
    """
    query_job = client.query(query)
    result = query_job.result()
    rows = [dict(row) for row in result]
    return pd.DataFrame(rows)


with open("estonia_regions.geojson", "r") as geojson_file:
    estonia_geojson = json.load(geojson_file)

province_data = fetch_data_from_bq()

color_steps = [
    (0, 25, '#FFE1FF'),
    (26, 100, '#FFB3FF'),
    (101, 200, '#FF80FF'),
    (201, 500, '#B300B3'),
    (501, 2000, '#6A0B9A'),
    (2001, 9000, '#433878')
]


def get_color(value):
    for lower_bound, upper_bound, color in color_steps:
        if lower_bound <= value <= upper_bound:
            return color
    return "#FFFFFF"


province_data["color"] = province_data["listing_count"].apply(get_color)


def create_heatmap(selected_region=None):
    fig = go.Figure()
    for _, row in province_data.iterrows():
        is_selected = row["Province"] == selected_region

        fig.add_trace(go.Choroplethmapbox(
            geojson=estonia_geojson,
            locations=[row["Province"]],
            z=[10 if is_selected else 1],
            featureidkey="properties.MNIMI",
            colorscale=[[0, row["color"]], [1, row["color"]]],
            showscale=False,
            marker=dict(
                opacity=1.0 if is_selected else 0.6,
                line_width=4.0 if is_selected else 0.5,
                line_color="red" if is_selected else "black"
            ),
            hovertemplate=(
                f"Province: <b>{row['Province']}</b><br>"
                f"Listings: <b>{row['listing_count']}</b>"
                "<extra></extra>"
            )
        ))

    fig.update_layout(
        hoverlabel=dict(
            bgcolor="#FFE1FF",
            font_size=12,
            font_family="Orbitron",
            align="left"),
        mapbox=dict(
            style="carto-positron",
            zoom=6,
            center={"lat": 58.5953, "lon": 25.0136},
            pitch=0,
        ),
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        uirevision="locked"
    )

    return fig


def heatmap_component():
    return dcc.Graph(
        id="heatmap",
        figure=create_heatmap(),
        config={
            "scrollZoom": False,
            "displayModeBar": False
        },
        style={
            "width": "720px",
            "height": "450px",
            "border": "5px solid #7E60BF",
            "borderRadius": "10px",
            "margin": "0",
            "padding": "0",
            "boxSizing": "border-box",
            "overflow": "hidden",
        }
    )


__all__ = ["create_heatmap", "heatmap_component"]
