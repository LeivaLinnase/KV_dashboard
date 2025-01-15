import os
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from dash import dcc

# BigQuery Setup
project_id = "testingdaata"
service_account_path = "service_account.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path
credentials = service_account.Credentials.from_service_account_file(service_account_path)
client = bigquery.Client(credentials=credentials, project=project_id)


# Function to get the latest table name from BigQuery
def get_latest_table_name(dataset_id):
    dataset_ref = client.dataset(dataset_id)
    tables = list(client.list_tables(dataset_ref))
    weeks = sorted(
        (int(table.table_id.split('_week_')[-1]), table.table_id)
        for table in tables if '_week_' in table.table_id
    )
    return weeks[-1][1] if weeks else None


# Fetch Data Function
def fetch_data(province=None):
    latest_table_name = get_latest_table_name("kv_real_estate")
    full_table_name = f"{project_id}.kv_real_estate.{latest_table_name}"

    if province:
        query = f"""
        SELECT
            CAST(FLOOR(Floor) AS INT) AS floor_number,
            ROUND(AVG(`Price per SqM`)) AS avg_price_per_sqm,
            COUNT(*) AS listings_count
        FROM
            `{full_table_name}`
        WHERE
            FLOOR(Floor) IS NOT NULL AND LENGTH(CAST(FLOOR(Floor) AS STRING)) <= 3
            AND Province = @province
        GROUP BY
            floor_number
        ORDER BY
            floor_number;
        """
        params = [bigquery.ScalarQueryParameter("province", "STRING", province)]
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        query_job = client.query(query, job_config=job_config)
    else:
        query = f"""
        SELECT
            CAST(FLOOR(Floor) AS INT) AS floor_number,
            ROUND(AVG(`Price per SqM`)) AS avg_price_per_sqm,
            COUNT(*) AS listings_count
        FROM
            `{full_table_name}`
        WHERE
            FLOOR(Floor) IS NOT NULL AND LENGTH(CAST(FLOOR(Floor) AS STRING)) <= 3
        GROUP BY
            floor_number
        ORDER BY
            floor_number;
        """
        query_job = client.query(query)

    result = query_job.result()
    rows = [dict(row) for row in result]
    data = pd.DataFrame(rows)

    if not data.empty:
        data = data.sort_values("floor_number")
        data["floor_number"] = data["floor_number"].astype(str)
    return data


def create_horizontal_bar_chart(data, province=None):
    title = f"Avg. Sqm Price by Floor<br> {province}" if province else "Avg. Sqm Price by Floor"

    # Dynamically adjust bar height based on the number of bars
    num_bars = len(data)
    chart_height = max(450, num_bars * 20)  # Ensure minimum chart height for fewer bars

    fig = px.bar(
        data,
        x="avg_price_per_sqm",
        y="floor_number",
        orientation="h",
        labels={
            "floor_number": "Floor Number",
            "avg_price_per_sqm": "Avg Price per SqM (€)"
        },
        title=title,
        height=chart_height,  # Dynamically adjust chart height
        width=350
    )
    fig.update_traces(
        texttemplate="%{x} €",
        textposition="inside",
        insidetextanchor="end",
        textfont=dict(color="white", size=16),
        marker_color="#433878",
        width=0.8,  # Keep the bar width constant
        hovertemplate=(
            "Average sqm price on floor %{y}:<br>"
            "<b>%{x} €</b><br>"
            "Listings count on floor %{y}: <b>%{customdata[0]}.</b>"
        ),
        customdata=data[["listings_count"]].values,
    )
    fig.update_layout(
        dragmode=False,
        hoverlabel=dict(
            bgcolor="#FFE1FF",
            font_size=12,
            font_family="Orbitron",
            align="left"
        ),
        title_font=dict(size=18, family="Orbitron", color="#7E60BF"),
        paper_bgcolor="#FFE1FF",
        plot_bgcolor="#FFE1FF",
        font=dict(family="Orbitron"),
        margin=dict(l=20, r=20, t=50, b=10),  # Adjust margins for title and bars
        xaxis=dict(
            showticklabels=False, showgrid=False, zeroline=False, title=None,
            fixedrange=True
        ),
        yaxis=dict(
            title=None,
            categoryorder="array",
            categoryarray=data["floor_number"] if not data.empty else [],
            showgrid=False,
            fixedrange=True
        ),
    )
    return fig


def horizontal_bar_chart_component(selected_region=None):
    data = fetch_data(selected_region)
    figure = create_horizontal_bar_chart(data, selected_region)
    return dcc.Graph(
        figure=figure,
        config={
            "scrollZoom": False,
            "displayModeBar": False,
            "doubleClick": False
        },
        style={
            "height": "450px",
            "width": "340px",
            "border": "5px solid #7E60BF",
            "borderRadius": "10px",
            "margin": "0",
            "padding": "0",
            "backgroundColor": "#FFE1FF",
            "boxSizing": "border-box",
            "overflow": "hidden",
        }
    )


__all__ = ["horizontal_bar_chart_component"]
