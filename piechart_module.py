import os
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.oauth2 import service_account
from dash import dcc

project_id = "testingdaata"
service_account_path = "service_account.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path
credentials = service_account.Credentials.from_service_account_file(service_account_path)
client = bigquery.Client(credentials=credentials, project=project_id)


def get_latest_table_name(dataset_id):
    dataset_ref = client.dataset(dataset_id)
    tables = list(client.list_tables(dataset_ref))
    weeks = sorted(
        (int(table.table_id.split('_week_')[-1]), table.table_id)
        for table in tables if '_week_' in table.table_id
    )
    return weeks[-1][1] if weeks else None


def fetch_property_age_data(province=None):
    latest_table_name = get_latest_table_name("kv_real_estate")
    full_table_name = f"{project_id}.kv_real_estate.{latest_table_name}"
    query = f"""
    SELECT
        CASE
            WHEN `Year Built` < 1920 THEN '<1920'
            WHEN `Year Built` BETWEEN 1920 AND 1950 THEN '1920-1950'
            WHEN `Year Built` BETWEEN 1951 AND 1980 THEN '1951-1980'
            WHEN `Year Built` BETWEEN 1981 AND 2000 THEN '1981-2000'
            WHEN `Year Built` > 2000 THEN '>=2001'
            ELSE 'Unknown'
        END AS built_year,
        COUNT(*) AS house_count
    FROM
        `{full_table_name}`
    """
    if province:
        query += f" WHERE Province = @province"
        query += " GROUP BY built_year ORDER BY built_year"
        params = [bigquery.ScalarQueryParameter("province", "STRING", province)]
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        query_job = client.query(query, job_config=job_config)
    else:
        query += " GROUP BY built_year ORDER BY built_year"
        query_job = client.query(query)

    result = query_job.result()
    rows = [dict(row) for row in result]
    return pd.DataFrame(rows)


def create_property_age_pie_chart(data, province=None):
    title = f"Listings by Built Year in<br> {province}" if province else "Listings by Built Year"
    fig = px.pie(
        data,
        names="built_year",
        height=450,
        width=350,
        values="house_count",
        color="built_year",
        color_discrete_map={
            '<1920': '#FFB3FF', '1920-1950': '#FF80FF', '1951-1980': '#B300B3',
            '1981-2000': '#6A0B9A', '>=2001': '#433878', 'Unknown': '#CCCCCC'
        }
    )
    fig.update_traces(
        textinfo='percent',
        textfont=dict(family="Orbitron", color="white"),
        hole=0.3,
        domain=dict(x=[0.08, 0.89], y=[0.2, 1]),
        textposition='inside',
        hovertemplate=(
            "Built Year: <b>%{label}</b><br>"
            "Units Listed: <b>%{value}</b>"
        ),
    )
    fig.update_layout(
        margin=dict(l=5, r=5, t=70, b=60),
        showlegend=True,
        legend=dict(
            orientation="h",
            y=0.1,
            x=0.48,
            xanchor="center",
            font=dict(family="Orbitron", size=14, color="#433878")
        ),
        hoverlabel=dict(
            bgcolor="#FFE1FF",
            font_size=12,
            font_family="Orbitron",
            align="left"
        ),
        title=dict(
            text=title,
            font=dict(size=18, color="#7E60BF", family="Orbitron"),
            y=0.95,
            x=0.5,
            xanchor="center",
            yanchor="top",
            automargin=True,
        ),
        paper_bgcolor='#FFE1FF',
        plot_bgcolor='#FFE1FF',
        height=450,
        width=330
    )
    return fig


def property_age_pie_chart(selected_region=None):
    data = fetch_property_age_data(selected_region)
    figure = create_property_age_pie_chart(data, selected_region)
    return dcc.Graph(
        figure=figure,
        style={
            "height": "450px",
            "width": "330px",
            "border": "5px solid #7E60BF",
            "borderRadius": "10px",
            "margin": "0",
            "padding": "0",
            "backgroundColor": "#FFE1FF",
            "boxSizing": "border-box",
            "overflow": "hidden",
        }
    )


__all__ = ["property_age_pie_chart"]

