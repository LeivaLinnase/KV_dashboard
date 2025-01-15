from dash import Dash, html, dcc, Input, Output
from flask_caching import Cache
from total_listings_KPI_module import total_listings_kpi
from avg_price_KPI_module import average_price_kpi
from avg_price_sqm_KPI_module import average_price_per_sqm_kpi
from heatmap_module import create_heatmap, heatmap_component
from piechart_module import property_age_pie_chart
from barchart_module import horizontal_bar_chart_component

app = Dash(__name__)
server = app.server

# Configure server-side caching for performance improvement
cache = Cache(server, config={
    'CACHE_TYPE': 'simple',
    'CACHE_DEFAULT_TIMEOUT': 300
})

app.layout = html.Div([
    html.H1("Estonian Condominium Listings by KV.ee", style={
        "textAlign": "center",
        "fontFamily": "Orbitron",
        "fontSize": "36px",
        "color": "#FFE1FF",
        "margin": "0 0 10px 0",
    }),
    # KPI Section
    html.Div([
        html.Div(id="total-listings-kpi-container"),
        html.Div(id="avg-price-kpi", children=average_price_kpi(None)),
        html.Div(id="avg-price-sqm-kpi", children=average_price_per_sqm_kpi(None)),
    ], style={
        "display": "flex",
        "justifyContent": "space-between",
        "gap": "10px",
        "marginBottom": "10px",
        "padding": "0 10px",
    }),
    # Chart Section
    html.Div([
        html.Div(heatmap_component(), id="heatmap-container", style={
            "width": "700px",
            "height": "450px",
            "marginRight": "10px",
        }),
        html.Div(id="pie-chart-container", children=property_age_pie_chart(None), style={
            "width": "350px",
            "height": "450px",
            "marginLeft": "10px",
        }),
        html.Div(id="bar-chart-container", children=horizontal_bar_chart_component(None), style={
            "width": "350px",
            "height": "450px",
        }),
    ], style={
        "display": "flex",
        "justifyContent": "space-between",
        "gap": "10px",
        "padding": "0 10px",
    })
], style={
    "backgroundColor": "#433878",
    "padding": "10px",
    "border": "5px solid #E4B1F0",
    "borderRadius": "10px",
    "boxSizing": "border-box",
    "width": "1460px",
    "margin": "0 auto",
    "overflow": "hidden",
})


@app.callback(
    Output("total-listings-kpi-container", "children"),
    Input("heatmap", "clickData"),
    memoize=True  # Use memoization to cache function results based on inputs
)
@cache.memoize()  # Use Flask-Caching here to cache the output based on the click_data input
def update_total_listings_kpi(click_data):
    selected_region = click_data["points"][0]["location"] if click_data and "points" in click_data else None
    return total_listings_kpi(selected_region)


@app.callback(
    Output("heatmap", "figure"),
    Input("heatmap", "clickData")
)
def update_heatmap(click_data):
    selected_region = click_data["points"][0]["location"] if click_data and "points" in click_data else None
    return create_heatmap(selected_region)


@app.callback(
    Output("avg-price-kpi", "children"),
    Input("heatmap", "clickData")
)
def update_avg_price_kpi(click_data):
    selected_region = click_data["points"][0]["location"] if click_data and "points" in click_data else None
    return average_price_kpi(selected_region)


@app.callback(
    Output("avg-price-sqm-kpi", "children"),
    Input("heatmap", "clickData")
)
def update_avg_price_sqm_kpi(click_data):
    selected_region = click_data["points"][0]["location"] if click_data and "points" in click_data else None
    return average_price_per_sqm_kpi(selected_region)


@app.callback(
    Output("pie-chart-container", "children"),
    Input("heatmap", "clickData")
)
def update_pie_chart(click_data):
    selected_region = click_data["points"][0]["location"] if click_data and "points" in click_data else None
    return property_age_pie_chart(selected_region)


@app.callback(
    Output("bar-chart-container", "children"),
    Input("heatmap", "clickData")
)
def update_bar_chart(click_data):
    selected_region = click_data["points"][0]["location"] if click_data and "points" in click_data else None
    return horizontal_bar_chart_component(selected_region)


if __name__ == "__main__":
    app.run_server(debug=True)


