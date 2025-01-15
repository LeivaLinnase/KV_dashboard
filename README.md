# KV(.ee) Real Estate Dashboard

A dynamic and interactive dashboard for analyzing real estate trends, designed as a portfolio project to demonstrate proficiency in data visualization, Google Cloud integration and advanced Python programming skills.

---

## Features

- **Real-Time Data Analysis**:
  - Fetches real estate data directly from Google BigQuery.
  - Displays average condo unit prices and week-over-week changes.
  
- **Regional Insights**:
  - Filter and analyze data by specific provinces or view trends across all regions.

- **Interactive UI**:
  - Styled using Dash components for a clean and modern look.
  - Dynamic KPI cards highlighting critical metrics.

---

## Workflow Overview

1. **Data Collection**:
   - Data is scraped from website and directly stored into Google BigQuery
   - Data is stored in Google BigQuery with weekly updates for the latest trends.

2. **Data Processing**:
   - The application filters auction listings and calculates average prices using SQL queries directly within BigQuery.

3. **Visualization**:
   - KPIs such as **Average Unit Price** and **Price Changes Since Last Week** are dynamically generated.
   - Dash ensures that the dashboard remains responsive and interactive.

4. **Styling**:
   - A custom-styled interface built with Dash and CSS ensures an intuitive user experience.

---

## Key Functions

### `fetch_average_price(table_name, province=None)`
Calculates the average condo unit price while excluding auction listings (e.g., prices <= 2700). Allows optional filtering by province.

### `get_latest_tables(dataset_id)`
Identifies the most recent and previous data tables in Google BigQuery for comparative analysis.

### `average_price_kpi(selected_region=None)`
Generates a KPI card displaying:
- **Average Price**: For the selected region or all regions.
- **Change Since Last Week**: Highlights week-over-week price fluctuations.

---

## Future Enhancements

- **Additional KPIs**: Integrate metrics like median prices, total listings, and sales volume.
- **Advanced Filtering**: Add options to filter by property type, price range, and more.
- **Deployment**: Host the application on platforms like AWS, Google Cloud, or Heroku for public access.

---

## Contact

For questions or collaboration inquiries, feel free to connect:

- **Email**: your-email@example.com
- **GitHub**: [@your-username](https://github.com/your-username)

