# KV(.ee) Real Estate Dashboard

A dynamic and interactive dashboard for analyzing real estate trends (condominium), designed as a portfolio project to demonstrate proficiency in data scraping, workflow orchestration with Apache Airflow, Google Cloud integration, and advanced Python programming skills.

---

## Features

- **Automated Data Collection**:
  - Weekly scraping of real estate listings from KV.ee using Selenium.
  - Workflow orchestrated with Apache Airflow for seamless automation.

- **Data Cleaning and Normalization**:
  - Data is cleaned, normalized, and structured during the scraping process using the Pandas library.

- **Real-Time Data Analysis**:
  - Data is stored in Google BigQuery for querying and analysis.
  - Dashboard modules fetch necessary data via integrated SQL queries.
  - Key metrics include:
    - Average Unit Price
    - Total Listings
    - Average Price per Sqm

- **Interactive UI**:
  - Dash-powered dashboard featuring:
    - A heatmap of provinces (listings count per province) acting as a filter.
    - KPIs for key metrics.
    - A pie chart showing listings by their age (Year Built).
    - A bar chart displaying average sqm price per floor.

---

## Workflow Overview

1. **Data Collection**:
   - Weekly scraping of real estate listings from KV.ee using Selenium.
   - Data is cleaned, normalized, and structured using Pandas.
   - Data is uploaded to Google BigQuery via the Python BigQuery API.
   - Automated workflow managed by Apache Airflow.

2. **Data Processing and Storage**:
   - Auction listings are filtered out, duplicates are dropped.

3. **Visualization**:
   - Interactive charts and tables built with Dash provide actionable insights.
   - Dashboard modules use SQL queries to fetch specific data on demand.

4. **Styling**:
   - A professional, user-friendly design implemented with Dash and CSS.

---

## Current State

- The project is operational with **weekly automation** via Airflow running locally.
- Data scraping, cleaning, and uploading to BigQuery works seamlessly when triggered.
- The Dash dashboard is integrated with BigQuery, offering real-time visualizations and insights.

---

## Future Enhancements

- **Full Cloud Automation**:
  - Deploy Apache Airflow and the Dash app to Google Cloud or AWS for continuous operation.

- **Public Deployment**:
  - Host the dashboard on a live platform (e.g., Google Cloud Run or Heroku) for public access.

---

## Contact

For questions or collaboration inquiries, feel free to connect:

- **Email**: riccardokiho05@gmail.com
- **GitHub**: [@LeivaLinnase](https://github.com/LeivaLinnase)

