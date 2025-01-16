import pandas as pd
import re
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from google.cloud import bigquery
from google.oauth2 import service_account
import datetime
import json
from airflow.models import Variable

# Fetch GCP credentials from Airflow Variables
gcp_credentials_json = Variable.get("GCP_CREDENTIALS")
credentials = service_account.Credentials.from_service_account_info(
    json.loads(gcp_credentials_json)
)

project_id = credentials.project_id
dataset_id = "kv_real_estate"

client = bigquery.Client(credentials=credentials, project=project_id)

chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
# chrome_options.add_argument("--headless")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

base_url = "https://www.kv.ee/search?orderby=ob&deal_type=1"

listings = []


def get_total_listings(driver):
    driver.get(base_url)
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.sorter"))
        )
        total_listings_text = driver.find_element(By.XPATH, "//span[contains(text(), 'Kuulutusi leitud')]").text.strip()
        match = re.search(r'(\d[\d\s]*)', total_listings_text)
        if match:
            total_listings = int(match.group(1).replace(" ", ""))
            return total_listings
        else:
            print("No valid number found in total listings text.")
            return 0
    except Exception as e:
        print(f"Error locating total listings element: {e}")
        return 0


total_listings = get_total_listings(driver)
listings_per_page = 50
total_pages = (total_listings // listings_per_page) + (1 if total_listings % listings_per_page > 0 else 0)
print(f"Total listings: {total_listings}, Total pages: {total_pages}")

if total_listings > 0:
    for page in range(total_pages):
        start = page * listings_per_page
        url = f"{base_url}&start={start}"
        print(f"Scraping page {page + 1}: {url}")

        driver.get(url)

        try:
            listing_elements = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "article.default.object-type-apartment"))
            )
            for listing in listing_elements:
                try:
                    address_element = listing.find_element(By.CSS_SELECTOR, "h2 > a[data-skeleton='object']")
                    address = address_element.text.strip() if address_element else None

                    sqm_element = listing.find_element(By.CSS_SELECTOR, "div.area")
                    sqm = sqm_element.text.replace("\xa0", " ").replace("m²", "").replace(",", ".").strip() if sqm_element else None

                    rooms_element = listing.find_element(By.CSS_SELECTOR, "div.rooms")
                    rooms = rooms_element.text.strip() if rooms_element else None

                    price_element = listing.find_element(By.CSS_SELECTOR, "div.price")
                    price = price_element.text.split("€")[0].replace("\xa0", " ").replace(" ", "").strip() if price_element else None
                    price_per_sqm_element = price_element.find_element(By.TAG_NAME, "small") if price_element else None
                    price_per_sqm = (
                        price_per_sqm_element.text.replace("\xa0", " ").replace("€/m²", "").replace(" ", "").strip() if price_per_sqm_element else None
                    )

                    listings.append({
                        "Address": address,
                        "Sqm": sqm,
                        "Rooms": rooms,
                        "Price": price,
                        "Price per SqM": price_per_sqm,
                    })
                except Exception as e:
                    print(f"Error extracting data for a listing: {e}")
                    continue
        except Exception as e:
            print(f"Error locating listing elements on page {page + 1}: {e}")
else:
    print("No listings found. Exiting.")

if listings:
    df = pd.DataFrame(listings)

    df["Sqm"] = pd.to_numeric(df["Sqm"], errors='coerce')
    df["Rooms"] = pd.to_numeric(df["Rooms"], errors='coerce', downcast="integer")
    df["Price"] = pd.to_numeric(df["Price"], errors='coerce')
    df["Price per SqM"] = pd.to_numeric(df["Price per SqM"], errors='coerce')

    week_number = datetime.datetime.now().isocalendar()[1]
    table_name = f"listings_week_{week_number}"
    table_full_id = f"{project_id}.{dataset_id}.{table_name}"

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    try:
        job = client.load_table_from_dataframe(df, table_full_id, job_config=job_config)
        job.result()
        print(f"Data uploaded to BigQuery table: {table_full_id}")
    except Exception as e:
        print(f"Error uploading data to BigQuery: {e}")

driver.quit()

