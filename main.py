import pandas as pd
import re
import os
import json
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

project_id = os.getenv("GOOGLE_PROJECT_ID")
dataset_id = "kv_real_estate"

credentials_info = {
    "type": os.getenv("GOOGLE_ACCOUNT_TYPE"),
    "project_id": os.getenv("GOOGLE_PROJECT_ID"),
    "private_key_id": os.getenv("GOOGLE_PRIVATE_KEY_ID"),
    "private_key": os.getenv("GOOGLE_PRIVATE_KEY"),
    "client_email": os.getenv("GOOGLE_CLIENT_EMAIL"),
    "client_id": os.getenv("GOOGLE_CLIENT_ID"),
    "auth_uri": os.getenv("GOOGLE_AUTH_URI"),
    "token_uri": os.getenv("GOOGLE_TOKEN_URI"),
    "auth_provider_x509_cert_url": os.getenv("GOOGLE_AUTH_PROVIDER_CERT_URL"),
    "client_x509_cert_url": os.getenv("GOOGLE_CLIENT_CERT_URL"),
    "universe_domain": os.getenv("GOOGLE_UNIVERSE_DOMAIN")
}

if not all(credentials_info.values()):
    missing_keys = [key for key, value in credentials_info.items() if not value]
    raise ValueError(f"Missing environment variables for: {', '.join(missing_keys)}")


credentials = service_account.Credentials.from_service_account_info(credentials_info)
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

                    excerpt_element = listing.find_element(By.CSS_SELECTOR, "p.object-excerpt")
                    floor = None
                    year_built = None

                    if excerpt_element:
                        excerpt_text = excerpt_element.text.strip().replace("\xa0", " ")
                        floor_match = re.search(r"Korrus\s(\d+)/(\d+)", excerpt_text)  # Case: 'Korrus x/y'
                        if floor_match:
                            floor = f"{floor_match.group(1)}.{floor_match.group(2)}"
                        else:
                            floor_match_alternative = re.search(r"(\d+)\.\skorrus", excerpt_text)  # Case: 'x. korrus'
                            floor = f"{floor_match_alternative.group(1)}.00" if floor_match_alternative else None

                        year_match = re.search(r"ehitusaasta\s(\d{4})", excerpt_text)
                        year_built = year_match.group(1) if year_match else None

                    if not floor:
                        try:
                            description_element = listing.find_element(By.CSS_SELECTOR, "p.description-content")
                            if description_element:
                                description_text = description_element.text.strip()
                                secondary_floor_match = re.search(r"(\d+)\.\skorrusel", description_text)  # Case: 'x. korrusel'
                                floor = f"{secondary_floor_match.group(1)}.00" if secondary_floor_match else None
                        except Exception:
                            pass

                    price_element = listing.find_element(By.CSS_SELECTOR, "div.price")
                    price = price_element.text.split("€")[0].replace("\xa0", " ").replace(" ", "").strip() if price_element else None
                    price_per_sqm_element = price_element.find_element(By.TAG_NAME, "small") if price_element else None
                    price_per_sqm = (
                        price_per_sqm_element.text.replace("\xa0", " ").replace("€/m²", "").replace(" ", "").strip() if price_per_sqm_element else None
                    )

                    def remove_duplicates(address_parts):
                        seen = set()
                        return [x for x in address_parts if not (x in seen or seen.add(x))]

                    address_parts = address.split(',')
                    address_parts_cleaned = remove_duplicates([part.strip() for part in address_parts if part.strip()])
                    province = address_parts_cleaned[0] if len(address_parts_cleaned) > 0 else None
                    city = address_parts_cleaned[1] if len(address_parts_cleaned) > 1 else None
                    district = address_parts_cleaned[2] if len(address_parts_cleaned) > 2 else None
                    sub_district = address_parts_cleaned[3] if len(address_parts_cleaned) > 3 else None

                    street = address_parts_cleaned[-1] if len(address_parts_cleaned) > 0 else None

                    if district == street:
                        district = None
                    if sub_district == street:
                        sub_district = None

                    if len(address_parts_cleaned) == 5:
                        district = address_parts_cleaned[2]
                        sub_district = address_parts_cleaned[3]
                    elif len(address_parts_cleaned) == 4:
                        district = address_parts_cleaned[2]
                        sub_district = None
                    elif len(address_parts_cleaned) == 3:
                        district = None
                        sub_district = None
                    else:
                        district = None
                        sub_district = None

                    listings.append({
                        "Province": province,
                        "City": city,
                        "District": district,
                        "Sub-District": sub_district,
                        "Street": street,
                        "Sqm": sqm,
                        "Rooms": rooms,
                        "Floor": floor,
                        "Year Built": year_built,
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
    df["Year Built"] = pd.to_numeric(df["Year Built"], errors='coerce', downcast="integer")
    df["Price"] = pd.to_numeric(df["Price"], errors='coerce')
    df["Price per SqM"] = pd.to_numeric(df["Price per SqM"], errors='coerce')
    df["Floor"] = pd.to_numeric(df["Floor"], errors='coerce')

    week_number = datetime.datetime.now().isocalendar()[1]
    table_name = f"listings_week_{week_number}"

    table_full_id = f"{project_id}.{dataset_id}.{table_name}"

    schema = [
        bigquery.SchemaField("Province", "STRING", mode="NULLABLE",
                             description="The province where the property is located."),
        bigquery.SchemaField("City", "STRING", mode="NULLABLE", description="The city where the property is located."),
        bigquery.SchemaField("District", "STRING", mode="NULLABLE", description="The district within the city."),
        bigquery.SchemaField("Sub-District", "STRING", mode="NULLABLE",
                             description="The sub-district or neighborhood of the property."),
        bigquery.SchemaField("Street", "STRING", mode="NULLABLE", description="The street name of the property."),
        bigquery.SchemaField("Sqm", "FLOAT", mode="NULLABLE", description="The area of the property in square meters."),
        bigquery.SchemaField("Rooms", "INTEGER", mode="NULLABLE", description="The number of rooms in the property."),
        bigquery.SchemaField("Floor", "FLOAT", mode="NULLABLE",
                             description="The floor number of the property, can include fractions for a range."
                                         "{sold_property_floor}/{total_floors}"),
        bigquery.SchemaField("Year Built", "INTEGER", mode="NULLABLE",
                             description="The year when the property was constructed."),
        bigquery.SchemaField("Price", "FLOAT", mode="NULLABLE",
                             description="The total price of the property. Price is in euros(€)"),
        bigquery.SchemaField("Price per SqM", "FLOAT", mode="NULLABLE",
                             description="The price per square meter of the property. Price is in euros(€)"),
    ]

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=schema,
    )

    try:
        job = client.load_table_from_dataframe(df, table_full_id, job_config=job_config)
        job.result()
        if job.state != 'DONE':
            print(f"Job failed with error: {job.error_result}")
        else:
            print(f"Data uploaded to BigQuery table: {table_full_id}")

    except Exception as e:
        print(f"Error uploading data to BigQuery: {e}")

driver.quit()
