import os
import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery
from datetime import datetime
from flask import Flask, jsonify

app = Flask(__name__)

def scrape_url(url):
    """
    Scrapes the given URL and returns a list of data rows.
    Each row is a dictionary with keys:
      - party (STRING)
      - donation_amt (STRING)
      - donor (STRING)
      - donation_rcd_dt (DATE in YYYY-MM-DD)
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
    except Exception as e:
        error = f"Error retrieving {url}: {e}"
        print(error)
        return None, error

    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Locate the table (adjust the selector if needed)
    table = soup.select_one("div.table-responsive table.table")
    if not table:
        error = f"Data table not found on {url}."
        print(error)
        return None, error

    tbody = table.find("tbody")
    if not tbody:
        error = f"Table body not found on {url}."
        print(error)
        return None, error

    data_rows = []
    for row in tbody.find_all("tr"):
        cells = row.find_all("td")
        # Process only rows with exactly 5 cells (skip grouping headers)
        if len(cells) != 5:
            continue

        # Extract only the first four columns:
        party = cells[0].get_text(strip=True)
        donation_amt = cells[1].get_text(strip=True)
        donor = cells[2].get_text(strip=True)
        donation_date_str = cells[3].get_text(strip=True)

        # Convert donation date from "DD.MM.YYYY" to "YYYY-MM-DD"
        try:
            donation_date = datetime.strptime(donation_date_str, "%d.%m.%Y").strftime("%Y-%m-%d")
        except ValueError:
            donation_date = None

        row_data = {
            "party": party,
            "donation_amt": donation_amt,
            "donor": donor,
            "donation_rcd_dt": donation_date
        }
        data_rows.append(row_data)
    
    if not data_rows:
        error = f"No valid data rows found on {url}."
        print(error)
        return None, error

    return data_rows, None

def scrape_and_load_data():
    all_data = []

    # List of historical URLs (for years 2009–2024)
    historical_urls = [
        "https://www.bundestag.de/parlament/praesidium/parteienfinanzierung/fundstellen50000/2009",
        "https://www.bundestag.de/parlament/praesidium/parteienfinanzierung/fundstellen50000/2010",
        "https://www.bundestag.de/parlament/praesidium/parteienfinanzierung/fundstellen50000/2011",
        "https://www.bundestag.de/parlament/praesidium/parteienfinanzierung/fundstellen50000/2012",
        "https://www.bundestag.de/parlament/praesidium/parteienfinanzierung/fundstellen50000/2013",
        "https://www.bundestag.de/parlament/praesidium/parteienfinanzierung/fundstellen50000/2014",
        "https://www.bundestag.de/parlament/praesidium/parteienfinanzierung/fundstellen50000/2015",
        "https://www.bundestag.de/parlament/praesidium/parteienfinanzierung/fundstellen50000/2016",
        "https://www.bundestag.de/parlament/praesidium/parteienfinanzierung/fundstellen50000/2017",
        "https://www.bundestag.de/parlament/praesidium/parteienfinanzierung/fundstellen50000/2018",
        "https://www.bundestag.de/parlament/praesidium/parteienfinanzierung/fundstellen50000/2019",
        "https://www.bundestag.de/parlament/praesidium/parteienfinanzierung/fundstellen50000/2020",
        "https://www.bundestag.de/parlament/praesidium/parteienfinanzierung/fundstellen50000/2021",
        "https://www.bundestag.de/parlament/praesidium/parteienfinanzierung/fundstellen50000/2022",
        "https://www.bundestag.de/parlament/praesidium/parteienfinanzierung/fundstellen50000/2023",
        "https://www.bundestag.de/parlament/praesidium/parteienfinanzierung/fundstellen50000/2024"
    ]
    # URL for current data (2025)
    current_url = "https://www.bundestag.de/parlament/praesidium/parteienfinanzierung/fundstellen50000/2025"

    # Load historical data if RUN_HISTORICAL is set to "true" (run this once)
    if os.getenv("RUN_HISTORICAL", "false").lower() == "true":
        for url in historical_urls:
            print(f"Scraping historical data from: {url}")
            data, error = scrape_url(url)
            if error:
                print(f"Error scraping {url}: {error}")
            elif data:
                all_data.extend(data)

    # Always scrape current data (2025)
    print(f"Scraping current data from: {current_url}")
    data, error = scrape_url(current_url)
    if error:
        print(f"Error scraping {current_url}: {error}")
        return {"status": "error", "message": error}, 500
    elif data:
        all_data.extend(data)

    if not all_data:
        error = "No data collected from any URL."
        print(error)
        return {"status": "error", "message": error}, 500

    # BigQuery configuration
    project_id = os.getenv("GCP_PROJECT") or "your-project-id"
    dataset_id = os.getenv("BQ_DATASET") or "your_dataset"
    table_id = os.getenv("BQ_TABLE") or "your_table"
    bq_table = f"{project_id}.{dataset_id}.{table_id}"

    client = bigquery.Client(project=project_id)
    errors = client.insert_rows_json(bq_table, all_data)
    if errors:
        error_message = f"Encountered errors while inserting rows: {errors}"
        print(error_message)
        return {"status": "error", "message": error_message}, 500
    else:
        success_message = "Data inserted successfully into BigQuery."
        print(success_message)
        return {"status": "success", "message": success_message}, 200

@app.route("/", methods=["GET"])
def index():
    result, status_code = scrape_and_load_data()
    return jsonify(result), status_code

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))