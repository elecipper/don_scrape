import os
import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery
from flask import Flask, jsonify

app = Flask(__name__)

def scrape_and_load_data():
    # URL to scrape
    url = "https://www.bundestag.de/parlament/praesidium/parteienfinanzierung/fundstellen50000/2025/2025-inhalt-1032412"

    # Get the page
    try:
        response = requests.get(url)
        response.raise_for_status()
    except Exception as e:
        error = f"Error retrieving the URL: {e}"
        print(error)
        return {"status": "error", "message": error}, 500

    # Parse HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table')
    if not table:
        error = "No table found on the page."
        print(error)
        return {"status": "error", "message": error}, 500

    # Extract table headers
    headers = []
    header_row = table.find('tr')
    if header_row:
        headers = [th.get_text(strip=True) for th in header_row.find_all('th')]
    else:
        error = "No header row found in the table."
        print(error)
        return {"status": "error", "message": error}, 500

    # Extract table rows
    data_rows = []
    for row in table.find_all('tr')[1:]:  # Skip header row
        cells = row.find_all('td')
        if len(cells) == len(headers):
            row_data = {headers[i]: cells[i].get_text(strip=True) for i in range(len(headers))}
            data_rows.append(row_data)
        else:
            print("Skipping a row due to mismatched number of columns.")
    
    if not data_rows:
        error = "No data rows found to insert."
        print(error)
        return {"status": "error", "message": error}, 500

    # BigQuery setup: use environment variables or default values.
    project_id = os.getenv("GCP_PROJECT") or "your-project-id"
    dataset_id = os.getenv("BQ_DATASET") or "your_dataset"
    table_id = os.getenv("BQ_TABLE") or "your_table"
    bq_table = f"{project_id}.{dataset_id}.{table_id}"

    # Create BigQuery client and insert data.
    client = bigquery.Client(project=project_id)
    errors = client.insert_rows_json(bq_table, data_rows)
    if errors:
        error = f"Encountered errors while inserting rows: {errors}"
        print(error)
        return {"status": "error", "message": error}, 500
    else:
        success = "Data inserted successfully into BigQuery."
        print(success)
        return {"status": "success", "message": success}, 200

@app.route("/", methods=["GET"])
def index():
    result, status_code = scrape_and_load_data()
    return jsonify(result), status_code

if __name__ == "__main__":
    # When running locally, use port 8080
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))