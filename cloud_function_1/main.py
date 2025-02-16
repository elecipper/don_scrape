import os
import json
from google.cloud import bigquery, storage

def export_data_to_json(request):
    """
    HTTP Cloud Function.
    Queries BigQuery and writes the result as a JSON file to Cloud Storage.
    """
    # Get configuration from environment variables.
    project_id = os.environ.get('GCP_PROJECT', 'tali-448712')
    dataset_id = os.environ.get('BQ_DATASET', 'donations')
    table_id = os.environ.get('BQ_TABLE', 'parteispenden_reporting')
    bucket_name = os.environ.get('BUCKET_NAME', 'donations-data-export')
    file_name = os.environ.get('FILE_NAME', 'data.json')

    # Query BigQuery.
    bq_client = bigquery.Client(project=project_id)
    query = f"""
      SELECT party, donation_amt, donor, donation_rcd_dt
      FROM `{project_id}.{dataset_id}.{table_id}`
    """
    query_job = bq_client.query(query)
    results = query_job.result()

    # Convert the results to a list of dictionaries.
    rows = [dict(row) for row in results]
    json_data = json.dumps(rows, default=str)  # default=str handles dates

    # Upload JSON to Cloud Storage.
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(json_data, content_type='application/json')

    return f"Exported {len(rows)} rows to gs://{bucket_name}/{file_name}"


# Optional: for local testing, run a simple Flask app.
if __name__ == '__main__':
    from flask import Flask, request
    app = Flask(__name__)
    app.add_url_rule('/', 'export_data_to_json', export_data_to_json, methods=['GET', 'POST'])
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)