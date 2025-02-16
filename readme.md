# DON_SCRAPE

This project scrapes data from a Bundestag webpage and inserts it into a BigQuery table. The application is built using Flask and is designed to run on Google Cloud Run.

## Project Structure

DON_SCRAPE/
├── Dockerfile
├── main.py
├── requirements.txt
├── README.md
└── .gitignore

## Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd DON_SCRAPE


2. Set Up the Development Environment

Using a Python Virtual Environment (venv)

It’s recommended to use a Python virtual environment for local development.
	•	Create the virtual environment:

    python -m venv venv

	•	Activate the virtual environment:
	•	macOS/Linux:
    source venv/bin/activate

    	•	Install Dependencies:

        pip install -r requirements.txt

3. Configure Environment Variables

The application expects the following environment variables:
	•	GCP_PROJECT: tali-448712
	•	BQ_DATASET: donations
	•	BQ_TABLE: The BigQuery table name.

For local testing, set these in your shell: