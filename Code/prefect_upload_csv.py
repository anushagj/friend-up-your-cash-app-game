import csv
import os
from google.cloud import bigquery
from prefect import task, Flow, Parameter

#TO BE UPDATED BY YOU
PROJECT_ID = "ghc23-394604"
DATASET_NAME = "Friends"
TABLE_NAME = "cash_friends3"

#TO BE UPDATED BY YOU
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/content/ghc23/ghc23-394604-07e797cf0921.json"


# Function to create a new table in BigQuery
def create_table(project_id, dataset_name, table_name):
    client = bigquery.Client(project=project_id)

    # Define the schema for your table (change the fields accordingly)
    schema = [
    bigquery.SchemaField("user_id", "STRING"),
    bigquery.SchemaField("age_on_cash_app", "INTEGER"),
    bigquery.SchemaField("gender", "STRING"),
    bigquery.SchemaField("transaction_count", "INTEGER"),
    bigquery.SchemaField("total_amount_ever_spent", "INTEGER"),
    bigquery.SchemaField("current_amount", "INTEGER"),
    bigquery.SchemaField("bitcoin_holdings", "FLOAT"),
    bigquery.SchemaField("stock_holdings", "INTEGER"),
    bigquery.SchemaField("cash_card_usage", "STRING"),
    bigquery.SchemaField("direct_deposit", "STRING"),
    bigquery.SchemaField("cash_boost_used", "STRING"),
    bigquery.SchemaField("account_creation_date", "STRING"),
    bigquery.SchemaField("cashtag", "STRING"),
    bigquery.SchemaField("most_interacted_user_id", "STRING"),
    bigquery.SchemaField("occupation", "STRING"),
    bigquery.SchemaField("location", "STRING"),
    bigquery.SchemaField("most_used_cash_app_feature", "STRING")
]

    table_ref = client.dataset(dataset_name).table(table_name)
    table = bigquery.Table(table_ref, schema=schema)

    # Create the table
    table = client.create_table(table)
    print(f"Table {table.project}.{table.dataset_id}.{table.table_id} created.")


# Function to upload CSV data to BigQuery table
def upload_csv_to_bigquery(csv_file_path, project_id, dataset_name, table_name):
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)

    with open(csv_file_path, "r") as f:
        # Assuming the first row contains the column headers
        reader = csv.DictReader(f)
        rows_to_insert = [row for row in reader]

    errors = client.insert_rows_json(table_ref, rows_to_insert)
    if errors:
        raise ValueError(f"Error uploading data to BigQuery: {errors}")

# Call the functions to create the table and upload data from the CSV file
if __name__ == "__main__":
    # Path to your CSV file
    csv_file_path = "CashFriends.csv"

    # Create the table (only needed if the table doesn't already exist)
    create_table(PROJECT_ID, DATASET_NAME, TABLE_NAME)

    # Upload the CSV data to the table
    upload_csv_to_bigquery(csv_file_path, PROJECT_ID, DATASET_NAME, TABLE_NAME)