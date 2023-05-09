from google.cloud import bigquery
from google.oauth2 import service_account
from modules.environ import (
    GOOGLE_APPLICATION_CREDENTIALS_PATH,
    PROJECT_ID,
    DATASET_ID,
    TABLE_ID,
)
import io

# google_path = "../bq.json"

# csv_path = "C:\\Users\\zWork\\Documents\\GitHub\\etl_estaqueue\\iam\\informes\\informe_2023-05-04-2023-05-04.csv"


def load_data_to_bigquery(
    credentials_path,
    project_id,
    dataset_id,
    table_id,
    csv_file_path,
):
    print("BQ credentials location:" + credentials_path)
    # Configure your Google Cloud credentials
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path
    )
    client = bigquery.Client(credentials=credentials, project=project_id)

    # Create the job configuration
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_APPEND",  # Appends data to the table
    )

    # Get the table reference
    table_ref = client.dataset(dataset_id).table(table_id)

    # Open the CSV file and load the data into BigQuery
    with open(csv_file_path, mode="rb") as csv_data:
        load_job = client.load_table_from_file(
            csv_data, table_ref, job_config=job_config
        )
        load_job.result()  # Waits for the job to complete

    print(f"Data uploaded to {project_id}.{dataset_id}.{table_id}")


# load_data_to_bigquery()
