from modules.environ import (
    IP_VPN,
    USER_ESTAQUEUE,
    PASSWORD_ESTAQUEUE,
    GOOGLE_APPLICATION_CREDENTIALS_PATH,
    PROJECT_ID,
    DATASET_ID,
    TABLE_ID,
)
from modules.sf_conection import get_salesforce_records
from modules.sf_csv_process import merge_cdr_and_salesforce_data

from modules.bigquery_conection import load_data_to_bigquery
from modules.utils import (
    get_userfield_ids,
    validate_and_correct_ids,
    establish_connection,
    read_config_file,
    create_log_file,
    write_log_message,
    get_last_processed_line,
    save_last_processed_line,
    get_paginated_data,
)
from modules.queue_csv_process import json_to_csv_cdr
import datetime
import csv
import json
import os


current_directory = os.path.dirname(os.path.realpath(__file__))
current_date = datetime.datetime.now().strftime("%Y-%m-%d")

params = {
    "CDRJSON": "1",
    "ip": IP_VPN,
    "FechaInicial": current_date,
    "FechaFinal": current_date,
    "user": USER_ESTAQUEUE,
    "password": PASSWORD_ESTAQUEUE,
}

url = "https://estaqueue.udpsa.com/estadisticasEntrada.php?"


def process_data(url, params):
    print(
        "Starting data processing..."
        + "\n"
        + f"Initial date: {current_date}"
        + " - "
        + f"End date: {current_date}"
    )

    log_file_name = create_log_file(
        os.path.join(current_directory, "iam/logs/process_log")
    )
    print(f"Log file created: {log_file_name}")
    last_processed_file = os.path.join(
        current_directory, "process_files/fileslast_processed.txt"
    )

    try:
        cdr_data_pages = get_paginated_data(url, params)

        for index, cdr_data in enumerate(cdr_data_pages):
            if index == 0:
                json_to_csv_cdr(
                    cdr_data,
                    os.path.join(current_directory, "process_files/cdr.csv"),
                    mode="w",
                )
            else:
                json_to_csv_cdr(
                    cdr_data,
                    os.path.join(current_directory, "process_files/cdr.csv"),
                    mode="a",
                )

        last_processed_line = get_last_processed_line(last_processed_file, current_date)

        with open(
            os.path.join(current_directory, "process_files/cdr.csv"), "r"
        ) as input_file, open(
            os.path.join(current_directory, "process_files/output_filtered.csv"),
            "w",
            newline="",
        ) as output_file:
            reader = csv.DictReader(input_file)
            fieldnames = reader.fieldnames
            writer = csv.DictWriter(output_file, fieldnames=fieldnames)
            writer.writeheader()

            for line_number, row in enumerate(reader, start=1):
                if line_number > last_processed_line:
                    writer.writerow(row)

        save_last_processed_line(last_processed_file, current_date, line_number)

        userfield_ids = get_userfield_ids("process_files/output_filtered.csv")
        corrected_userfield_ids = validate_and_correct_ids(userfield_ids)

        salesforce_query = "SELECT Id, Name, ID_Cliente__c, Operado_Por__c FROM contact WHERE Id IN ({})"
        salesforce_records = get_salesforce_records(
            salesforce_query, corrected_userfield_ids
        )

        merge_cdr_and_salesforce_data(
            os.path.join(current_directory, "process_files/output_filtered.csv"),
            salesforce_records,
            os.path.join(
                current_directory,
                f"iam/informes/informe_{current_date}-{current_date}.csv",
            ),
        )

        csv_file_path = os.path.join(
            current_directory, f"iam/informes/informe_{current_date}-{current_date}.csv"
        )

        load_data_to_bigquery(
            credentials_path=GOOGLE_APPLICATION_CREDENTIALS_PATH,
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            csv_file_path=csv_file_path,
        )

        print("Process finished successfully")
        write_log_message(
            log_file_name,
            f"Process finished successfully {current_date}-{current_date}",
        )
    except Exception as e:
        error_message = f"Error: {str(e)}\n"
        write_log_message(log_file_name, error_message)
        print(error_message)


process_data(url, params)
