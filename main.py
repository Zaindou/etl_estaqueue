from modules.environ import IP_VPN, USER_ESTAQUEUE, PASSWORD_ESTAQUEUE
from modules.sf_conection import get_salesforce_records
from modules.sf_csv_process import merge_cdr_and_salesforce_data
from modules.utils import (
    get_userfield_ids,
    validate_and_correct_ids,
    establish_connection,
    read_config_file,
)
from modules.queue_csv_process import json_to_csv_cdr
import csv


config = read_config_file("config.txt")
fecha_inicial = config["fecha_inicial"]
fecha_final = config["fecha_final"]

params = {
    "CDRJSON": "1",
    "ip": IP_VPN,
    "FechaInicial": fecha_inicial,
    "FechaFinal": fecha_final,
    "user": USER_ESTAQUEUE,
    "password": PASSWORD_ESTAQUEUE,
}

url = "https://estaqueue.udpsa.com/estadisticasEntrada.php?"


def process_data(url, params):
    try:
        cdr_data = establish_connection(url, params=params).text
        json_to_csv_cdr(cdr_data, "process_files/cdr.csv")

        with open("process_files/cdr.csv", "r") as input_file, open(
            "process_files/output_filtered.csv", "w", newline=""
        ) as output_file:
            reader = csv.DictReader(input_file)
            fieldnames = reader.fieldnames
            writer = csv.DictWriter(output_file, fieldnames=fieldnames)
            writer.writeheader()
            for row in reader:
                writer.writerow(row)

        userfield_ids = get_userfield_ids("process_files/output_filtered.csv")
        corrected_userfield_ids = validate_and_correct_ids(userfield_ids)

        salesforce_query = "SELECT Id, Name, ID_Cliente__c, Operado_Por__c FROM contact WHERE Id IN ({})"
        salesforce_records = get_salesforce_records(
            salesforce_query, corrected_userfield_ids
        )

        merge_cdr_and_salesforce_data(
            "process_files/output_filtered.csv",
            salesforce_records,
            f"iam/informes/informe_{fecha_inicial}-{fecha_final}.csv",
        )

        print("Process finished successfully")
    except Exception as e:
        error_message = f"Error: {str(e)}\n"
        with open(f"iam/logs/process_log-{fecha_final}.txt", "a") as log_file:
            log_file.write(error_message)


process_data(url, params)
