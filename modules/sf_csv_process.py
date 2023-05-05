from modules.utils import create_log_file, write_log_message
import csv


def merge_cdr_and_salesforce_data(input_file, salesforce_data, output_file):
    salesforce_dict = {
        record["Id"]: record
        for record in salesforce_data
        if isinstance(record, dict) and "Id" in record
    }

    combined_data = []

    log_file_name = create_log_file("sf_cdr_log")

    with open(input_file, "r") as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            userfield_id = row["userfield"]

            if userfield_id in salesforce_dict:
                combined_row = {**row, **salesforce_dict[userfield_id]}
                combined_data.append(combined_row)
            else:
                write_log_message(
                    log_file_name,
                    f"Userfield ID not found in Salesforce: {userfield_id}",
                )

    if combined_data:
        with open(output_file, "w", newline="") as outfile:
            fieldnames = [
                "tipo_call",
                "id_campana",
                "nombre_cliente",
                "identificacion",
                "resultado_maquina",
                "telefono",
                "fecha",
                "operado_por",
                "id_salesforce",
            ]

            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in combined_data:
                transformed_row = {
                    "tipo_call": row["dcontext"],
                    "id_campana": row["accountcode"],
                    "nombre_cliente": row["Name"],
                    "identificacion": row["ID_Cliente__c"],
                    "resultado_maquina": row["lastapp"],
                    "telefono": row["src"],
                    "fecha": row["calldate"],
                    "operado_por": row["Operado_Por__c"],
                    "id_salesforce": row["userfield"],
                }
                writer.writerow(transformed_row)
    else:
        print(
            "No combined data found. Please check if userfield IDs match Salesforce IDs."
        )
