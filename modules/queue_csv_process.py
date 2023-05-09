import json
import csv


def json_to_csv_cdr(json_data, output_file, mode="w"):
    data = json.loads(json_data)

    desired_columns = [
        "calldate",
        "src",
        "userfield",
        "accountcode",
        "dcontext",
        "lastapp",
    ]

    filtered_data = []
    for row in data["CDR"]:
        if row["userfield"]:
            filtered_row = {
                key: value for key, value in row.items() if key in desired_columns
            }
            filtered_data.append(filtered_row)

    with open(output_file, mode, newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=desired_columns)
        if mode == "w":
            writer.writeheader()
        writer.writerows(filtered_data)

    print(f"Generated CSV file: {output_file}")
