from tqdm import tqdm
import csv
import json
import requests
import time

import sys


def tqdm_bar(total, desc, bar_format, sleep_time=0.1):
    with tqdm(total=total, desc=desc, bar_format=bar_format) as pbar:
        for i in range(total):
            time.sleep(sleep_time)
            pbar.update(1)


def establish_connection(url, params=None, payload=None):
    tqdm_bar(100, "Conectando...", "{l_bar}{bar}| {n_fmt}/{total_fmt} ")

    response = requests.get(url, params=params, data=payload)

    if response.status_code == 200:
        tqdm_bar(
            100,
            "Conexión establecida, descargando información...",
            "{l_bar}{bar}| {n_fmt}/{total_fmt} ",
        )
        # print(response.json())
        return response
    else:
        print("\nConnection failed")
        print(response.status_code)
    return response


def json_to_csv(json_data, output_filename):
    tqdm_bar(100, "Generando archivo CSV...",
             "{l_bar}{bar}| {n_fmt}/{total_fmt} ")

    data = json.loads(json_data)
    cdr_data = data["CDR"]

    with open(output_filename, "w", newline="") as csvfile:
        fieldnames = list(cdr_data[0].keys())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for row in cdr_data:
            writer.writerow(row)

    print(f"Archivo CSV generado: {output_filename}")


url = "https://estaqueue.udpsa.com/estadisticasEntrada.php?"


# establish_connection(url, params=params)

json_to_csv(establish_connection(url, params=params).text, "cdr.csv")
