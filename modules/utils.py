from tqdm import tqdm
import csv
import datetime
import json
import os
import requests
import time


def tqdm_bar(total, desc, bar_format, sleep_time=0.1):
    with tqdm(total=total, desc=desc, bar_format=bar_format) as pbar:
        for i in range(total):
            time.sleep(sleep_time)
            pbar.update(1)


def establish_connection(url, params=None, payload=None):
    tqdm_bar(100, "Connecting...", "{l_bar}{bar}| {n_fmt}/{total_fmt} ")

    response = requests.get(url, params=params, data=payload)

    if response.status_code == 200:
        tqdm_bar(
            100,
            "Connection established, downloading information...",
            "{l_bar}{bar}| {n_fmt}/{total_fmt} ",
        )
        return response
    else:
        print("\nConnection failed")
        print(response.status_code)
    return response


def get_userfield_ids(csv_path):
    unique_ids = set()
    with open(csv_path, "r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            userfield_id = row["userfield"]
            if len(userfield_id) >= 18:
                unique_ids.add(row["userfield"])
    return list(unique_ids)


def validate_and_correct_ids(ids):
    """
    The function validates and corrects IDs by truncating them to 18 characters if they are longer than
    18.

    :param ids: a list of strings representing IDs that need to be validated and corrected if necessary
    :return: a list of corrected IDs after validating and correcting them.
    """
    corrected_ids = []
    for id_ in ids:
        if len(id_) > 18:
            corrected_id = id_[:18]
            corrected_ids.append(corrected_id)
        else:
            corrected_ids.append(id_)
    return corrected_ids


def read_config_file(filename):
    config = {}
    with open(filename, "r") as file:
        for line in file.readlines():
            key, value = line.strip().split("=")
            config[key] = value
    return config


# LOG SYSTEM
def create_log_file(log_name):
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    log_filename = f"{log_name}_{current_date}.txt"
    with open(log_filename, "w") as log_file:
        log_file.write(f"Log created on {current_date}\n")
    return log_filename


def write_log_message(log_filename, message):
    with open(log_filename, "a") as log_file:
        log_file.write(
            f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}\n"
        )


# LAST PROCESSED LINE SYSTEM
def get_last_processed_line(file_path, date):
    try:
        with open(file_path, "r") as f:
            for line in f:
                if line.startswith(date):
                    return int(line.strip().split(" ")[-1])
    except FileNotFoundError:
        pass

    return 0


def save_last_processed_line(file_path, date, last_line):
    lines = []
    updated = False

    try:
        with open(file_path, "r") as f:
            lines = f.readlines()
            for i, line in enumerate(lines):
                if line.startswith(date):
                    lines[i] = f"{date} {last_line}\n"
                    updated = True
                    break
    except FileNotFoundError:
        pass

    if not updated:
        lines.append(f"{date} {last_line}\n")

    with open(file_path, "w") as f:
        f.writelines(lines)


# Teleamigo add pagination to the API, so we need to get the total number of pages.abs
def get_paginated_data(url, params):
    """
    The function retrieves paginated data from a specified URL using a specified set of parameters.

    :param url: The URL of the API endpoint to fetch data from
    :param params: a dictionary containing parameters to be passed in the API request
    :return: a list of CDR data pages.
    """
    page = 1
    cdr_data_pages = []

    while True:
        print(f"Fetching data for page {page}")
        params["Pagina"] = page
        response = establish_connection(url, params=params)
        if response.status_code == 200:
            data = response.text
            if not data or json.loads(data) == []:
                print(f"No data found on page {page}")
                break
            else:
                cdr_data_pages.append(data)
                print(f"Data fetched for page {page}")
        else:
            print(f"Failed to fetch data for page {page}")
            break

        page += 1

    return cdr_data_pages
