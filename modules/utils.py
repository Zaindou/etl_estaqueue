import time
import requests
import csv
import datetime

from tqdm import tqdm


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
    with open(f"iam/{filename}", "r") as file:
        for line in file.readlines():
            key, value = line.strip().split("=")
            config[key] = value
    return config


# LOG SYSTEM


def create_log_file(log_name):
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    log_filename = f"{log_name}_{current_date}.txt"
    with open(f"iam/logs/{log_filename}", "w") as log_file:
        log_file.write(f"Log created on {current_date}\n")
    return log_filename


def write_log_message(log_filename, message):
    with open(f"iam/logs/{log_filename}", "a") as log_file:
        log_file.write(
            f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}\n"
        )
