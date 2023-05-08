from dotenv import load_dotenv
import os

load_dotenv()


# ESTAQUEUE CONECTION
IP_VPN = os.environ.get("IP_VPN")
USER_ESTAQUEUE = os.environ.get("USER_ESTAQUEUE")
PASSWORD_ESTAQUEUE = os.environ.get("PASSWORD_ESTAQUEUE")


# SALESFORCE CONECTION
SF_INSTANCE_URL = os.environ.get("SF_INSTANCE_URL")
SF_USERNAME = os.environ.get("SF_USERNAME")
SF_PASSWORD = os.environ.get("SF_PASSWORD")
SF_SECURITY_TOKEN = os.environ.get("SF_SECURITY_TOKEN")


# GOOGLE AUTH

GOOGLE_APPLICATION_CREDENTIALS_PATH = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS_PATH"
)
PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_ID = os.environ.get("DATASET_ID")
TABLE_ID = os.environ.get("TABLE_ID")
