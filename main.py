import logging
import time
from os import getenv

import gspread
import psycopg2
from dotenv import load_dotenv

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
handler = logging.FileHandler("logs.log", encoding='utf-8', mode='a')
handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s'))
logger.addHandler(handler)

# Load environment variables
load_dotenv()
sheet = getenv("SHEET")
db_host = getenv("DB_HOST")
db_user = getenv("DB_USER")
db_pass = getenv("DB_PASS")
db_name = getenv("DB_NAME")
update_interval = int(getenv("UPDATE_INTERVAL"))

# Connect to the Google Sheets API
try:
    gc = gspread.service_account(filename="service_account.json")
except FileNotFoundError:
    print("Service account file not found")
    exit(1)
except:
    print("Error loading service account. Please ensure the service account file is valid")
    exit(1)

try:
    sh = gc.open(sheet)
except gspread.SpreadsheetNotFound:
    print(f"Spreadsheet {sheet} not found")
    exit(1)

# Connect to the database
try:
    conn = psycopg2.connect(
        host=db_host,
        user=db_user,
        password=db_pass,
        dbname=db_name
    )
except psycopg2.OperationalError:
    print("Error connecting to the database")
    exit(1)

cur = conn.cursor()

# Start syncronisation
logging.info("Starting syncronisation")
try:
    while True:
        # Do processing here

        # Sleep for the update interval
        time.sleep(update_interval)
except KeyboardInterrupt:
    logging.info("Stopping syncronisation")
    conn.close()
    exit(0)
except Exception as e:
    logging.error(f"An error occured: {e}")
    conn.close()
    exit(1)
