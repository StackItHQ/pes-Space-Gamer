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
conflict_priority = getenv("CONFLICT_PRIORITY")

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
    print(f"Spreadsheet {sheet} not found. Please ensure the spreadsheet exists and the service account has access to it")
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

# Last updated time of the sheet
last_sheet_update = 0
last_db_update = 0

# Start syncronisation
logger.info("Starting syncronisation")
try:
    while True:
        # Get the updated version of the sheet
        sh = gc.open(sheet)
        sh_updated = sh.lastUpdateTime
        print("Sheet updated at: ", sh_updated)

        # Get the  updated version of the database
        db_updated = cur.execute("SELECT max(last_updated) FROM candidates")
        db_updated = cur.fetchone()[0]

        db_deleted = cur.execute("SELECT max(last_updated) FROM deleted_candidates")
        db_deleted = cur.fetchone()[0]

        if db_updated and db_deleted:
            db_updated = max(db_updated, db_deleted)
        elif db_deleted:
            db_updated = db_deleted
        print("Database updated at: ", db_updated)

        if sh_updated != last_sheet_update and last_db_update == db_updated:    # If only the sheet has been updated since the last update
            logger.info("Sheet has been updated")
            # Do processing here
        elif sh_updated == last_sheet_update and last_db_update != db_updated:  # If only the database has been updated since the last update
            logger.info("Database has been updated")
            # Do processing here
        elif sh_updated != last_sheet_update and last_db_update != db_updated:  # If both the sheet and the database have been updated since the last update
            logger.info("Sheet and database have been updated")
            if conflict_priority == "Sheet":
                # Do processing here
                pass
            else:
                # Do processing here
                pass
        else:                                                                   # No updates
            pass

        # Update the last updated times
        last_sheet_update = sh_updated
        last_db_update = db_updated

        # Sleep for the update interval
        time.sleep(update_interval)
except KeyboardInterrupt:
    logger.info("Stopping syncronisation")
    conn.close()
    exit(0)
except Exception as e:
    logger.error(f"An error occured: {e}")
    conn.close()
    exit(1)
