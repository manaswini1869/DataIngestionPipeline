import csv
import logging
from datetime import datetime
import json

try:
    from infra.mongodb_connector import find_item, get_db, close_mongo_connection
except ImportError:
    logging.error("Could not import from 'infra' module. Ensure it's in the Python path.")
    exit(1)

# Configuration
MONGO_COLLECTION_NAME = 'testing_jobs'
OUTPUT_CSV_FILE = 'final_jobs.csv'

CSV_FIELDNAMES = [
    'req_id',
    'slug',
    'title',
    'brand',
    'employment_type',
    'language',
    'languages',
    'tags',
    'location_name',
    'street_address',
    'city',
    'state',
    'postal_code',
    'country_code',
    'full_location',
    'latitude',
    'longitude',
    'apply_url',
    'update_date',
    'create_date',
    'description'
]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

"""
Helper function to format values for CSV writing.
"""
def format_value_for_csv(value):
    if isinstance(value, list):
        return "|".join(str(item) for item in value if item is not None)
    elif isinstance(value, datetime):
        return value.isoformat()
    elif isinstance(value, (dict, bool)):
         try:
             return json.dumps(value)
         except TypeError:
             return str(value)
    elif value is None:
        return ""
    else:
        return str(value)

"""
    Fetches job data from MongoDB and exports selected fields to a CSV file.
"""
def export_jobs_to_csv():

    logging.info("Starting job export process...")

    db = get_db()
    if db is None:
        logging.error("Failed to connect to MongoDB. Aborting export.")
        return

    logging.info(f"Fetching all items from MongoDB collection: '{MONGO_COLLECTION_NAME}'")
    try:
        cursor = find_item(query={}, collection_name=MONGO_COLLECTION_NAME)
        all_jobs = list(cursor)
    except Exception as e:
        logging.error(f"An error occurred while fetching data from MongoDB: {e}", exc_info=True)
        close_mongo_connection()
        return

    if not all_jobs:
        logging.warning(f"No jobs found in collection '{MONGO_COLLECTION_NAME}'. Creating CSV with headers only.")
    else:
         logging.info(f"Retrieved {len(all_jobs)} jobs from MongoDB.")

    logging.info(f"Exporting to CSV file: {OUTPUT_CSV_FILE}")
    try:
        with open(OUTPUT_CSV_FILE, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=CSV_FIELDNAMES)

            writer.writeheader()

            for job_doc in all_jobs:
                row_data = {}
                for field in CSV_FIELDNAMES:
                    raw_value = job_doc.get(field, None)
                    row_data[field] = format_value_for_csv(raw_value)

                writer.writerow(row_data)

        logging.info(f"Successfully exported {len(all_jobs)} jobs to {OUTPUT_CSV_FILE}")

    except IOError as e:
        logging.error(f"Error writing to CSV file {OUTPUT_CSV_FILE}: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during CSV export: {e}", exc_info=True)

    close_mongo_connection()
    logging.info("MongoDB connection closed.")


if __name__ == "__main__":
    export_jobs_to_csv()