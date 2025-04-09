# query.py
import csv
import logging
from datetime import datetime

# --- Import Infrastructure Connectors ---
try:
    from infra.mongodb_connector import find_items, get_db, close_mongo_connection
    # from infra.redis_connector import get_redis_connection # Uncomment if you need Redis info
except ImportError:
    logging.error("Could not import from 'infra' module. Ensure it's in the Python path.")
    exit(1) # Exit if connectors are missing

# --- Configuration ---
MONGO_COLLECTION_NAME = 'testing_jobs' # Should match the collection used in settings.py/pipelines
OUTPUT_CSV_FILE = 'final_jobs.csv'

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def export_jobs_to_csv():
    """
    Fetches job data from MongoDB and exports it to a CSV file.
    """
    logging.info("Starting job export process...")

    # 1. Connect to MongoDB
    db = get_db()
    if not db:
        logging.error("Failed to connect to MongoDB. Aborting export.")
        return

    # 2. Retrieve Data
    logging.info(f"Fetching all items from MongoDB collection: '{MONGO_COLLECTION_NAME}'")
    # Using the reusable find_items function from the connector
    # Pass an empty query {} to find all documents
    try:
        all_jobs = find_items(query={}, collection_name=MONGO_COLLECTION_NAME)
    except Exception as e:
        logging.error(f"An error occurred while fetching data from MongoDB: {e}")
        close_mongo_connection() # Attempt to close connection on error
        return

    if not all_jobs:
        logging.warning(f"No jobs found in collection '{MONGO_COLLECTION_NAME}'. CSV file will be empty.")
        close_mongo_connection()
        # Create empty CSV with header? Or just exit? Let's create header.
        # return # Or proceed to write empty file with header

    logging.info(f"Retrieved {len(all_jobs)} jobs from MongoDB.")

    # 3. Prepare for CSV Export
    if not all_jobs: # Handle case where find_items returns empty list
        fieldnames = ['job_id', 'title', 'company', 'location', 'description', 'date_posted', 'url', 'source_file', 'scraped_timestamp'] # Default header
    else:
        # Dynamically determine headers from the first item (or union of keys)
        # Be cautious if items have varying structures
        fieldnames = list(all_jobs[0].keys())
        # Ensure common fields are first? Or sort alphabetically?
        # Let's remove MongoDB's internal '_id' if present
        if '_id' in fieldnames:
            fieldnames.remove('_id')
        # You might want to define a fixed order for columns:
        # fieldnames = ['job_id', 'title', 'company', 'location', 'date_posted', 'url', ...]

    logging.info(f"Exporting to CSV file: {OUTPUT_CSV_FILE} with headers: {fieldnames}")

    # 4. Write to CSV
    try:
        with open(OUTPUT_CSV_FILE, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore') # Ignore extra fields in data not in header

            writer.writeheader()
            for job in all_jobs:
                # MongoDB's _id is not usually needed in CSV, handled by extrasaction='ignore' if not removed from fieldnames
                # Convert datetime objects to ISO format string for CSV compatibility
                for key, value in job.items():
                    if isinstance(value, datetime):
                        job[key] = value.isoformat()
                writer.writerow(job)

        logging.info(f"Successfully exported {len(all_jobs)} jobs to {OUTPUT_CSV_FILE}")

    except IOError as e:
        logging.error(f"Error writing to CSV file {OUTPUT_CSV_FILE}: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during CSV export: {e}")

    # 5. Close Connections (Optional but good practice)
    close_mongo_connection()
    # Close Redis connection if used

if __name__ == "__main__":
    export_jobs_to_csv()