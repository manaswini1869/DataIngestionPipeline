import os
import logging
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

# adding logging formatting
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
MONGO_DB_NAME = os.getenv('MONGO_DB_NAME', 'data_ingestion_db')

mongo_client = None

'''
Function is used to connect the the MONGO_URI server running
For the application it is the mongo server running in the docker container
The function will check for an existing connection and will reuse if there is a connection
Otherwise, it will try to establish a new connection
Function will return None if there is an exception or if the connection timed out in 5000ms
'''
def get_mongo_client():
    global mongo_client
    if mongo_client is not None:
        try:
            mongo_client.admin.command('ping')
            logging.debug("Reusing existing connection")
            return mongo_client
        except (ConnectionFailure, ServerSelectionTimeoutError):
            logging.warning("Exiting MongoDB connection lost, trying to reconnect!")
            mongo_client = None
    try:
        logging.info(f"Trying to connect to the MongoDB at {MONGO_URI}")
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ismaster')
        mongo_client = client
        logging.info(f"Successfully connected to MongoDd: {MONGO_URI}")
        return mongo_client
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        logging.error(f"Failed to connect to MongoServer {MONGO_URI}: {e}")
        return None
    except Exception as e:
        logging.error(f"Failed to connect to the MongoServer {MONGO_URI}: {e}")
        return None

'''
The function get the mention Mongo URI client and get the mentioned Mongo Db from the server
If any error during the process logs the exception
'''
def get_db():
    client = get_mongo_client()
    if client is not None:
        try:
            db = client[MONGO_DB_NAME]
            return db
        except Exception as e:
            logging.error(f"Failed to get database '{MONGO_DB_NAME}': '{e}'")
            return None
    else:
        logging.error("Cannot get database as MongoDB client is not available")
        return None

'''
Inserts the job dictionary into the specified collection
If successful return the result from pymongo.result.InsertOneResult else None
'''
def insert_item(item: dict, collection_name: str):
    db = get_db()
    if db is not None:
        try:
            collection = db[collection_name]
            result = collection.insert_one(item)
            logging.info(f"Inserted item with id {result.inserted_id} into {collection_name}")
            return result
        except Exception as e:
            logging.error(f"Failed to insert item into {collection_name}: {e}")
            return None
    return None

'''
Get the db from the server
Runs the query against the collection
return query response, if exception return empty list
'''
def find_item(query: dict, collection_name: str, projection: dict = None):
    db = get_db()
    items = []
    if db is not None:
        try:
            collection = db[collection_name]
            cursor = collection.find(query, projection)
            items = list(cursor)
            logging.debug(f"Found {len(items)} items in {collection_name} matching query")
        except Exception as e:
            logging.error(f"Failed to fin items in {collection_name}: {e}")
            return []
    return items

'''
Close the mongo server connection
'''
def close_mongo_connection():
    global mongo_client
    if mongo_client:
        mongo_client.close()
        mongo_client = None
        logging.info("MongoDB connection closed")

'''
Runs a sample test connection to the mongo server when the python file is directly ran
'''
if __name__ == "__main__":
    logging.info("Testing MongoDB connection")
    db_instance = get_db()

    if db_instance is not None:
        logging.info(f"Successfully obtained database instance: {db_instance.name}")
    else:
        logging.error("Failed to get database instance.")
    close_mongo_connection()