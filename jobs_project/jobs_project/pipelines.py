# jobs_project/jobs_project/pipelines.py
import logging
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

try:
    from infra.mongodb_connector import insert_item, get_db, close_mongo_connection
    from infra.redis_connector import get_redis_connection, add_to_set, is_member
except ImportError:
    logging.error("Could not import from 'infra' module. Ensure it's in the Python path.")

    def insert_item(*args, **kwargs): raise ImportError("infra.mongodb_connector missing")
    def get_db(*args, **kwargs): raise ImportError("infra.mongodb_connector missing")
    def close_mongo_connection(*args, **kwargs): pass
    # def get_redis_connection(*args, **kwargs): raise ImportError("infra.redis_connector missing")
    # def add_to_set(*args, **kwargs): raise ImportError("infra.redis_connector missing")
    # def is_member(*args, **kwargs): raise ImportError("infra.redis_connector missing")


# class RedisDeduplicationPipeline:
#     """
#     Pipeline for filtering out items that have already been seen, using Redis.
#     Requires 'DUPEFILTER_KEY' to be set in settings or spider custom_settings,
#     which specifies the item field to use for the unique ID (e.g., 'job_id', 'url').
#     """
#     def __init__(self, redis_conn, dupefilter_key_field, stats):
#         self.redis_conn = redis_conn
#         self.seen_set_key_template = "{spider_name}_seen_ids" # Template for Redis set key
#         self.dupefilter_key_field = dupefilter_key_field
#         self.stats = stats # Scrapy stats collector

#     @classmethod
#     def from_crawler(cls, crawler):
#         # Get settings
#         settings = crawler.settings
#         dupefilter_key_field = settings.get('DUPEFILTER_KEY', 'url') # Default to 'url' if not set

#         # Get Redis connection from infra module
#         redis_conn = get_redis_connection()
#         if not redis_conn:
#             logging.error("Redis connection failed. Deduplication pipeline will be disabled.")
#             # You might want to raise NotConfigured here if Redis is mandatory
#             # from scrapy.exceptions import NotConfigured
#             # raise NotConfigured("Redis connection failed")

#         # Pass crawler stats for tracking duplicates
#         pipeline = cls(redis_conn, dupefilter_key_field, crawler.stats)
#         return pipeline

#     def open_spider(self, spider):
#         if not self.redis_conn: return # Do nothing if connection failed

#         self.seen_set_key = self.seen_set_key_template.format(spider_name=spider.name)
#         logging.info(f"RedisDeduplicationPipeline: Using key '{self.seen_set_key}' for deduplication based on item field '{self.dupefilter_key_field}'.")

#     def process_item(self, item, spider):
#         if not self.redis_conn: return item # Pass through if no connection

#         adapter = ItemAdapter(item)
#         item_unique_id = adapter.get(self.dupefilter_key_field)

#         if not item_unique_id:
#             logging.warning(f"Item lacks unique identifier field '{self.dupefilter_key_field}'. Skipping deduplication check.", extra={'spider': spider})
#             return item # Allow item if no ID found or field missing

#         # Convert to string as Redis sets store strings
#         item_unique_id_str = str(item_unique_id)

#         # Check if the ID is already in the Redis set
#         if is_member(self.seen_set_key, item_unique_id_str):
#             self.stats.inc_value('redis/duplicate_items', spider=spider)
#             raise DropItem(f"Duplicate item found based on '{self.dupefilter_key_field}': {item_unique_id_str}")
#         else:
#             # Add the ID to the set
#             added = add_to_set(self.seen_set_key, item_unique_id_str)
#             if added:
#                 self.stats.inc_value('redis/new_items', spider=spider)
#             else:
#                 # This case (is_member was false but add_to_set fails/returns 0) might indicate a race condition or Redis issue
#                 logging.warning(f"Failed to add presumably new item ID '{item_unique_id_str}' to Redis set '{self.seen_set_key}'.", extra={'spider': spider})
#             return item # Item is unique (or failed to add), continue processing


class MongoDBPipeline:
    """
    Pipeline for storing Scrapy items in a MongoDB database.
    Requires 'MONGO_COLLECTION' to be set in settings or spider custom_settings.
    """
    def __init__(self, mongo_db, collection_name, stats):
        self.mongo_db = mongo_db
        self.collection_name = collection_name
        self.stats = stats

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        collection_name = settings.get('MONGO_COLLECTION', 'default_items') # Default collection name

        # Get DB instance from infra module
        mongo_db = get_db()
        if mongo_db is None:
             logging.error("MongoDB connection failed. MongoDB pipeline will be disabled.")
             # from scrapy.exceptions import NotConfigured
             # raise NotConfigured("MongoDB connection failed")

        pipeline = cls(mongo_db, collection_name, crawler.stats)
        return pipeline

    def open_spider(self, spider):
         if self.mongo_db is None: return # Do nothing if connection failed
         logging.info(f"MongoDBPipeline: Storing items in collection '{self.collection_name}'")
         # Optional: Create indexes if they don't exist
         try:
            self.mongo_db[self.collection_name].create_index("slug", unique=True) # Example index
         except Exception as e:
            logging.warning(f"Could not ensure index on {self.collection_name}: {e}")


    def close_spider(self, spider):
        # The global client connection is managed by the connector module
        # You might call close_mongo_connection() here if needed, but be careful
        # if multiple spiders/processes might use the same connection pool.
        pass

    def process_item(self, item, spider):
        if self.mongo_db is None: return item # Pass through if no connection

        adapter = ItemAdapter(item)
        item_dict = adapter.asdict()

        try:
            result = insert_item(item_dict, self.collection_name)
            if result and result.inserted_id:
                logging.debug(f"Item inserted into MongoDB collection {self.collection_name} with ID {result.inserted_id}", extra={'spider': spider})
                self.stats.inc_value('mongodb/inserted_items', spider=spider)
            else:
                 # This might happen if insert_item returns None due to connection issues inside
                 logging.error(f"Failed to insert item into MongoDB collection {self.collection_name}. Insert function returned None.", extra={'spider': spider})
                 self.stats.inc_value('mongodb/failed_inserts', spider=spider)

        except Exception as e:
            logging.error(f"Exception inserting item into MongoDB: {e}", extra={'spider': spider})
            self.stats.inc_value('mongodb/failed_inserts', spider=spider)
            # Decide how to handle failed inserts (e.g., drop item, log error)
            # Raising DropItem here might be too aggressive if it's a temporary DB issue
        return item # Return item for potential further processing