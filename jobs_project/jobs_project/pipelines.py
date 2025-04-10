import logging
import json
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem, NotConfigured

# importing the mongodb and redis function to be re-used in the pipeline
try:
    from infra.mongodb_connector import insert_item, get_db, close_mongo_connection
    from infra.redis_connector import get_redis_connection, add_to_set, is_member
except ImportError as e:
    logging.error(f"Could not import from 'infra' module: {e}. Ensure it's in the Python path.")
    def insert_item(*args, **kwargs): raise ImportError("infra.mongodb_connector missing")
    def get_db(*args, **kwargs): raise ImportError("infra.mongodb_connector missing")
    def close_mongo_connection(*args, **kwargs): pass
    def get_redis_connection(*args, **kwargs): raise ImportError("infra.redis_connector missing")
    def add_to_set(*args, **kwargs): raise ImportError("infra.redis_connector missing")
    def is_member(*args, **kwargs): raise ImportError("infra.redis_connector missing")

"""
    Pipeline for filtering out items that have already been seen, using a Redis set.
"""
class RedisDeduplicationPipeline:
    def __init__(self, redis_conn, dupefilter_key_field, stats):
        self.redis_conn = redis_conn
        self.seen_set_key_template = "{spider_name}:seen_ids"
        self.dupefilter_key_field = dupefilter_key_field
        self.stats = stats
        self.seen_set_key = None

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        dupefilter_key_field = settings.get('DUPEFILTER_KEY_FIELD')

        if not dupefilter_key_field:
            raise NotConfigured("RedisDeduplicationPipeline requires the 'DUPEFILTER_KEY_FIELD' setting (e.g., 'slug', 'req_id').")

        try:
            redis_conn = get_redis_connection()
            if not redis_conn:
                logging.error("Redis connection failed via get_redis_connection(). Deduplication pipeline will be disabled.")
                raise NotConfigured("Redis connection failed")
        except ImportError:
             logging.error("infra.redis_connector missing or get_redis_connection failed. Deduplication pipeline disabled.")
             raise NotConfigured("infra.redis_connector missing or connection failed")
        except Exception as e:
            logging.error(f"Error getting Redis connection: {e}. Deduplication pipeline disabled.")
            raise NotConfigured(f"Redis connection error: {e}")


        pipeline = cls(redis_conn, dupefilter_key_field, crawler.stats)
        return pipeline

    def open_spider(self, spider):
        self.seen_set_key = self.seen_set_key_template.format(spider_name=spider.name)
        logging.info(f"RedisDeduplicationPipeline: Using key '{self.seen_set_key}' for deduplication based on item field '{self.dupefilter_key_field}'.")

    def close_spider(self, spider):
        try:
            logging.info("Redis connection closed for deduplication pipeline.")
        except Exception as e:
             logging.warning(f"Error closing Redis connection: {e}")


    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        item_unique_id = adapter.get(self.dupefilter_key_field)

        if item_unique_id is None:
            logging.warning(f"Item lacks unique identifier field '{self.dupefilter_key_field}' or value is None. Skipping deduplication check.", extra={'spider': spider})
            self.stats.inc_value('redis/skipped_no_id', spider=spider)
            return item
        item_unique_id_str = str(item_unique_id)

        try:
            already_seen = is_member(self.seen_set_key, item_unique_id_str)

            if already_seen:
                self.stats.inc_value('redis/duplicate_items', spider=spider)
                raise DropItem(f"Duplicate item found based on '{self.dupefilter_key_field}': {item_unique_id_str}")
            else:
                added = add_to_set(self.seen_set_key, item_unique_id_str)
                if added:
                    self.stats.inc_value('redis/new_items', spider=spider)
                else:
                    logging.warning(f"Failed to add presumably new item ID '{item_unique_id_str}' to Redis set '{self.seen_set_key}'. Possible race condition or Redis error.", extra={'spider': spider})
                    self.stats.inc_value('redis/add_failed', spider=spider)

                return item

        except Exception as e:
             logging.error(f"Redis error during deduplication check/add for ID '{item_unique_id_str}' in set '{self.seen_set_key}': {e}", extra={'spider': spider})
             self.stats.inc_value('redis/errors', spider=spider)
             return item

"""
    Pipeline for storing Scrapy items in a MongoDB database.
"""
class MongoDBPipeline:
    def __init__(self, mongo_db, collection_name, stats):
        self.mongo_db = mongo_db
        self.collection_name = collection_name
        self.stats = stats

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        collection_name = settings.get('MONGO_COLLECTION')

        if not collection_name:
             raise NotConfigured("MongoDBPipeline requires the 'MONGO_COLLECTION' setting.")

        try:
            mongo_db = get_db()
            if mongo_db is None:
                logging.error("MongoDB connection failed via get_db(). MongoDB pipeline will be disabled.")
                raise NotConfigured("MongoDB connection failed")
        except ImportError:
             logging.error("infra.mongodb_connector missing or get_db failed. MongoDB pipeline disabled.")
             raise NotConfigured("infra.mongodb_connector missing or connection failed")
        except Exception as e:
             logging.error(f"Error getting MongoDB connection: {e}. MongoDB pipeline disabled.")
             raise NotConfigured(f"MongoDB connection error: {e}")


        pipeline = cls(mongo_db, collection_name, crawler.stats)
        return pipeline

    def open_spider(self, spider):
        logging.info(f"MongoDBPipeline: Storing items in collection '{self.collection_name}'")
        unique_key_field = spider.settings.get('DUPEFILTER_KEY_FIELD', 'slug')
        try:
            self.mongo_db[self.collection_name].create_index(unique_key_field, unique=True, background=True)
            logging.info(f"Ensured unique index on '{unique_key_field}' in collection '{self.collection_name}'")
        except Exception as e:
            logging.warning(f"Could not ensure unique index on '{unique_key_field}' in {self.collection_name}: {e}")


    def close_spider(self, spider):
        close_mongo_connection()
        pass

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        item_dict = adapter.asdict()

        try:
            result = insert_item(item_dict, self.collection_name)
            if result and result.inserted_id:
                logging.debug(f"Item inserted into MongoDB collection {self.collection_name} with ID {result.inserted_id}", extra={'spider': spider})
                self.stats.inc_value('mongodb/inserted_items', spider=spider)
            elif result is None:
                 logging.error(f"Failed to insert item into MongoDB (insert_item returned None). Item: {item_dict.get('slug', 'N/A')}", extra={'spider': spider})
                 self.stats.inc_value('mongodb/failed_inserts', spider=spider)

        except Exception as e:
            if "duplicate key error" in str(e).lower():
                 logging.warning(f"Duplicate key error inserting item into MongoDB (likely already exists). Key: {item_dict.get(self.settings.get('DUPEFILTER_KEY_FIELD', 'slug'), 'N/A')}", extra={'spider': spider})
                 self.stats.inc_value('mongodb/duplicate_key_error', spider=spider)
            else:
                logging.error(f"Exception inserting item into MongoDB: {e}", extra={'spider': spider}, exc_info=True)
                self.stats.inc_value('mongodb/failed_inserts', spider=spider)

        return item