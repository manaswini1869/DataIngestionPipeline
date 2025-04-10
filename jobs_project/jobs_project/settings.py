BOT_NAME = "jobs_project"

SPIDER_MODULES = ["jobs_project.spiders"]
NEWSPIDER_MODULE = "jobs_project.spiders"

ROBOTSTXT_OBEY = False

ITEM_PIPELINES = {
    "jobs_project.pipelines.RedisDeduplicationPipeline": 100,
    "jobs_project.pipelines.MongoDBPipeline": 200,
}

DUPEFILTER_KEY_FIELD = 'slug'

# MongoDB collection name where job items will be stored.
MONGO_COLLECTION = 'testing_jobs' #

REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"

# Logging Settings
LOG_LEVEL = 'INFO'