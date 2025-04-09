import scrapy
import json
import logging
from pathlib import Path
from jobs_project import jobs_project

class JobProjectSpider(scrapy.Spider):
    name = "JobProjectSpider"
    file_urls = [
        "/data/s01.json",
        "/data/s02.json",
    ]
    def __init__(self, name = None, **kwargs):
        super().__init__(name, **kwargs)
        logging.info(f"Starting request for the urls: {self.file_urls}")

    def parse(self, response, **kwargs):

        return super()._parse(response, **kwargs)