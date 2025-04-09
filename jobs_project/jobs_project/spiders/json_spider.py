import scrapy
import json
import logging
from pathlib import Path

import scrapy.loader
from jobs_project.items import JobsProjectItem

class JobProjectSpider(scrapy.Spider):
    name = "JobProjectSpider"
    data_dir = Path(__file__).parent.parent.parent / 'jobs_project' / 'data'
    start_urls = [
        f'file://{str(data_dir.resolve() / "s01.json")}',
        f'file://{str(data_dir.resolve() / "s02.json")}',
    ]

    def start_requests(self):
        for url in self.start_urls:
            logging.info(f"{url}")
            if url.startswith('file://'):
                filepath = url[7:]
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        yield from self.parse(data=data, file_path=filepath)
                except FileNotFoundError:
                    self.logger.error(f"File not found: {filepath}")
                except json.JSONDecodeError:
                    self.logger.error(f"Error decoding JSON in: {filepath}")
            else:
                yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, data, file_path=None):
        if file_path:
            logging.info(f"Processing file: {file_path}")
        else:
            logging.info("Processing data from a web response.")

        jobs_data_list = data.get('jobs', [])
        if not isinstance(jobs_data_list, list):
            logging.error(f"Expected 'jobs' key to be a list in {file_path}, found {type(jobs_data_list)}. Skipping.")
            return

        logging.info(f"Found {len(jobs_data_list)} jobs in {file_path}")

        for job_entry in jobs_data_list:
            if not isinstance(job_entry, dict):
                logging.warning(f"Skipping non-dictionary item in jobs list: {job_data}")
                continue

            job_data = job_entry.get('data')
            if not isinstance(job_data, dict):
                logging.warning(f"Skipping item without 'data' dictionary: {job_entry}")
                continue

            item = JobsProjectItem()
            loader = scrapy.loader.ItemLoader(item=item)
            loader.add_value('slug', job_data.get('slug'))
            loader.add_value('language', job_data.get('language'))
            loader.add_value('req_id', job_data.get('req_id'))
            loader.add_value('title', job_data.get('title'))
            loader.add_value('description', job_data.get('description'))
            loader.add_value('street_address', job_data.get('street_address'))
            loader.add_value('city', job_data.get('city'))
            loader.add_value('state', job_data.get('state'))
            loader.add_value('country_code', job_data.get('country_code'))
            loader.add_value('postal_code', job_data.get('postal_code'))
            loader.add_value('location_type', job_data.get('location_type'))
            loader.add_value('latitude', job_data.get('latitude'))
            loader.add_value('longitude', job_data.get('longitude'))
            loader.add_value('categories', job_data.get('categories'))
            loader.add_value('tags', job_data.get('tags'))
            loader.add_value('tags5', job_data.get('tags5'))
            loader.add_value('tags6', job_data.get('tags6'))
            loader.add_value('brand', job_data.get('brand'))
            loader.add_value('promotion_value', job_data.get('promotion_value'))
            loader.add_value('salary_currency', job_data.get('salary_currency'))
            loader.add_value('salary_value', job_data.get('salary_value'))
            loader.add_value('salary_min_value', job_data.get('salary_min_value'))
            loader.add_value('salary_max_value', job_data.get('salary_max_value'))
            loader.add_value('benefits', job_data.get('benefits'))
            loader.add_value('employment_type', job_data.get('employment_type'))
            loader.add_value('hiring_organization', job_data.get('hiring_organization'))
            loader.add_value('source', job_data.get('source'))
            loader.add_value('apply_url', job_data.get('apply_url'))
            loader.add_value('internal', job_data.get('internal'))
            loader.add_value('searchable', job_data.get('searchable'))
            loader.add_value('applyable', job_data.get('applyable'))
            loader.add_value('li_easy_applyable', job_data.get('li_easy_applyable'))
            loader.add_value('ats_code', job_data.get('ats_code'))
            loader.add_value('meta_data', job_data.get('meta_data'))
            loader.add_value('update_date', job_data.get('update_date'))
            loader.add_value('create_date', job_data.get('create_date'))
            loader.add_value('category', job_data.get('category'))
            loader.add_value('full_location', job_data.get('full_location'))
            loader.add_value('short_location', job_data.get('short_location'))
            yield loader.load_item()