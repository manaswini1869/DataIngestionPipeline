# Define here the models for your scraped items
#
# See documentation in=
# https=//docs.scrapy.org/en/latest/topics/items.html

import scrapy
class JobsProjectItem(scrapy.Item):
    # define the fields for your item here like=
    # name = scrapy.Field()

    # job data details
    slug = scrapy.Field()
    language= scrapy.Field()
    req_id= scrapy.Field()
    title= scrapy.Field()
    description= scrapy.Field()
    street_address= scrapy.Field()
    city= scrapy.Field()
    state= scrapy.Field()
    country_code= scrapy.Field()
    postal_code= scrapy.Field()
    location_type= scrapy.Field()
    latitude= scrapy.Field()
    longitude= scrapy.Field()
    categories= scrapy.Field()
    tags= scrapy.Field()
    tags5= scrapy.Field()
    tags6= scrapy.Field()
    brand= scrapy.Field()
    promotion_value= scrapy.Field()
    salary_currency= scrapy.Field()
    salary_value= scrapy.Field()
    salary_min_value= scrapy.Field()
    salary_max_value= scrapy.Field()
    benefits= scrapy.Field()
    employment_type= scrapy.Field()
    hiring_organization= scrapy.Field()
    source= scrapy.Field()
    apply_url= scrapy.Field()
    internal= scrapy.Field()
    searchable= scrapy.Field()
    applyable= scrapy.Field()
    li_easy_applyable= scrapy.Field()
    ats_code= scrapy.Field()
    meta_data= scrapy.Field()
    update_date= scrapy.Field()
    create_date= scrapy.Field()
    category= scrapy.Field()
    full_location= scrapy.Field()
    short_location= scrapy.Field()
    pass
