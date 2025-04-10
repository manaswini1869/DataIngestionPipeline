# DataIngestionPipeline
The repository contains implementation for take-home assignment from Canaria.

Technologies used: Scrapy, MongoDB, Redis, Docker, Python

## Setup
### Cloning the code
1. Follow the steps here" https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository to clone the repo into you local system

### Running the pipeline
1. To run the pipeline, first you have to make sure that docker engine is running, if you don't have docker installed you can do so here: https://docs.docker.com/engine/install/
2. After installing docker and starting the docker engine, cd into `Dataingestionpipeline` folder.
3. You don't have to export any `Environment Variables` as that is taken in the `docker-compose.yml` file, feel free to take a look at it.
4. Run the command `docker compose up -d` to start the mongodb, redis, and data_ingestion_pipeline container
5. Once the containers are started and running, run the command
`docker exec -it -w /app/jobs_project data_ingestion_scrapy_app scrapy crawl JobProjectSpider`this will start the redis and mongo pipeline defines in `./jobs_project/jobs_project/pipelines.py` and start scrapping the data from the files in `./jobs_project/jobs_project/data` folder.
6. When the pipeline is completed running, you can see the stats from scrapy.
7. If you want to export the data from mongo database into a csv file run the command `docker exec -it -w /app data_ingestion_scrapy_app python query.py` which will create a `final_jobs.csv` in the `data_ingestion_scrapy_app` docker container. You can copy the file into your local system using the command `docker cp data_ingestion_scrapy_app:/app/final_jobs.csv final_jobs.csv`.

### Running the pipeline using script
1. There are a lot of commands to get the pipeline working, to make the evaluators or the users life easier I wrote a script that runs all the commands for you, when you run a single command.
2. Run the command `.\run_script.sh` in a bash terminal and the scraping takes care of itself.
3. Use the `.\run_script.sh` for more options that can be used to run the script.

## Project Structure

I followed the sample project structure given in the instructions. To make the scrapy app set up easy, I ran the command `scrapy startproject job_project` which created a bunch of scrapy files required to run the pipeline similar to the structure in the docs.

### Brief over of project structure

#### infra
This folder contains the