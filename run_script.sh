#!/bin/bash

auto_confirm="no" # to run the commands full
# Function to display help options
print_help() {
  echo "Usage: dataingestionpipeline [options]"
  echo ""
  echo "Options:"
  echo "  -h, --help      Show this help message and exit"
  echo "  -y, --yes-pipeline   Run all commands without any user input"
  echo "  -u, --user-input ask user for permission related to few steps"
  echo ""
}

run_the_full_pipeline() {
  docker compose up -d
  if [[ "$auto_confirm" == "yes" ]]; then
    echo "Running scrape job"
    docker exec -it -w /app/jobs_project data_ingestion_scrapy_app scrapy crawl JobProjectSpider
  else
    echo "Do you want to run the scrape job? (yes/no):"
    read run_scrape
    if [[ "$run_scrape" == "yes" || "$run_scrape" == "y" ]]; then
      docker exec -it -w /app/jobs_project data_ingestion_scrapy_app scrapy crawl JobProjectSpider
    fi
  fi

  if [[ "$auto_confirm" == "yes" ]]; then
    echo "Running query script"
    docker exec -it -w /app data_ingestion_scrapy_app python query.py
  else
    echo "Do you want to run the query script to save data from mongo to file? (yes/no):"
    read run_query
    if [[ "$run_query" == "yes" || "$run_query" == "y" ]]; then
      docker exec -it -w /app data_ingestion_scrapy_app python query.py
    fi
  fi

  if [[ "$auto_confirm" == "yes" ]]; then
    echo "Getting final_jobs.csv"
    docker cp data_ingestion_scrapy_app:/app/final_jobs.csv final_jobs.csv
  else
    echo "Do you want to run get the final_job.csv into your local system? (yes/no):"
    read get_csv
    if [[ "$get_csv" == "yes" || "$get_csv" == "y" ]]; then
      docker cp data_ingestion_scrapy_app:/app/final_jobs.csv final_jobs.csv
    fi
  fi
}


while [[ "$#" -gt 0 ]]; do
  case "$1" in
    -h|--help)
      print_help
      exit 0
      ;;
    -y|--yes-pipeline)
      auto_confirm="yes"
      run_the_full_pipeline
      exit 0
      ;;
    -u|--user-input)
      run_the_full_pipeline
      exit 0
      ;;
    *)
      echo "Error: Unknown option: $1"
      print_help
      exit 1
      ;;
  esac
done

# Your main script logic goes here
if [ -z "$INPUT_FILE" ]; then
  echo "Warning: No input file specified. Use -i or --input."
fi

if [ -z "$OUTPUT_FILE" ]; then
  echo "Warning: No output file specified. Use -o or --output."
fi

if [ -z "$QUIET" ]; then
  echo "Running the script..."
  # Add your script's core functionality here
else
  # Running in quiet mode
  : # No output
fi

# Example of using the defined variables
# if [ -n "$INPUT_FILE" ]; then
#   cat "$INPUT_FILE"
# fi