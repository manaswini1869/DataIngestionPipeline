FROM python:3.9-slim

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# copying the project to the container
COPY ./infra /app/infra
COPY ./jobs_project /app/jobs_project
COPY ./query.py /app/query.py

WORKDIR /app/jobs_project

# This will keep the container running
CMD [ "sleep", "infinity" ]