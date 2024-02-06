# GameTuner Enrich Bad Sink

## Overview

This service is responsible for sinking pub/sub messages from enrich bad topic to BigQuery. It is written in Python and uses the Google Cloud Pub/Sub and BigQuery libraries.

## Requirements

- Python 3.10+
- Google Cloud SDK

## Installation

### Running locally

1. Install the required dependencies:

```bash
pip install -r requirements.txt
```

2. Set the required environment variables:

* `GCP_PROJECT_ID` - project id
* `ENRICH_BAD_SUB` - subscription id of topic where bad messages are published
* `BIGQUERY_DATASET` - dataset id in bigquery
* `BIGQUERY_TABLE` - table id in bigquery

```bash
export GCP_PROJECT_ID=<project-id>
export ENRICH_BAD_SUB=<subscription-id>
export BIGQUERY_DATASET=<dataset-id>
export BIGQUERY_TABLE=<table-id>
```

3. Run the service:

```bash
python main.py
```

## Deployment

### Deploying to Google Cloud

Deploy project to GCP using cloud build command:

```bash
gcloud builds submit --config=cloudbuild.yaml .
```

## Usage

### Get data

Query example for enrich bad data

```SQL
SELECT
*,
SPLIT(JSON_EXTRACT(data, '$.failure.messages[0].schemaKey'), '/')[OFFSET(1)] as event_name
FROM `<project-id>.gametuner_monitoring.enrich_bad_events`
WHERE DATE(load_tstamp) = CURRENT_DATE()
```

## Licence

The GameTuner ETL service is copyright 2022-2024 AlgebraAI.

GameTuner ETL service is released under the [Apache 2.0 License][license].

[license]: https://www.apache.org/licenses/LICENSE-2.0