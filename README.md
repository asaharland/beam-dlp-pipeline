# Apache Beam Cloud DLP Pipeline

## Description
This example demonstrates how you can call Cloud DLP from Apache Beam to 
redact PII from free text. The flow is as follows: 
1. Reads data from a CSV
2. Sends the free text field to the Cloud DLP API 
to redact ALL BASIC information
3. Writes the output to BigQuery

## Running the example

### Project setup
Please follow the steps below to run the example:
1. Configure `gcloud` with your credentials 
2. Enable Cloud Dataflow and Cloud DLP in your 
Google Cloud Platform project 

#### Batch mode:
1. Run the following command to execute the pipeline in 
 batch mode:
```
python -m pipeline 
--input [PATH TO FILE] 
--output [BIGQUERY OUTPUT TABLE] 
--temp_location [TEMP BUCKET] 
--staging_location [STAGING BUCKET] 
--project [GCP PROJECT ID]
--runner DataflowRunner 
--setup_file ./setup.py
```

#### Streaming mode:
1. Run the following command to execute the pipeline in 
 streaming mode:
```
python -m pipeline 
--input [PubSub Topic] 
--output [BIGQUERY OUTPUT TABLE] 
--temp_location [TEMP BUCKET] 
--staging_location [STAGING BUCKET] 
--project [GCP PROJECT ID]
--runner DataflowRunner 
--setup_file ./setup.py
--streaming
```

