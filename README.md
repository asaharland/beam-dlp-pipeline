## Apache Beam Cloud DLP Pipeline

### Description
This example demonstrates how you can call Cloud DLP from Apache Beam to 
redact PII from free text. The flow is as follows: 
1. Reads data from a CSV
2. Sends the free text field to the Cloud DLP API 
to redact ALL BASIC information
3. Writes the output to BigQuery

### Running the example
Please follow the steps below to run the example:
1. Configure `gcloud` with your credentials 
2. Enable Cloud Dataflow and Cloud DLP in your 
Google Cloud Platform project 
3. Run the following command, passing in your variables:
```
python -m dlp_pipeline 
--input [PATH TO FILE] 
--output [BIGQUERY OUTPUT TABLE] 
--temp_location [TEMP BUCKET] 
--staging_location [STAGING BUCKET] 
--project [GCP PROJECT ID]
--runner DataflowRunner 
--setup_file ./setup.py
```
