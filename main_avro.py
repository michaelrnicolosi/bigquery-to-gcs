from google.cloud import bigquery
import base64
import json

def extract_bigquery_table(event, context):
     pubsub_message = base64.b64decode(event['data']).decode('utf-8')
     obj = json.loads(pubsub_message)
     
     dataset_id = obj['protoPayload']['serviceData']['jobCompletedEvent']['job']['jobConfiguration']['load']['destinationTable']['datasetId']
     table_id = obj['protoPayload']['serviceData']['jobCompletedEvent']['job']['jobConfiguration']['load']['destinationTable']['tableId']
     
     BUCKET_NAME = '<YOUR-GCS-BUCKET>'
     PROJECT = '<YOUR-PROJECT-ID>'

     bq_client = bigquery.Client()

     file_name = f"{dataset_id}_{table_id}"
     destination_uri = f"gs://{BUCKET_NAME}/{file_name}"
     dataset_ref = bq_client.dataset(dataset_id, project=PROJECT)
     table_ref = dataset_ref.table(table_id)
     
     job_config = bigquery.ExtractJobConfig()
     job_config.compression = 'DEFLATE'
     job_config.destination_format = 'AVRO'
     
     extract_job = bq_client.extract_table(
        table_ref,
        destination_uri,
        location='US',
        job_config=job_config
        )
     extract_job.result()
