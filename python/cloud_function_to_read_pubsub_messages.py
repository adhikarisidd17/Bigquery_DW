
import base64
from google.cloud import bigquery
import json

import pandas as pd

def pubsub(event, context):

     pubsub_message = base64.b64decode(event['data']).decode('utf-8')

     json_message = json.loads(pubsub_message)

     file_name = "gs://postgres-gcs-bucket/" + json_message["name"]

     print(file_name)

     results = pd.read_json(file_name, lines=True)

     results_final = results[["source_metadata","payload"]]


     results_filtered = results_final
     results_filtered["change_type"] = results_filtered["source_metadata"].apply(lambda x: x.get('change_type', None))
     results_filtered = results_filtered[results_filtered["change_type"] == "DELETE"]

     bq_results = [{"payload": dict(payload.items()), "source_metadata": dict(metadata.items())} for payload, metadata in zip(results_filtered["payload"], results_filtered["source_metadata"])]
     if len(bq_results) == 0:
          bq_results = [{"payload": None, "source_metadata": None}]

     bq_table_path = "normative-analytics.raw_sensitive.postgres_deleted_activities"

     #%%

     #Write dataframe to bigquery json column:

     Client = bigquery.Client()

     bq_table_schema = [

     bigquery.SchemaField("payload", "JSON", mode="NULLABLE"),

     bigquery.SchemaField("source_metadata", "JSON", mode="NULLABLE"),

     ]

     job_config = bigquery.LoadJobConfig(

     schema=bq_table_schema,

     source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,

     write_disposition=bigquery.WriteDisposition.WRITE_APPEND,

     )

     #%%

     Client.get_table(bq_table_path) # Will throw NotFound exception if table does not exist

     #%%

     bq_upload_job = Client.load_table_from_json(bq_results, bq_table_path, job_config=job_config)



# Requirement file content:

#pandas
#fsspec
#gcsfs
#google-cloud-storage
#google-cloud-bigquery