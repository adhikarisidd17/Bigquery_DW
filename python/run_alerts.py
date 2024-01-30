#%%
import yaml

from google.cloud import bigquery
import os
import json

import requests

#%%
# Function to send Slack alert using webhook URL
def send_slack_alert(webhook_url, channel, message):
    payload = {
        "channel": channel,
        "text": message,
        "icon_emoji": ":x:"
    }
    response = requests.post(webhook_url, json=payload)
    return response
#%%
# Function to read YAML file from the same directory
def read_yaml_file(file_path):
    with open(file_path, 'r') as file:
        yaml_data = yaml.safe_load(file)
    return yaml_data
#%%
yaml_file_path = "slack_alerts_config.yml"
gcp_service_account_key_info = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
json_data = open(gcp_service_account_key_info)
data = json.load(json_data)
print("Testing file path " +gcp_service_account_key_info)
print("Content of file " + json.dumps(data))
config = read_yaml_file(yaml_file_path)
client = bigquery.Client.from_service_account_info(json.loads(gcp_service_account_key_info))
slack_weebhook_url = os.environ['SLACK_WEEBHOOK_URL']
# %%
# Iterate through platforms in the YAML file
for platform in config.get('platforms', []):
    table_name = platform.get('id')
    message = platform.get('description')
    #replace __ with . in table_name
    table_name = table_name.replace('__', '.')
    query = f"SELECT COUNT(*) as row_count FROM {table_name}"
    query_job = client.query(query)
    result = query_job.result()
    row_count = list(result)[0]['row_count']
    if row_count == 0:
        alert_message = f"{message}: {table_name}"
        send_slack_alert(slack_weebhook_url, "#bot-orphan-data", alert_message)
    