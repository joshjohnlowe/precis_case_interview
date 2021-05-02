import requests
import json
import logging
from google.cloud import secretmanager


"""
Runs a DataPrep job
Triggered by a GCS landing event
"""


def run_job(job_id: str, params: dict):
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(name="projects/405737384548/secrets/dataprep_runner_token/versions/1")
    access_token = response.payload.data.decode("utf-8")

    logging.info(job_id)
    logging.info(params)

    body = {
        "wrangledDataset": {
            "id": job_id
        },
        "runParameters": {
            "overrides": {"data": params}
        }
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }

    r = requests.post("https://api.clouddataprep.com/v4/jobGroups", data=json.dumps(body), headers=headers)

    logging.info(r.text)



def run(event, context):
    logging.info('Event ID: {}'.format(context.event_id))
    logging.info('Event type: {}'.format(context.event_type))
    logging.info('Bucket: {}'.format(event['bucket']))
    logging.info('File: {}'.format(event['name']))
    logging.info('Metageneration: {}'.format(event['metageneration']))
    logging.info('Created: {}'.format(event['timeCreated']))
    logging.info('Updated: {}'.format(event['updated']))       

    file_path = event["name"].split("/")

    _date = file_path[1]
    _filename = file_path[2]

    # Remove .csv suffix
    job_type = _filename.split(".")[0]

    job_id_map = {
        "file_payments": 2507748,
        "file_orders": 2507252,
        "file_customers": 2507200
    }

    run_parameters = [
        {"key": "date", "value": _date},
        {"key": "filename", "value": _filename}
    ]

    job_id = job_id_map[job_type]
    run_job(job_id=job_id, params=run_parameters)

