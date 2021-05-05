import requests
import os

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import uuid

"""
Enables change-detection on all files within the "datasets" folder
in Google drive. This allows us to automaticaly copy these files to
GCS as they are updated
"""


def enable_watch():
    scopes = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/drive.file",
    ]

    file_ids = [
        "14gR0D2NLVRo8lEm5romd1ZGaP1A2iXc-", # Customers
        "1c5Sp174GdEGe0W6MOWT93JGIuSDv8XS6", # Payments
        "1hks4jM6oluYpi1tfQqQDNQkDmSMka89s", # Orders
    ]

    # TODO: Retreive credentials from secret store

    # credentials = ServiceAccountCredentials.from_json_keyfile_name(
    #     "./drive-reader.json", scopes=scopes
    # )

    service = build("drive", "v3", credentials=credentials)

    channel_id = str(uuid.uuid4())

    body = {
        "channel_id": channel_id,
        "type": "webhook",
        "address": "https://us-central1-precis-digital-case-interview.cloudfunctions.net/drive_to_gcs"
    }

    for _id in file_ids:
        print(_id)
        _list = service.files().list().execute()
        resp = service.files().watch(fileId=_id, body=body).execute()


if __name__ == "__main__":
    resp = enable_watch()


# curl -X POST "https://us-central1-precis-digital-case-interview.cloudfunctions.net/drive_to_gcs" -H "Content-Type:application/json" --data '{"fileid":"1hks4jM6oluYpi1tfQqQDNQkDmSMka89s"}'