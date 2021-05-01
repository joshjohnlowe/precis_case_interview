import requests
import os

from apiclient.discovery import build
# from oauth2client.service_account import ServiceAccountCredentials
import google.auth
import uuid
import io
import shutil
from googleapiclient.http import MediaIoBaseDownload

import datetime

from google.cloud import storage
import logging


"""
Copies a file from Google Drive, to GCS
"""


def copy_file(file_id, file_name):
    scopes = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/drive.file",
    ]

    credentials, project_id = google.auth.default()

    current_dt = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d-%H-%M-%S")
    destination_blob_name = f"{file_name}/{current_dt}/{file_name}.csv"

    storage_client = storage.Client(project="precis-digital-case-interview")
    bucket = storage_client.bucket("precis-digital-case-interview-landing-bucket")

    blob = bucket.blob(destination_blob_name)

    drive_service = build("drive", "v3", credentials=credentials)

    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        logging.info(status)

    # Save G Drive file locally, then upload it to GSC

    logging.info("Download file from Google Drive")
    fh.seek(0)
    with open('/tmp/your_filename.csv', 'wb') as f:
        shutil.copyfileobj(fh, f, length=131072)

    logging.info("Uploading file to GCS")
    blob.upload_from_filename("/tmp/your_filename.csv")


def run(request):
    request_json = request.get_json(silent=True)
    request_args = request.args    
    if request_json and 'fileid' in request_json:
        fileid = request_json['fileid']
    if request_json and 'filename' in request_json:
        filename = request_json['filename']        
    elif request_args and 'fileid' in request_args:
        fileid = request_args['fileid']
    elif request_args and 'filename' in request_args:
        filename = request_args['filename']        

    else:
        raise Exception("No params passed.")

    copy_file(fileid, filename)