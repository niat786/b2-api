from fastapi import FastAPI, UploadFile, File
from botocore.config import Config
import requests
import boto3
import os
import re
from io import BytesIO



app = FastAPI()

APP_KEY = 'K004MAA5hrBO1psiqj7OozsGCsiBeIQ'
KEY_ID = '004b9b536b4e9e40000000007'
BUCKET_ID = '7b090b7503167b948e290e14'
BUCKET_NAME = 'apkeve'
ENDPOINT = 'https://s3.us-west-004.backblazeb2.com'

@app.get("/")
def index():
    return {'message':'Welcome to apkeve'}

@app.post("/upload")
def upload(file: UploadFile = File(...), bucket_name: str = "apkeve"):
    
    # Create a Boto3 S3 client
    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=KEY_ID,
        aws_secret_access_key=APP_KEY,
        config=Config(signature_version="s3v4"),
    )

    # Upload the file to Backblaze S3-compatible API
    response = s3.upload_fileobj(file.file, bucket_name, file.filename)

    return {"message": f"Successfully uploaded {file.filename} to {bucket_name}"}



@app.get('/download-upload')
def download_upload(url,file_name, chunk_size=1024):

    # Create a Boto3 S3 client
    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=KEY_ID,
        aws_secret_access_key=APP_KEY,
        config=Config(signature_version="s3v4"),
    )
    
    # Create a session object to optimize TCP connections
    session = requests.Session()

    # Send a HEAD request to determine the size and name of the file
    response = session.head(url)
    file_size = int(response.headers.get("Content-Length", 0)) 
    try:
        # Send a GET request to download the file
        response = session.get(url, stream=True)

        # Raise an exception if the GET request was unsuccessful
        response.raise_for_status()

    except requests.exceptions.HTTPError:
        return {'status':404}
    except requests.exceptions.RequestException:
        return {'status':404}
    else:

        # s3 = boto3.client('s3', endpoint_url=ENDPOINT)
         # Set the chunk size (e.g., 5 MB)
        chunk_size = 5 * 1024 * 1024

        # Define the bucket and key
        bucket_name = BUCKET_NAME
        key_name = file_name

        # Stream and upload the file in chunks
        with requests.get(url, stream=True) as response:
            response.raise_for_status()

            # Create a new multipart upload
            upload = s3.create_multipart_upload(Bucket=bucket_name, Key=key_name)
            parts = []
            part_number = 1

            # Read the content in chunks
            for chunk in response.iter_content(chunk_size=chunk_size):
                # Upload each chunk to S3
                result = s3.upload_part(
                    Body=chunk,
                    Bucket=bucket_name,
                    Key=key_name,
                    UploadId=upload['UploadId'],
                    PartNumber=part_number
                )

                # Save the ETag and part number for completing the upload
                parts.append({'ETag': result['ETag'], 'PartNumber': part_number})
                part_number += 1

            # Complete the multipart upload
            s3.complete_multipart_upload(
                Bucket=bucket_name,
                Key=key_name,
                UploadId=upload['UploadId'],
                MultipartUpload={'Parts': parts}
            )

        return {'status': 200, 'file_size': file_size, 'file_name': file_name}


        # # Set the chunk size based on the file size
        # chunk_size = 0
        # if file_size > 0:
        #     chunk_size = max(chunk_size, file_size // (1024 * 10))

        # # Download the file in chunks
        # with open(file_name, "wb") as file:
        #     for chunk in response.iter_content(chunk_size=chunk_size):
        #         file.write(chunk)

        # # Upload the file to Backblaze B2 using the S3-compatible API
        # with open(file_name, "rb") as f:
        #     s3.upload_fileobj(f, "apkeve", file_name)

        # # Delete the local file
        # os.remove(file_name)
        # return {'status': 200, 'file_size': file_size, 'file_name': file_name}