# get_bucket_details.py
from fastapi import APIRouter, HTTPException
import boto3
import json

router = APIRouter(prefix="/get-bucket-details", tags=["bucket-details"])

AWS_ACCESS_KEY = "AKIAWOAVSU6U72T77FUR"
AWS_SECRET_KEY = "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"
BUCKET_NAME = "belden-demo-bucket"

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name="us-east-1"
)

def list_s3_objects(bucket_name):
    """Fetch all folders and files from the given S3 bucket."""
    try:
        objects = s3_client.list_objects_v2(Bucket=bucket_name)
        if "Contents" not in objects:
            return {"Folders": [], "Files": []}

        folders = set()
        files = []

        for obj in objects["Contents"]:
            key = obj["Key"]
            if key.endswith("/"):
                folders.add(key)
            else:
                files.append(key)

        return {"Folders": sorted(list(folders)), "Files": sorted(files)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing S3 objects: {str(e)}")

@router.get("")
async def get_bucket_details():
    """Retrieve all folders and files from the specified S3 bucket."""
    try:
        s3_data = list_s3_objects(BUCKET_NAME)
        return s3_data
    except HTTPException as e:
        raise e
