from fastapi import FastAPI, HTTPException
import pandas as pd
import boto3
from io import StringIO
import json
import numpy as np

app = FastAPI()

# Configuration
ACCESS_KEY = "AKIAWOAVSU6U72T77FUR"
SECRET_KEY = "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"
BUCKET_NAME = "belden-demo-bucket"
CSV_FILE_KEY = "Product Sheet/Product_sheet.csv"

s3_client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

def replace_nan_inf(obj):
    """Recursively replace NaN, Inf, -Inf with None in a dictionary or list."""
    if isinstance(obj, dict):
        return {k: replace_nan_inf(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_nan_inf(item) for item in obj]
    elif isinstance(obj, float):
        if pd.isna(obj) or obj in (float('inf'), float('-inf')):
            return None
        return obj
    return obj

def read_csv_from_s3(bucket_name, file_key):
    """Reads CSV file from S3 and converts it to JSON format."""
    try:
        obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        csv_data = obj["Body"].read().decode("utf-8")

        df = pd.read_csv(StringIO(csv_data))
        
        # Convert DataFrame to dict and handle NaN/Inf values
        json_data = replace_nan_inf(df.to_dict(orient="list"))
        return json.dumps(json_data, indent=4)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading CSV from S3: {str(e)}")

# API endpoint
@app.get("/get-prod-details")
async def get_product_details():
    """
    Retrieve product details from S3 CSV file and return as JSON.
    """
    try:
        json_result = read_csv_from_s3(BUCKET_NAME, CSV_FILE_KEY)
        return json.loads(json_result)  # Return parsed JSON for proper response formatting
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
