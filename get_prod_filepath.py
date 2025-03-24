from fastapi import FastAPI, HTTPException
import boto3
import json

app = FastAPI()

# Configuration
AWS_ACCESS_KEY = "AKIAWOAVSU6U72T77FUR"
AWS_SECRET_KEY = "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"
BUCKET_NAME = "belden-demo-bucket"

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name="us-east-1"
)

def get_csv_path(bucket_name, file_key):
    """Fetch the specified CSV file path from the S3 bucket."""
    try:
        s3_client.head_object(Bucket=bucket_name, Key=file_key)
        s3_path = f"s3://{bucket_name}/{file_key}"
        return {"status": "success", "path": s3_path}
    except s3_client.exceptions.ClientError as e:
        raise HTTPException(status_code=404, detail=f"File not found: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

# API endpoint
@app.get("/get-product-file-path")
async def get_product_file_path():
    """
    Retrieve the S3 path of the product CSV file.
    """
    file_key = "Product Sheet/Product_sheet.csv"
    try:
        result = get_csv_path(BUCKET_NAME, file_key)
        return result
    except HTTPException as e:
        raise e

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
