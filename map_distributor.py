# map_distributor.py
from fastapi import APIRouter, HTTPException
import boto3
import pandas as pd
from io import StringIO

router = APIRouter(prefix="/map-distributor", tags=["map-distributor"])

AWS_ACCESS_KEY = "AKIAWOAVSU6U72T77FUR"
AWS_SECRET_KEY = "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"
BUCKET_NAME = "belden-demo-bucket"

s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

def read_csv_from_s3(bucket, key):
    """Read CSV from S3 into a pandas DataFrame."""
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading CSV from S3: {str(e)}")

def write_csv_to_s3(df, bucket, key):
    """Write DataFrame to S3 as CSV."""
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
        return True
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error writing CSV to S3: {str(e)}")

@router.post("")
async def map_distributor():
    """
    Map distributor URLs from Demo Distributor URLs.csv to Product_sheet_updated.csv
    and update the S3 file.
    """
    try:
        distributor_df = read_csv_from_s3(BUCKET_NAME, "Distributor URL/Demo Distributor URLs.csv")
        product_df = read_csv_from_s3(BUCKET_NAME, "Product Sheet/Product_sheet_updated.csv")
        
        dist_urls = {
            'Distributor 1': distributor_df['Distributor 1 URL'].iloc[0] if 'Distributor 1 URL' in distributor_df.columns else None,
            'Distributor 2': distributor_df['Distributor 2 URL'].iloc[0] if 'Distributor 2 URL' in distributor_df.columns else None,
            'Distributor 3': distributor_df['Distributor 3 URL'].iloc[0] if 'Distributor 3 URL' in distributor_df.columns else None
        }
        
        for dist_name, url in dist_urls.items():
            if url and dist_name + ' URL' in product_df.columns:
                product_df[dist_name + ' URL'] = url
        
        write_csv_to_s3(product_df, BUCKET_NAME, "Product Sheet/Product_sheet_updated.csv")
        
        return {"message": "Successfully updated Product_sheet_updated.csv with distributor URLs"}
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
