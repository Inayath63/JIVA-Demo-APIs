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
    Map distributor URLs from Demo Distributor URLs.csv to Product_sheet_test.csv
    based on Product Name and web_part_number, and update the S3 file.
    """
    try:
        # Read both CSVs
        distributor_df = read_csv_from_s3(BUCKET_NAME, "Distributor URL/Demo Distributor URLs.csv")
        product_df = read_csv_from_s3(BUCKET_NAME, "Product Sheet/Product_sheet.csv")
        
        # Ensure common columns exist
        if 'Product Name' not in distributor_df.columns:
            raise HTTPException(status_code=500, detail="Missing 'Product Name' in Demo Distributor URLs.csv")
        if 'web_part_number' not in product_df.columns:
            raise HTTPException(status_code=500, detail="Missing 'web_part_number' in Product_sheet.csv")

        # Rename 'Product Name' to 'web_part_number' in distributor_df for merging
        distributor_df = distributor_df.rename(columns={'Product Name': 'web_part_number'})
        
        # Merge DataFrames on 'web_part_number'
        merged_df = product_df.merge(
            distributor_df[['web_part_number', 'Distributor URL 1', 'Distributor URL 2', 'Distributor URL 3']],
            on='web_part_number',
            how='left',
            suffixes=('', '_new')
        )

        # Update distributor URL columns if they exist in product_df
        for dist_col in ['Distributor URL 1', 'Distributor URL 2', 'Distributor URL 3']:
            if dist_col in product_df.columns:
                merged_df[dist_col] = merged_df[dist_col + '_new'].where(merged_df[dist_col + '_new'].notna(), merged_df[dist_col])
            else:
                merged_df[dist_col] = merged_df[dist_col + '_new']

        # Drop temporary '_new' columns
        merged_df = merged_df.drop(columns=[col for col in merged_df.columns if col.endswith('_new')])

        # Write back to S3 (using Product_sheet_test.csv as in your working version)
        write_csv_to_s3(merged_df, BUCKET_NAME, "Product Sheet/Product_sheet_test.csv")
        
        return {"message": "Successfully updated Product_sheet_test.csv with distributor URLs"}
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
