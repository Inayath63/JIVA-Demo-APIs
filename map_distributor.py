from fastapi import FastAPI, HTTPException
import boto3
import pandas as pd
from io import StringIO

app = FastAPI()

# AWS credentials and bucket name
AWS_ACCESS_KEY = "AKIAWOAVSU6U72T77FUR"
AWS_SECRET_KEY = "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"
BUCKET_NAME = "belden-demo-bucket"

# Initialize S3 client
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

@app.post("/map-distributor")
async def map_distributor():
    """
    Map distributor URLs from Demo Distributor URLs.csv to Product_sheet_updated.csv
    and update the S3 file.
    """
    try:
        # Read distributor URLs
        distributor_df = read_csv_from_s3(BUCKET_NAME, "Distributor URL/Demo Distributor URLs.csv")
        
        # Read product sheet
        product_df = read_csv_from_s3(BUCKET_NAME, "Product Sheet/Product_sheet_updated.csv")
        
        # Create a mapping of distributor URLs
        dist_urls = {
            'Distributor 1': distributor_df['Distributor 1 URL'].iloc[0] if 'Distributor 1 URL' in distributor_df.columns else None,
            'Distributor 2': distributor_df['Distributor 2 URL'].iloc[0] if 'Distributor 2 URL' in distributor_df.columns else None,
            'Distributor 3': distributor_df['Distributor 3 URL'].iloc[0] if 'Distributor 3 URL' in distributor_df.columns else None
        }
        
        # Update product sheet with distributor URLs
        for dist_name, url in dist_urls.items():
            if url and dist_name + ' URL' in product_df.columns:
                product_df[dist_name + ' URL'] = url
        
        # Write updated product sheet back to S3
        write_csv_to_s3(product_df, BUCKET_NAME, "Product Sheet/Product_sheet_updated.csv")
        
        return {"message": "Successfully updated Product_sheet_updated.csv with distributor URLs"}

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
