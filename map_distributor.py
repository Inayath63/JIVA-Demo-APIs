from fastapi import APIRouter, HTTPException
import boto3
import pandas as pd
from io import StringIO
from pydantic import BaseModel
from urllib.parse import urlparse

router = APIRouter(prefix="/map-distributor", tags=["map-distributor"])

AWS_ACCESS_KEY = "AKIAWOAVSU6U72T77FUR"
AWS_SECRET_KEY = "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"
BUCKET_NAME = "belden-demo-bucket"

s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

class ProductRequest(BaseModel):
    product_name: str

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

def load_distributor_domains():
    """Extract root domains and names from distributor-list.csv in S3."""
    try:
        df = read_csv_from_s3(BUCKET_NAME, "Distributor List/distributor-list.csv")
        if "website" not in df.columns or "name" not in df.columns:
            raise HTTPException(status_code=500, detail="'website' or 'name' column not found in distributor-list.csv")
        
        distributor_info = {}
        for _, row in df.dropna(subset=["website"]).iterrows():
            website = row["website"].strip().lower()
            # If it's not a full URL, assume it's a domain
            if not website.startswith(('http://', 'https://')):
                domain = website.replace("www.", "")
            else:
                parsed_url = urlparse(website)
                domain = parsed_url.netloc.replace("www.", "") if parsed_url.netloc else parsed_url.path.replace("www.", "")
            if domain:
                distributor_info[domain] = row["name"]
                
        return distributor_info
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading distributor CSV: {str(e)}")

@router.post("")
async def map_distributor(request: ProductRequest):
    """
    Map distributor URLs and names for a specific product name from Demo Distributor URLs.csv 
    and distributor-list.csv to Product_sheet.csv and update the S3 file.
    """
    try:
        # Read all CSVs
        distributor_df = read_csv_from_s3(BUCKET_NAME, "Distributor URL/Demo Distributor URLs.csv")
        product_df = read_csv_from_s3(BUCKET_NAME, "Product Sheet/Product_sheet.csv")
        distributor_domains = load_distributor_domains()
        
        # Validate required columns
        if 'Product Name' not in distributor_df.columns:
            raise HTTPException(status_code=500, detail="Missing 'Product Name' in Demo Distributor URLs.csv")
        if 'web_part_number' not in product_df.columns:
            raise HTTPException(status_code=500, detail="Missing 'web_part_number' in Product_sheet.csv")

        # Filter distributor_df for the requested product_name
        distributor_match = distributor_df[distributor_df['Product Name'] == request.product_name]
        
        if distributor_match.empty:
            raise HTTPException(status_code=404, detail=f"No distributor URLs found for product: {request.product_name}")

        # Get the distributor URLs
        distributor_urls = {
            'Distributor URL 1': distributor_match['Distributor URL 1'].iloc[0] if 'Distributor URL 1' in distributor_match.columns else None,
            'Distributor URL 2': distributor_match['Distributor URL 2'].iloc[0] if 'Distributor URL 2' in distributor_match.columns else None,
            'Distributor URL 3': distributor_match['Distributor URL 3'].iloc[0] if 'Distributor URL 3' in distributor_match.columns else None
        }

        # Find matching distributor names
        distributor_names = set()
        for url in distributor_urls.values():
            if url:
                parsed_url = urlparse(url.strip().lower())
                domain = parsed_url.netloc.replace("www.", "") if parsed_url.netloc else parsed_url.path.replace("www.", "")
                if domain in distributor_domains:
                    distributor_names.add(distributor_domains[domain])

        # Update product_df
        product_idx = product_df[product_df['web_part_number'] == request.product_name].index
        
        if product_idx.empty:
            raise HTTPException(status_code=404, detail=f"Product {request.product_name} not found in Product_sheet.csv")

        # Update distributor URLs
        for dist_col in ['Distributor URL 1', 'Distributor URL 2', 'Distributor URL 3']:
            if dist_col in product_df.columns and distributor_urls.get(dist_col):
                product_df.loc[product_idx, dist_col] = distributor_urls[dist_col]

        # Update Distributor Names column
        if 'Distributor Names' not in product_df.columns:
            if 'Distributor URL 1' in product_df.columns:
                url1_position = product_df.columns.get_loc('Distributor URL 1')
                product_df.insert(url1_position, 'Distributor Names', None)
            else:
                product_df['Distributor Names'] = None
        
        product_df.loc[product_idx, 'Distributor Names'] = ', '.join(distributor_names) if distributor_names else None

        # Write back to S3
        write_csv_to_s3(product_df, BUCKET_NAME, "Product Sheet/Product_sheet.csv")
        
        num_distributors = len(distributor_names)
        
        return {
            "message": f"Successfully updated {num_distributors} distributors in Product_sheet.csv for {request.product_name}",
            "distributor_urls": {k: v for k, v in distributor_urls.items() if v is not None},
            "distributor_names": list(distributor_names)
        }
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
