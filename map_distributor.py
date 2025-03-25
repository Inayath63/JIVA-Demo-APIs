from fastapi import APIRouter, HTTPException
import boto3
import pandas as pd
from io import StringIO
from pydantic import BaseModel
from urllib.parse import urlparse
import google.generativeai as genai
import json
import numpy as np
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/map-distributor", tags=["map-distributor"])

AWS_ACCESS_KEY = "AKIAWOAVSU6U72T77FUR"
AWS_SECRET_KEY = "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"
BUCKET_NAME = "belden-demo-bucket"

s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

# Configure Google Generative AI
genai.configure(api_key="AIzaSyAhaac6yPECRFwRmpifQattFvK__8YCyaY")
model = genai.GenerativeModel(model_name='gemini-2.0-flash')

class ProductRequest(BaseModel):
    product_name: str  # This will be treated as part_number

def read_csv_from_s3(bucket, key):
    """Read CSV from S3 into a pandas DataFrame."""
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        return df.replace([np.nan, pd.NA], None)
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

def get_domains_from_urls(urls):
    """Get main domains from URLs using Google Generative AI."""
    try:
        logger.info(f"URLs passed to get_domains_from_urls: {urls}")
        # If no valid URLs, return empty list without calling AI
        if not urls or all(url is None for url in urls):
            logger.info("No valid URLs provided, returning empty list")
            return []
        
        urls_str = ", ".join([str(url) for url in urls if url])
        logger.info(f"Prompting AI with URLs: {urls_str}")
        prompt = (
            f"""Get me the main domains(without https or http or www or in or com and so on) from the below urls in list object:
            {urls_str}.
            Don't print any other information except the list of urls in json format: {{"Domains":[]}}
            """
        )
        
        response = model.generate_content(prompt)
        response_text = response.text.strip('```json').strip().strip('```').strip()
        logger.info(f"AI response: {response_text}")
        
        try:
            domains_data = json.loads(response_text)
            return domains_data.get("Domains", [])
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON response from AI: {response_text}, returning empty list")
            return []
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting domains from AI: {str(e)}")

@router.post("")
async def map_distributor(request: ProductRequest):
    """
    Map distributor URLs and domains for a specific part_number from Product_sheet.csv.
    Uses part_number to find corresponding web_part_number, then maps to Demo Distributor URLs.csv.
    Updates Product_sheet.csv in S3 with URLs and distributor names.
    """
    try:
        # Read all CSVs from S3
        distributor_df = read_csv_from_s3(BUCKET_NAME, "Distributor URL/Demo Distributor URLs.csv")
        product_df = read_csv_from_s3(BUCKET_NAME, "Product Sheet/Product_sheet.csv")
        
        # Validate required columns
        if 'Product Name' not in distributor_df.columns:
            raise HTTPException(status_code=500, detail="Missing 'Product Name' in Demo Distributor URLs.csv")
        if 'web_part_number' not in product_df.columns:
            raise HTTPException(status_code=500, detail="Missing 'web_part_number' in Product_sheet.csv")
        if 'part_number' not in product_df.columns:
            raise HTTPException(status_code=500, detail="Missing 'part_number' in Product_sheet.csv")

        # Find the web_part_number corresponding to the provided part_number
        product_match = product_df[product_df['part_number'] == request.product_name]
        
        if product_match.empty:
            raise HTTPException(status_code=404, detail=f"No product found for part_number: {request.product_name}")
        
        web_part_number = product_match['web_part_number'].iloc[0]
        logger.info(f"Resolved web_part_number: {web_part_number}")

        # Filter distributor_df using the web_part_number
        distributor_match = distributor_df[distributor_df['Product Name'] == web_part_number]
        
        if distributor_match.empty:
            raise HTTPException(status_code=404, detail=f"No distributor URLs found for web_part_number: {web_part_number}")

        # Get the distributor URLs
        distributor_urls = {
            'Distributor URL 1': distributor_match['Distributor URL 1'].iloc[0] if 'Distributor URL 1' in distributor_match.columns else None,
            'Distributor URL 2': distributor_match['Distributor URL 2'].iloc[0] if 'Distributor URL 2' in distributor_match.columns else None,
            'Distributor URL 3': distributor_match['Distributor URL 3'].iloc[0] if 'Distributor URL 3' in distributor_match.columns else None
        }
        logger.info(f"Distributor URLs: {distributor_urls}")

        # Get domains using Google Generative AI
        url_list = [url for url in distributor_urls.values() if url]
        domains = get_domains_from_urls(url_list)

        # Find the product in product_df using the original part_number
        product_idx = product_df[product_df['part_number'] == request.product_name].index
        
        if product_idx.empty:
            raise HTTPException(status_code=404, detail=f"Product {request.product_name} not found in Product_sheet.csv")

        # Add columns to product_df if they don’t exist
        if 'Distributor Names' not in product_df.columns:
            if 'web_part_number' in product_df.columns:
                position = product_df.columns.get_loc('web_part_number') + 1
                product_df.insert(position, 'Distributor Names', None)
        
        # Update Distributor URLs and Distributor Names
        for dist_col in ['Distributor URL 1', 'Distributor URL 2', 'Distributor URL 3']:
            if dist_col not in product_df.columns:
                product_df[dist_col] = None
            if distributor_urls.get(dist_col):
                product_df.loc[product_idx, dist_col] = distributor_urls[dist_col]
        
        product_df.loc[product_idx, 'Distributor Names'] = ', '.join(domains) if domains else None

        # Write updated DataFrame back to S3
        write_csv_to_s3(product_df, BUCKET_NAME, "Product Sheet/Product_sheet.csv")
        
        num_domains = len(domains)
        distributor_label = "distributor" if num_domains == 1 else "distributors"
        message = f"Successfully updated {num_domains} {distributor_label} in Product_sheet.csv for part_number {request.product_name} (web_part_number: {web_part_number})"

        # Convert the updated row to dict and ensure NaN is handled
        updated_row_dict = product_df.loc[product_idx].iloc[0].replace([np.nan, pd.NA], None).to_dict()

        return {
            "message": message,
            "distributor_urls": {k: v for k, v in distributor_urls.items() if v is not None},
            "domain_names": domains,
            "updated_product_row": updated_row_dict
        }
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
