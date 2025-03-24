# reset_product_sheet.py
from fastapi import APIRouter, HTTPException
import boto3
from botocore.exceptions import ClientError

router = APIRouter(prefix="/reset-product-sheet", tags=["reset-product-sheet"])

ACCESS_KEY = "AKIAWOAVSU6U72T77FUR"
SECRET_KEY = "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"
BUCKET_NAME = "belden-demo-bucket"
SOURCE_KEY = "Product Sheet/Product_sheet_reset.csv"
DESTINATION_KEY = "Product Sheet/Product_sheet.csv"

s3_client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

@router.post("")
async def reset_product_sheet():
    """
    Replace Product_sheet.csv with Product_sheet_reset.csv in S3 bucket.
    """
    try:
        # Check if source file exists
        s3_client.head_object(Bucket=BUCKET_NAME, Key=SOURCE_KEY)
        
        # Copy Product_sheet_reset.csv to Product_sheet.csv
        s3_client.copy_object(
            Bucket=BUCKET_NAME,
            CopySource={'Bucket': BUCKET_NAME, 'Key': SOURCE_KEY},
            Key=DESTINATION_KEY
        )
        
        return {"message": "Successfully replaced Product_sheet.csv with Product_sheet_reset.csv"}
    
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            raise HTTPException(
                status_code=404,
                detail="Product_sheet_reset.csv not found in S3 bucket"
            )
        raise HTTPException(
            status_code=500,
            detail=f"Error resetting product sheet: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"An unexpected error occurred: {str(e)}"
        )
