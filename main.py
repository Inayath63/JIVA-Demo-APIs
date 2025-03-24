# main.py
from fastapi import FastAPI
import uvicorn
from map_distributor import router as map_distributor_router
from get_product_file_path import router as file_path_router
from get_datasheet import router as datasheet_router
from get_bucket_details import router as bucket_details_router
from process_product_specs import router as specs_router
from get_prod_details import router as prod_details_router

app = FastAPI(
    title="JIVA Demo APIs",
    description="Consolidated API for Belden product operations",
    version="1.0.0"
)

# Include routers from all files
app.include_router(map_distributor_router)
app.include_router(file_path_router)
app.include_router(datasheet_router)
app.include_router(bucket_details_router)
app.include_router(specs_router)
app.include_router(prod_details_router)

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
