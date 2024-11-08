# app.py

import pandas as pd
from fastapi import FastAPI, HTTPException
from azure.storage.blob import BlobServiceClient
import datetime
import uuid
import json
from io import StringIO
import logging
from typing import List
from pydantic import BaseModel

# Load configurations from config.json
config = json.load(open("config.json"))

# Azure Storage credentials
AZURE_STORAGE_CONNECTION_STRING = config['AZURE_STORAGE_CONNECTION_STRING']
CONTAINER_NAME = config['CONTAINER_NAME']
SAFUTUREPRICE_CONNECTION_STRING = config['SAFUTUREPRICE_CONNECTION_STRING']
SAFUTUREPRICE_CONTAINER_NAME = config['SAFUTUREPRICE_CONTAINER_NAME']
BLOB_NAME = 'LinksFuturePrice_GYG.csv'

# Load pricing tiers from config
pricing_tiers = config.get('PRICING_TIERS')

# Constants
time_per_request = 6  # seconds
machine_price_per_sec = 0.001984161851851852  # EUR per second
refresh_frequency_options = {
    'Daily': 30,
    'Twice a Day': 60,
    'Every Other Day': 15,
    'Three Times a Week': 12,
    'Twice a Week': 8,
    'Weekly': 4,
    'Three Times a Month': 3,
    'Twice a Month': 2,
    'Monthly': 1,
    'Custom': None
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG for more detailed logs
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

app = FastAPI()

# Define Pydantic models for request and response validation
class Position(BaseModel):
    days_future: int
    lang_count: int
    adults_count: int
    refresh_frequency: str
    refresh_frequency_num: int
    cost: float = 0.0
    languages_selected: List[str] = []
    adults_selected: List[str] = []

class Activity(BaseModel):
    name: str
    positions: List[Position]

class UploadRequest(BaseModel):
    data: List[dict]
    blob_name: str

class CalculateCostRequest(BaseModel):
    additional_days: int
    start_day: int
    lang_count: int
    adults_count: int
    refresh_frequency_in_month: int

class SummaryRequest(BaseModel):
    activities: List[Activity]
    company_name: str
    email: str

class ActivitiesRequest(BaseModel):
    activities: List[Activity]

# Function to calculate cost
def calculate_cost(additional_days, start_day, lang_count, adults_count, refresh_frequency_in_month, time_per_request, machine_price_per_sec):
    """
    Calculate the cost based on tiered pricing factors.
    """
    total_cost = 0
    remaining_days = additional_days
    current_day = start_day

    logging.info(f"Calculating cost for {additional_days} additional days starting from day {start_day}.")

    for tier in pricing_tiers:
        if remaining_days <= 0:
            break

        tier_min = tier["min_days"]
        tier_max = tier["max_days"]
        factor = tier["factor"]

        if tier_min <= current_day <= tier_max:
            # Calculate how many days fall into this tier
            days_in_tier = min(remaining_days, tier_max - current_day + 1)
            
            logging.debug(f"Applying Tier {tier_min}-{tier_max} Days: {days_in_tier} days * factor {factor}.")

            # Calculate the number of requests for these days
            requests = days_in_tier * lang_count * adults_count * refresh_frequency_in_month

            # Calculate the total time required for these requests
            time_single_run = requests * time_per_request

            # Calculate the cost with the applied factor
            cost = time_single_run * machine_price_per_sec * factor

            logging.debug(f"Cost for {days_in_tier} days: {cost:.4f} EUR.")

            # Add to the total cost
            total_cost += cost

            # Update remaining days and current day
            remaining_days -= days_in_tier
            current_day += days_in_tier

    logging.info(f"Total cost calculated: {total_cost:.4f} EUR.")

    return total_cost

# Function to upload DataFrame to Azure Blob Storage
def upload_to_azure_blob(df, blob_name):
    try:
        logging.info(f"Uploading file to Azure Blob Storage: {blob_name}")
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
        csv_data = df.to_csv(index=False)
        blob_client.upload_blob(csv_data, overwrite=True)
        logging.info("File successfully uploaded!")
    except Exception as e:
        logging.error(f"Failed to upload file to Azure Blob Storage: {e}")
        raise e

# Function to download CSV from Azure Blob Storage
def download_csv_from_blob(container_name, blob_name, connection_string):
    try:
        logging.info(f"Downloading CSV from Azure Blob Storage: {blob_name}")
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        csv_data = blob_client.download_blob().readall().decode('utf-8')
        df = pd.read_csv(StringIO(csv_data))
        logging.info(f"Downloaded {len(df)} rows from blob storage.")
        return df
    except Exception as e:
        logging.error(f"Error downloading CSV from blob storage: {e}")
        raise e

# Function to parse configuration DataFrame into activities dictionary
def parse_configuration(df_configuration):
    activities = {}
    for index, row in df_configuration.iterrows():
        activity_name = row['Activity Name']
        if activity_name not in activities:
            activities[activity_name] = {
                'name': activity_name,
                'positions': []
            }
        activities[activity_name]['positions'].append({
            'days_future': row['Days in Future'],
            'lang_count': row['Language Count'],
            'adults_count': row['Adults Count'],
            'refresh_frequency': row['Refresh Frequency per Month'],
            'refresh_frequency_num': row['refresh_frequency_num'],
            'cost': row['Cost (EUR) per month']
        })
    logging.info(f"Parsed configuration for {len(activities)} activities.")
    return activities

# Function to calculate activity positions
def calculate_activity_positions(activity: Activity):
    total_cost = 0
    total_days_covered = 0
    positions_data = []
    
    # Sort positions by days_future
    activity.positions.sort(key=lambda x: x.days_future)
    
    for position in activity.positions:
        days_future = position.days_future
        lang_count = position.lang_count
        adults_count = position.adults_count
        refresh_frequency_in_month = position.refresh_frequency_num
        
        # Calculate additional days
        additional_days = max(0, days_future - total_days_covered)
        start_day = total_days_covered + 1 if additional_days > 0 else 0
        total_days_covered = max(total_days_covered, days_future)
        
        if additional_days > 0 and start_day > 0:
            cost = calculate_cost(
                additional_days=additional_days,
                start_day=start_day,
                lang_count=lang_count,
                adults_count=adults_count,
                refresh_frequency_in_month=refresh_frequency_in_month,
                time_per_request=time_per_request,
                machine_price_per_sec=machine_price_per_sec
            )
        else:
            cost = 0
            logging.debug(f"No additional days to calculate for position with days_future={days_future}.")

        position.cost = cost
        total_cost += cost
        positions_data.append(position.dict())
    
    logging.info(f"Calculated positions for activity '{activity.name}' with total cost: {total_cost:.4f} EUR.")
    return positions_data, total_cost

# Endpoint to calculate cost
@app.post("/calculate_cost")
async def api_calculate_cost(request: CalculateCostRequest):
    try:
        total_cost = calculate_cost(
            additional_days=request.additional_days,
            start_day=request.start_day,
            lang_count=request.lang_count,
            adults_count=request.adults_count,
            refresh_frequency_in_month=request.refresh_frequency_in_month,
            time_per_request=time_per_request,
            machine_price_per_sec=machine_price_per_sec
        )
        return {"total_cost": total_cost}
    except Exception as e:
        logging.error(f"Error calculating cost: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Endpoint to download CSV from Azure Blob Storage
@app.get("/download_csv")
async def api_download_csv():
    try:
        df = download_csv_from_blob(SAFUTUREPRICE_CONTAINER_NAME, BLOB_NAME, SAFUTUREPRICE_CONNECTION_STRING)
        data = df.to_dict(orient='records')
        return data
    except Exception as e:
        logging.error(f"Error downloading CSV: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Endpoint to upload DataFrame to Azure Blob Storage
@app.post("/upload_to_blob")
async def api_upload_to_blob(request: UploadRequest):
    try:
        df = pd.DataFrame(request.data)
        upload_to_azure_blob(df, request.blob_name)
        return {"message": "File successfully uploaded!"}
    except Exception as e:
        logging.error(f"Failed to upload file: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Endpoint to get user configuration based on UID
@app.get("/get_configuration/{uid}")
async def api_get_configuration(uid: str):
    try:
        df_configuration = download_csv_from_blob(SAFUTUREPRICE_CONTAINER_NAME, BLOB_NAME, SAFUTUREPRICE_CONNECTION_STRING)
        df_user_config = df_configuration[df_configuration['Viewer'] == uid]
        activities = parse_configuration(df_user_config)
        return activities
    except Exception as e:
        logging.error(f"Error fetching configuration for UID {uid}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Endpoint to calculate activity positions
@app.post("/calculate_activity_positions")
async def api_calculate_activity_positions(activity: Activity):
    try:
        positions_data, total_cost = calculate_activity_positions(activity)
        return {"positions": positions_data, "total_cost": total_cost}
    except Exception as e:
        logging.error(f"Error calculating activity positions: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Endpoint to calculate multiple activities
@app.post("/calculate_activities")
async def api_calculate_activities(request: ActivitiesRequest):
    try:
        all_positions = []
        total_cost = 0
        for activity in request.activities:
            positions_data, activity_cost = calculate_activity_positions(activity)
            all_positions.extend(positions_data)
            total_cost += activity_cost
        return {"positions": all_positions, "total_cost": total_cost}
    except Exception as e:
        logging.error(f"Error calculating activities: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Endpoint to handle the summary and send to verification
@app.post("/submit_summary")
async def api_submit_summary(request: SummaryRequest):
    try:
        summary_data = []
        total_cost = 0
        for activity in request.activities:
            positions_data, activity_cost = calculate_activity_positions(activity)
            total_cost += activity_cost
            summary_data.append({
                'Activity Name': activity.name,
                'Cost (EUR)': activity_cost,
                'URL': '',  # URL can be included in the activity if needed
            })
        
        df_summary = pd.DataFrame(summary_data)
        unique_filename = f"{datetime.date.today().strftime('%Y-%m-%d')}_{request.company_name}_future_price_request_{uuid.uuid4()}.csv"
        df_summary['Viewer'] = request.company_name
        df_summary['Email'] = request.email

        # Upload the summary to Azure Blob Storage
        upload_to_azure_blob(df_summary, unique_filename)
        logging.info(f"Summary uploaded with filename: {unique_filename}")
        return {"message": "Request has been sent!", "total_cost": total_cost}
    except Exception as e:
        logging.error(f"Error submitting summary: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Root endpoint to confirm the API is running
@app.get("/")
async def root():
    return {"message": "API is running smoothly"}    
    
    