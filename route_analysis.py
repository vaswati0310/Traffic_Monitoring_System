import os
import json
import requests
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

# Constants
S3_BUCKET_NAME = 'traffic-monitoring-data-lake'
REGION = 'eu-north-1'
TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY", "XyglsOO95QlsXCXM13vdQt8ZhrKECesI")

# AWS S3 client
s3_client = boto3.client('s3', region_name=REGION)

# List of routes with start/end coordinates and location names
ROUTES = [
    {
        "name": "dehradun_to_chandigarh",
        "start_lat": 30.3165,
        "start_lon": 78.0322,
        "end_lat": 30.7333,
        "end_lon": 76.7794
    },
    {
        "name": "kolkata_to_tinsukia",
        "start_lat": 22.5726,
        "start_lon": 88.3639,
        "end_lat": 27.4924,
        "end_lon": 95.3468
    },
    {
        "name": "delhi_to_dehradun",
        "start_lat": 28.7041,
        "start_lon": 77.1025,
        "end_lat": 30.3165,
        "end_lon": 78.0322
    },
    {
        "name": "mumbai_to_goa",
        "start_lat": 19.0760,
        "start_lon": 72.8777,
        "end_lat": 15.2993,
        "end_lon": 74.1240
    },
    {
        "name": "bangalore_to_chennai",
        "start_lat": 12.9716,
        "start_lon": 77.5946,
        "end_lat": 13.0827,
        "end_lon": 80.2707
    },
    {
        "name": "jaipur_to_kota",
        "start_lat": 26.9124,
        "start_lon": 75.7873,
        "end_lat": 25.2138,
        "end_lon": 75.8648
    }
]

def fetch_route_data(start_lat, start_lon, end_lat, end_lon):
    url = (
        f"https://api.tomtom.com/routing/1/calculateRoute/"
        f"{start_lat},{start_lon}:{end_lat},{end_lon}/json"
        f"?key={TOMTOM_API_KEY}&traffic=true"
    )
    try:
        response = requests.get(url)
        response.raise_for_status()
        print(f"Route fetched successfully for {start_lat},{start_lon} to {end_lat},{end_lon}.")
        return response.json()
    except Exception as e:
        print(f"Failed to fetch route data for {start_lat},{start_lon} to {end_lat},{end_lon}: {e}")
        return None

def upload_route_to_s3(route_data, location_name):
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d_%H-%M-%S')
    file_key = f"routes/{location_name}/static_route_{timestamp}.json"

    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=file_key,
            Body=json.dumps(route_data, indent=4),
            ContentType='application/json'
        )
        print(f"Route uploaded to S3: s3://{S3_BUCKET_NAME}/{file_key}")
    except ClientError as e:
        print(f"Failed to upload route to S3 for {location_name}: {e}")

def main():
    for route in ROUTES:
        route_data = fetch_route_data(
            route["start_lat"],
            route["start_lon"],
            route["end_lat"],
            route["end_lon"]
        )
        if route_data:
            upload_route_to_s3(route_data, route["name"])
        else:
            print(f"No route data to upload for {route['name']}.")

if __name__ == "__main__":
    main()