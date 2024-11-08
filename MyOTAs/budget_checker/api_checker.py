# %%
import requests
import json

# LOCAL
BASE_URL = 'http://127.0.0.1:8000'
# EXTERNAL 
BASE_URL = "https://future-price-calculator-api-fafyfub8d8f6c0a3.northeurope-01.azurewebsites.net/"

def test_root_endpoint():
    print("\nTesting Root Endpoint...")
    response = requests.get(f'{BASE_URL}/')
    if response.status_code == 200:
        print(response.json())
    else:
        print(f"Error: {response.status_code}")

def test_calculate_cost():
    print("\nTesting Calculate Cost Endpoint...")
    url = f'{BASE_URL}/calculate_cost'
    payload = {
        "additional_days": 5,
        "start_day": 1,
        "lang_count": 2,
        "adults_count": 2,
        "refresh_frequency_in_month": 30
    }
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        data = response.json()
        print(f"Total Cost: {data['total_cost']}")
    else:
        print(f"Error: {response.status_code} - {response.text}")

def test_calculate_activity_positions():
    print("\nTesting Calculate Activity Positions Endpoint...")
    url = f'{BASE_URL}/calculate_activity_positions'
    payload = {
        "name": "Activity 1",
        "positions": [
            {
                "days_future": 10,
                "lang_count": 2,
                "adults_count": 2,
                "refresh_frequency": "Daily",
                "refresh_frequency_num": 30,
                "languages_selected": ["English", "Spanish"],
                "adults_selected": ["2 Adults", "3 Adults"]
            }
        ]
    }
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        data = response.json()
        print(f"Total Cost for Activity: {data['total_cost']}")
        print("Positions:")
        for position in data['positions']:
            print(position)
    else:
        print(f"Error: {response.status_code} - {response.text}")

def test_calculate_activities():
    print("\nTesting Calculate Activities Endpoint...")
    url = f'{BASE_URL}/calculate_activities'
    payload = {
        "activities": [
            {
                "name": "Activity 1",
                "positions": [
                    {
                        "days_future": 10,
                        "lang_count": 2,
                        "adults_count": 2,
                        "refresh_frequency": "Daily",
                        "refresh_frequency_num": 30,
                        "languages_selected": ["English", "Spanish"],
                        "adults_selected": ["2 Adults", "3 Adults"]
                    }
                ]
            },
            {
                "name": "Activity 2",
                "positions": [
                    {
                        "days_future": 15,
                        "lang_count": 1,
                        "adults_count": 1,
                        "refresh_frequency": "Weekly",
                        "refresh_frequency_num": 4,
                        "languages_selected": ["English"],
                        "adults_selected": ["2 Adults"]
                    }
                ]
            }
        ]
    }
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        data = response.json()
        print(f"Total Cost for All Activities: {data['total_cost']}")
        print("Positions:")
        for position in data['positions']:
            print(position)
    else:
        print(f"Error: {response.status_code} - {response.text}")

def test_download_csv():
    print("\nTesting Download CSV Endpoint...")
    url = f'{BASE_URL}/download_csv'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        print("Downloaded Data:")
        for item in data:
            print(item)
    else:
        print(f"Error: {response.status_code} - {response.text}")

def test_upload_to_blob():
    print("\nTesting Upload to Blob Endpoint...")
    url = f'{BASE_URL}/upload_to_blob'
    data_to_upload = [
        {
            "Column1": "Value1",
            "Column2": "Value2"
        },
        {
            "Column1": "Value3",
            "Column2": "Value4"
        }
    ]
    payload = {
        "data": data_to_upload,
        "blob_name": "test_upload.csv"
    }
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        data = response.json()
        print(data["message"])
    else:
        print(f"Error: {response.status_code} - {response.text}")

def test_get_configuration():
    print("\nTesting Get Configuration Endpoint...")
    uid = "Carpediem Tours"  # Replace with the actual UID
    url = f'{BASE_URL}/get_configuration/{uid}'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        print("User Configuration:")
        print(json.dumps(data, indent=4))
    else:
        print(f"Error: {response.status_code} - {response.text}")

def test_submit_summary():
    print("\nTesting Submit Summary Endpoint...")
    url = f'{BASE_URL}/submit_summary'
    payload = {
        "activities": [
            {
                "name": "Activity 1",
                "positions": [
                    {
                        "days_future": 10,
                        "lang_count": 2,
                        "adults_count": 2,
                        "refresh_frequency": "Daily",
                        "refresh_frequency_num": 30,
                        "languages_selected": ["English", "Spanish"],
                        "adults_selected": ["2 Adults", "3 Adults"]
                    }
                ]
            },
            {
                "name": "Activity 2",
                "positions": [
                    {
                        "days_future": 15,
                        "lang_count": 1,
                        "adults_count": 1,
                        "refresh_frequency": "Weekly",
                        "refresh_frequency_num": 4,
                        "languages_selected": ["English"],
                        "adults_selected": ["2 Adults"]
                    }
                ]
            }
        ],
        "company_name": "Your Company Name",
        "email": "your.email@example.com"
    }
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        data = response.json()
        print(data["message"])
        print(f"Total Cost: {data['total_cost']}")
    else:
        print(f"Error: {response.status_code} - {response.text}")
# %%
if __name__ == "__main__":
    test_root_endpoint()
    test_calculate_cost()
    test_calculate_activity_positions()
    test_calculate_activities()
    # Uncomment the following lines if Azure Blob Storage is configured
    test_download_csv()
    test_upload_to_blob()
    test_get_configuration()
    test_submit_summary()


# %%
test_root_endpoint()

# %%
