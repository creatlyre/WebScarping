# test_app.py

import requests
import json

# LOCAL
BASE_URL = 'http://127.0.0.1:5000'
# EXTERNAL 
BASE_URL = "https://future-price-calculator-api-fafyfub8d8f6c0a3.northeurope-01.azurewebsites.net/"

def test_root_endpoint():
    print("\nTesting Root Endpoint...")
    response = requests.get(f'{BASE_URL}/')
    if response.status_code == 200:
        print("Success:", response.json())
    else:
        print(f"Error: {response.status_code} - {response.text}")


def test_calculate_cost():
    print("\nTesting Calculate Cost Endpoint...")
    url = f'{BASE_URL}/calculate_cost'
    payload = {
        "additional_days": 7,
        "start_day": 1,
        "lang_count": 1,
        "adults_count": 1,
        "refresh_frequency_in_month": 30  # 30 refreshes per month (daily)
    }
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        data = response.json()
        print(f"Total Cost: {data['total_cost']:.4f} EUR")
    else:
        print(f"Error: {response.status_code} - {response.text}")

def test_calculate_activity_positions():
    print("\nTesting Calculate Activity Positions Endpoint...")
    url = f'{BASE_URL}/calculate_activity_positions'
    payload = {
        "name": "URL_1",
        "url": "https://www.viator.com/tours/Venice/Murano-Glass-and-Burano-Lace-Tour-from-Venice/d522-3731MURANO",
        "ota": 'Viator',
        "positions": [
            {
                "days_future": 7,
                "lang_count": 1,
                "adults_count": 1,
                "refresh_frequency": "daily",
                "refresh_frequency_num": 30,  # 30 refreshes per month (daily)
                "languages_selected": ["en"],
                "adults_selected": ["1 Adult"],
                "frequency_type": "weekly",
                "interval": 30,
                "times_per_day": 1,
                "run_day": None,  # Not applicable for daily frequency
                "extract_hours": False
            },
            {
                "days_future": 150,
                "lang_count": 1,
                "adults_count": 1,
                "refresh_frequency": "daily",
                "refresh_frequency_num": 8,  # 30 refreshes per month (daily)
                "languages_selected": ["en"],
                "adults_selected": ["1 Adult"],
                "frequency_type": "daily",
                "interval": 30,
                "occurrences_per_week": 7,
                "occurrences_per_month": 30,
                "times_per_day": 1,
                "run_day": None,  # Not applicable for daily frequency
                "extract_hours": False
            }
        ]
    }
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        data = response.json()
        print("Positions:")
        for position in data['positions']:
            print(json.dumps(position, indent=4))
        print(f"Total Cost for Activity '{payload['name']}': {data['total_cost']:.4f} EUR")
    else:
        print(f"Error: {response.status_code} - {response.text} ")

def test_calculate_activities():
    print("\nTesting Calculate Activities Endpoint...")
    url = f'{BASE_URL}/calculate_activities'
    payload = {
        "activities": [
            {
                "name": "URL_1_Viator",
                "url": "https://www.viator.com/tours/Venice/Murano-Glass-and-Burano-Lace-Tour-from-Venice/d522-3731MURANO",
                "positions": [
                    {
                        "days_future": 7,
                        "lang_count": 1,
                        "adults_count": 2,
                        "refresh_frequency": "daily",
                        "refresh_frequency_num": 30,  # 30 refreshes per month (daily)
                        "languages_selected": ["en"],
                        "adults_selected": ["1 Adult" , "2 Adults"],
                        "frequency_type": "daily",
                        "interval": 30,
                        "occurrences_per_week": 7,
                        "occurrences_per_month": 30,
                        "times_per_day": 1,
                        "run_day": None,
                        "extract_hours": False
                    },
                    {
                        "days_future": 7,
                        "lang_count": 1,
                        "adults_count": 1,
                        "refresh_frequency": "daily",
                        "refresh_frequency_num": 30,  # 30 refreshes per month (daily)
                        "languages_selected": ["en"],
                        "adults_selected": ["1 Adult"],
                        "frequency_type": "daily",
                        "interval": 30,
                        "occurrences_per_week": 7,
                        "occurrences_per_month": 30,
                        "times_per_day": 1,
                        "run_day": None,
                        "extract_hours": False
                    }
                ]
            },
            {
                "name": "URL_2_GYG",
                "url": "https://www.getyourguide.com/rome-l33/rome-colosseum-roman-forum-and-palatine-hill-guided-tour-t406538/",
                "positions": [
                    {
                        "days_future": 15,
                        "lang_count": 1,
                        "adults_count": 1,
                        "refresh_frequency": "weekly",
                        "refresh_frequency_num": 4,  # 4 refreshes per month (weekly)
                        "languages_selected": ["en"],
                        "adults_selected": ["2 Adults"],
                        "frequency_type": "weekly",
                        "interval": 4,
                        "occurrences_per_week": 1,
                        "occurrences_per_month": 4,
                        "times_per_day": 1,
                        "run_day": "friday",
                        "extract_hours": True
                    }
                ]
            }
        ]
    }
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        data = response.json()
        print(f"Total Cost for All Activities: {data['total_cost']:.4f} EUR")
        print("Positions:")
        for position in data['positions']:
            print(json.dumps(position, indent=4))
    else:
        print(f"Error: {response.status_code} - {response.text}")

def test_download_csv():
    print("\nTesting Download CSV Endpoint...")
    url = f'{BASE_URL}/download_csv'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        print("Downloaded Data (First 5 Records):")
        for item in data[:5]:  # Print only first 5 items for brevity
            print(json.dumps(item, indent=4))
    else:
        print(f"Error: {response.status_code} - {response.text}")

def test_upload_to_blob():
    print("\nTesting Upload to Blob Endpoint...")
    url = f'{BASE_URL}/upload_to_blob'
    data_to_upload = [
        {
            "OTA": "Viator",
            "URL": "https://www.viator.com/tours/Venice/Murano-Glass-and-Burano-Lace-Tour-from-Venice/d522-3731MURANO",
            "Viewer": "TestViewer",
            "Adults": 1,
            "Language": "en",
            "Frequency_Type": "daily",
            "Interval": 30,
            "Occurrences_Per_Week": 7,
            "Occurrences_Per_Month": 30,
            "Times_Per_Day": 1,
            "Days_In_Future": 7,
            "Run_Day": "",
            "Extract_Hours": False
        },
        {
            "OTA": "GetYourGuide",
            "URL": "https://www.getyourguide.com/rome-l33/rome-colosseum-roman-forum-and-palatine-hill-guided-tour-t406538/",
            "Viewer": "Carpediem Tours",
            "Adults": 1,
            "Language": "es",
            "Frequency_Type": "weekly",
            "Interval": 4,
            "Occurrences_Per_Week": 1,
            "Occurrences_Per_Month": 4,
            "Times_Per_Day": 1,
            "Days_In_Future": 35,
            "Run_Day": "friday",
            "Extract_Hours": True
        }
    ]
    payload = {
        "data": data_to_upload,
        "blob_name": "test_upload_summary.csv"
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
    uid = "Carpediem Tours"  # Replace with the actual UID if different
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
                "name": "Viator",
                "url": "https://www.viator.com/tours/Venice/Murano-Glass-and-Burano-Lace-Tour-from-Venice/d522-3731MURANO",
                "positions": [
                    # Position 1: Adult 1, Language 'en', Days in Future = 7, Refresh Daily
                    {
                        "days_future": 7,
                        "lang_count": 1,
                        "adults_count": 1,
                        "refresh_frequency": "daily",
                        "refresh_frequency_num": 30,  # Daily refreshes
                        "languages_selected": ["en"],
                        "adults_selected": ["1 Adult"],
                        "frequency_type": "daily",
                        "interval": 1,
                        "times_per_day": 1,
                        "run_day": None,
                        "extract_hours": False
                    },
                    # Position 2: Same Adult and Language, Days in Future = 30, Refresh Weekly
                    {
                        "days_future": 30,
                        "lang_count": 1,
                        "adults_count": 1,
                        "refresh_frequency": "weekly",
                        "refresh_frequency_num": 4,  # Weekly refreshes
                        "languages_selected": ["en"],
                        "adults_selected": ["1 Adult"],
                        "frequency_type": "weekly",
                        "occurrences_per_week": 1,
                        "run_day": "monday",
                        "extract_hours": False
                    },
                    # Position 3: Same Adult and Language, Days in Future = 150, Refresh Monthly
                    {
                        "days_future": 150,
                        "lang_count": 1,
                        "adults_count": 1,
                        "refresh_frequency": "monthly",
                        "refresh_frequency_num": 1,  # Monthly refreshes
                        "languages_selected": ["en"],
                        "adults_selected": ["1 Adult"],
                        "frequency_type": "monthly",
                        "occurrences_per_month": 1,
                        "run_day": None,
                        "extract_hours": False
                    }
                ]
            },
            {
                "name": "GetYourGuide",
                "url": "https://www.getyourguide.com/rome-l33/rome-colosseum-roman-forum-and-palatine-hill-guided-tour-t406538/",
                "positions": [
                    # Position 1: Adult 2, Language 'en', Days in Future = 7, Refresh Daily
                    {
                        "days_future": 7,
                        "lang_count": 1,
                        "adults_count": 1,
                        "refresh_frequency": "daily",
                        "refresh_frequency_num": 30,
                        "languages_selected": ["en"],
                        "adults_selected": ["2 Adults"],
                        "frequency_type": "daily",
                        "interval": 1,
                        "times_per_day": 1,
                        "run_day": None,
                        "extract_hours": False
                    },
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
        print(f"Total Cost: {data['total_cost']:.4f} EUR")
    else:
        print(f"Error: {response.status_code} - {response.text}")


# %%
if __name__ == "__main__":
    test_root_endpoint()
    # test_calculate_cost()
    # Uncomment the following lines as needed
    test_calculate_activity_positions()
    # test_calculate_activities()
    # test_download_csv()
    # test_upload_to_blob()
    # test_get_configuration()
    # test_submit_summary()

# %%
