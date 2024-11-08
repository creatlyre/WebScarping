import os
import sys

# Set the current directory to the script location
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the root directory (project directory) to sys.path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)

import yaml
import datetime
import calendar
from typing import List, Dict, Any, Optional
import pandas as pd
from file_management.file_path_manager_future_price import FilePathManagerFuturePrice



class ConfigReader:
    def __init__(self, config_file: str):
        """
        Initialize the ConfigReader with the path to the YAML configuration file.
        """
        self.config_file = config_file
        self.config_data = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """
        Load the YAML configuration file.
        """
        try:
            with open(self.config_file, 'r') as file:
                data = yaml.safe_load(file)
                print(f"Configuration loaded successfully from {self.config_file}.")
                return data
        except FileNotFoundError:
            print(f"Configuration file {self.config_file} not found. Creating a new one.")
            return {"OTAs": {}}
        except yaml.YAMLError as exc:
            print(f"Error parsing YAML file: {exc}")
            return {"OTAs": {}}

    def get_otas(self) -> List[str]:
        """
        Get the list of available OTAs.
        """
        return list(self.config_data.get('OTAs', {}).keys())

    def get_urls_by_ota(self, ota: str) -> List[Dict[str, Any]]:
        """
        Get the list of URL configurations for a specific OTA.
        """
        otas = self.config_data.get('OTAs', {})
        if ota in otas:
            return otas[ota].get('urls', [])
        else:
            print(f"OTA '{ota}' not found in configuration.")
            return []

    def get_url_configurations(self, ota: str, url: str) -> Optional[List[Dict[str, Any]]]:
        """
        Get configurations for a specific URL under a specific OTA.
        """
        urls = self.get_urls_by_ota(ota)
        for url_entry in urls:
            if url_entry.get('url') == url:
                return url_entry.get('configurations', [])
        print(f"URL '{url}' not found under OTA '{ota}'.")
        return None

    def add_ota(self, ota: str):
        """
        Add a new OTA if it doesn't exist.
        """
        if 'OTAs' not in self.config_data:
            self.config_data['OTAs'] = {}
        if ota not in self.config_data['OTAs']:
            self.config_data['OTAs'][ota] = {'urls': []}
            print(f"Added new OTA: '{ota}'")

    def get_configuration_by_criteria(self, ota: str, url: str, adults: int, language: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific configuration based on OTA, URL, adults, and language.
        """
        configurations = self.get_url_configurations(ota, url)
        if configurations:
            for config in configurations:
                if config.get('adults') == adults and config.get('language') == language:
                    return config
        print(f"No matching configuration found for OTA '{ota}', URL '{url}' with adults={adults} and language='{language}'.")
        return None

    def get_schedules(self, ota: str, url: str, adults: int, language: str) -> Optional[List[Dict[str, Any]]]:
        """
        Retrieve all schedules for a specific configuration.
        """
        config = self.get_configuration_by_criteria(ota, url, adults, language)
        if config:
            return config.get('schedules', [])
        return None

    def get_highest_order_schedule(self, schedules: List[Dict[str, Any]]) -> (str, int):
        """
        Determine the highest priority schedule that should run today based on days_in_future.

        Priority:
            - Higher `days_in_future` has higher priority.
            - Among the same `days_in_future`, priority is based on frequency_type.

        Returns:
            Tuple of (frequency_type, days_in_future) if a schedule should run today.
            ("No schedule for today", None) otherwise.
        """
        today = datetime.datetime.today()
        day = today.day
        weekday = today.weekday()  # Monday=0, Sunday=6
        month_length = calendar.monthrange(today.year, today.month)[1]

        # Sort schedules by days_in_future descending, then by frequency priority descending
        sorted_schedules = sorted(
            schedules,
            key=lambda s: (s.get('days_in_future', 0), self.frequency_priority(s.get('frequency_type', ''))),
            reverse=True
        )

        for schedule in sorted_schedules:
            frequency_type = schedule.get('frequency_type', '').lower()
            days_in_future = schedule.get('days_in_future', 0)

            if self.should_run_today(today, day, weekday, month_length, schedule):
                print(f"Today matches schedule: {frequency_type} with days_in_future={days_in_future}")
                return frequency_type, days_in_future

        return "No schedule for today", None

    def frequency_priority(self, frequency_type: str) -> int:
        """
        Assign priority to frequency types.

        Higher frequency_type values have higher priority within the same days_in_future.

        Args:
            frequency_type: The type of frequency (e.g., "daily", "weekly").

        Returns:
            Integer representing priority (higher is higher priority).
        """
        priority_map = {
            "twice_a_day": 5,
            "three_times_a_day": 4,
            "daily": 3,
            "every_other_day": 2,
            "weekly": 1,
            "every_other_week": 0,
            "monthly": -1
            # Add more frequency types as needed
        }
        return priority_map.get(frequency_type, -2)  # Default low priority

    def should_run_today(self, today: datetime.datetime, day: int, weekday: int, month_length: int, schedule: Dict[str, Any]) -> bool:
        """
        Determine if the script should run today based on the schedule.

        Args:
            today: Current datetime.
            day: Current day of the month.
            weekday: Current day of the week (Monday=0, Sunday=6).
            month_length: Total number of days in the current month.
            schedule: Schedule dictionary.

        Returns:
            True if the script should run today based on the schedule, False otherwise.
        """
        frequency_type = schedule.get('frequency_type', '').lower()

        if frequency_type == "daily":
            interval = schedule.get('interval', 1)
            # Determine if today is a run day based on the interval
            if (day - 1) % interval == 0:
                return True

        elif frequency_type == "every_other_day":
            interval = schedule.get('interval', 2)
            if (day - 1) % interval == 0:
                return True

        elif frequency_type == "weekly":
            occurrences_per_week = schedule.get('occurrences_per_week', 1)
            run_days = self.get_weekly_run_days(occurrences_per_week)
            if weekday in run_days:
                return True

        elif frequency_type == "every_other_week":
            run_day = schedule.get('run_day', 'monday').lower()
            run_day_num = self.get_weekday_num(run_day)
            if run_day_num == -1:
                return False
            week_number = today.isocalendar()[1]  # ISO week number
            if week_number % 2 == 0 and weekday == run_day_num:
                return True

        elif frequency_type == "monthly":
            occurrences_per_month = schedule.get('occurrences_per_month', 1)
            run_days = self.get_monthly_run_days(occurrences_per_month, month_length)
            if day in run_days:
                return True

        elif frequency_type in ["twice_a_day", "three_times_a_day"]:
            # These frequencies imply multiple runs within the same day.
            # The usage code should handle multiple runs accordingly.
            return True  # Indicate that today is a run day

        # Add more frequency types as needed

        return False

    def get_weekly_run_days(self, occurrences_per_week: int) -> List[int]:
        """
        Get the run days of the week based on occurrences_per_week.

        Args:
            occurrences_per_week: Number of times to run per week.

        Returns:
            List of weekday numbers (Monday=0, Sunday=6).
        """
        if occurrences_per_week <= 0:
            return []

        # Distribute run days evenly across the week
        interval = 7 / occurrences_per_week
        run_days = []
        for i in range(occurrences_per_week):
            day = int(round(i * interval)) % 7
            if day not in run_days:
                run_days.append(day)
        return run_days

    def get_monthly_run_days(self, occurrences_per_month: int, month_length: int) -> List[int]:
        """
        Get the run days of the month based on occurrences_per_month.

        Args:
            occurrences_per_month: Number of times to run per month.
            month_length: Total number of days in the current month.

        Returns:
            List of day numbers.
        """
        if occurrences_per_month <= 0:
            return []

        interval = month_length / occurrences_per_month
        run_days = []
        for i in range(occurrences_per_month):
            day = int(1 + i * interval)
            if day > month_length:
                day = month_length
            if day not in run_days:
                run_days.append(day)
        return run_days

    def get_weekday_num(self, day_name: str) -> int:
        """
        Convert day name to weekday number.

        Args:
            day_name: Name of the day (e.g., "monday").

        Returns:
            Weekday number (Monday=0, Sunday=6). Returns -1 if invalid.
        """
        days = {
            "monday": 0,
            "tuesday": 1,
            "wednesday": 2,
            "thursday": 3,
            "friday": 4,
            "saturday": 5,
            "sunday": 6
        }
        return days.get(day_name.lower(), -1)
    

    def add_url(self, ota: str, url: str, viewer: str, configurations: List[Dict[str, Any]]):
        """
        Add a new URL configuration under a specific OTA.
        """
        self.add_ota(ota)  # Ensure OTA exists
        ota_entry = self.config_data['OTAs'][ota]

        # Check if URL already exists
        for url_entry in ota_entry['urls']:
            if url_entry['url'] == url:
                print(f"URL '{url}' already exists under OTA '{ota}'. Skipping Add action.")
                return

        new_url_entry = {
            'url': url,
            'viewer': viewer,
            'configurations': configurations
        }
        ota_entry['urls'].append(new_url_entry)
        print(f"Added new URL '{url}' under OTA '{ota}'.")

    def save_config(self):
        """
        Save the current configuration back to the YAML file.
        """
        try:
            with open(self.config_file, 'w') as file:
                yaml.dump(self.config_data, file, sort_keys=False)
                print(f"Configuration saved successfully to {self.config_file}.")
        except Exception as exc:
            print(f"Error saving configuration: {exc}")

    def remove_url(self, ota: str, url: str):
        """
        Remove a URL configuration from a specific OTA.
        """
        if ota not in self.config_data['OTAs']:
            print(f"OTA '{ota}' does not exist. Cannot remove URL '{url}'.")
            return

        ota_entry = self.config_data['OTAs'][ota]
        initial_count = len(ota_entry['urls'])
        ota_entry['urls'] = [u for u in ota_entry['urls'] if u['url'] != url]
        final_count = len(ota_entry['urls'])

        if final_count < initial_count:
            print(f"Removed URL '{url}' from OTA '{ota}'.")
        else:
            print(f"URL '{url}' not found under OTA '{ota}'.")


    def update_url(self, ota: str, url: str, viewer: str, configurations: List[Dict[str, Any]]):
        """
        Update an existing URL configuration under a specific OTA.
        """
        if ota not in self.config_data['OTAs']:
            print(f"OTA '{ota}' does not exist. Cannot update URL '{url}'.")
            return

        ota_entry = self.config_data['OTAs'][ota]
        for url_entry in ota_entry['urls']:
            if url_entry['url'] == url:
                url_entry['viewer'] = viewer
                url_entry['configurations'] = configurations
                print(f"Updated URL '{url}' under OTA '{ota}'.")
                return

        print(f"URL '{url}' not found under OTA '{ota}'. Cannot perform Update action.")
    
    def get_url_entry(self, ota: str, url: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a URL entry from a specific OTA.
        """
        if ota not in self.config_data['OTAs']:
            return None
        for url_entry in self.config_data['OTAs'][ota]['urls']:
            if url_entry['url'] == url:
                return url_entry
        return None
    
    
    def process_csv(self, csv_file, config_reader):
        """
        Process the CSV file to add, remove, or update configurations.
        """
        df = pd.read_csv(csv_file)

        # Iterate over each row where Done is False
        for index, row in df[df['Done'] == False].iterrows():
            action = row['Action'].strip().lower()
            done_flag = row['Done']
            ota = row['OTA'].strip()
            url = row['URL'].strip()
            viewer = row['Viewer'].strip()
            adults = row['Adults'] if not pd.isna(row['Adults']) else None
            language = row['Language'].strip() if not pd.isna(row['Language']) else None
            frequency_type = row['Frequency_Type'].strip().lower() if not pd.isna(row['Frequency_Type']) else None
            interval = int(row['Interval']) if not pd.isna(row['Interval']) else None
            occurrences_per_week = int(row['Occurrences_Per_Week']) if not pd.isna(row['Occurrences_Per_Week']) else None
            occurrences_per_month = int(row['Occurrences_Per_Month']) if not pd.isna(row['Occurrences_Per_Month']) else None
            times_per_day = int(row['Times_Per_Day']) if not pd.isna(row['Times_Per_Day']) else None
            days_in_future = int(row['Days_In_Future']) if not pd.isna(row['Days_In_Future']) else None
            run_day = row['Run_Day'].strip().lower() if not pd.isna(row['Run_Day']) else None
            extract_hours = row['Extract_Hours'] if 'Extract_Hours' in row and not pd.isna(row['Extract_Hours']) else False


            # Prepare the configuration dictionary
            config = {}
            if adults is not None:
                config['adults'] = int(adults)
            if language:
                config['language'] = language
            if frequency_type:
                schedule = {
                    'frequency_type': frequency_type,
                    'days_in_future': days_in_future
                }
                if frequency_type in ['daily', 'every_other_day']:
                    schedule['interval'] = interval
                if frequency_type == 'weekly':
                    schedule['occurrences_per_week'] = occurrences_per_week
                if frequency_type == 'monthly':
                    schedule['occurrences_per_month'] = occurrences_per_month
                if frequency_type in ['twice_a_day', 'three_times_a_day']:
                    schedule['times_per_day'] = times_per_day
                if frequency_type == 'every_other_week':
                    schedule['run_day'] = run_day
                
                schedule['extract_hours'] = bool(extract_hours) if extract_hours is not None else False
                config['schedules'] = [schedule]

            # Handle multiple schedules by checking for multiple rows with the same OTA and URL
            # This example assumes each row corresponds to one schedule. To aggregate multiple schedules, additional logic is needed.

            if action == 'add':
                # Check if URL already exists
                existing_entry = config_reader.get_url_entry(ota, url)
                if existing_entry:
                    print(f"URL '{url}' already exists under OTA '{ota}'. Skipping Add action.")
                else:
                    # Aggregate all schedules for this OTA and URL
                    schedules = []
                    # Find all rows with the same OTA and URL and Action=Add and Done=False
                    related_rows = df[
                        (df['Action'].str.lower() == 'add') &
                        (df['OTA'].str.strip() == ota) &
                        (df['URL'].str.strip() == url) &
                        (df['Done'] == False)
                    ]
                    for _, related_row in related_rows.iterrows():
                        freq_type = related_row['Frequency_Type'].strip().lower() if not pd.isna(related_row['Frequency_Type']) else None
                        sched = {
                            'frequency_type': freq_type,
                            'days_in_future': int(related_row['Days_In_Future']) if not pd.isna(related_row['Days_In_Future']) else None
                        }
                        if freq_type in ['daily', 'every_other_day']:
                            sched['interval'] = int(related_row['Interval']) if not pd.isna(related_row['Interval']) else 1
                        if freq_type == 'weekly':
                            sched['occurrences_per_week'] = int(related_row['Occurrences_Per_Week']) if not pd.isna(related_row['Occurrences_Per_Week']) else 1
                        if freq_type == 'monthly':
                            sched['occurrences_per_month'] = int(related_row['Occurrences_Per_Month']) if not pd.isna(related_row['Occurrences_Per_Month']) else 1
                        if freq_type in ['twice_a_day', 'three_times_a_day']:
                            sched['times_per_day'] = int(related_row['Times_Per_Day']) if not pd.isna(related_row['Times_Per_Day']) else 1
                        if freq_type == 'every_other_week':
                            sched['run_day'] = related_row['Run_Day'].strip().lower() if not pd.isna(related_row['Run_Day']) else 'monday'
                        sched['extract_hours'] = bool(related_row['Extract_Hours']) if not pd.isna(related_row['Extract_Hours']) else False
                        schedules.append(sched)
                    # Prepare configurations
                    configurations = [{
                        'adults': int(row['Adults']) if not pd.isna(row['Adults']) else None,
                        'language': language,
                        'schedules': schedules
                    }]
                    # Add the URL
                    config_reader.add_url(ota=ota, url=url, viewer=viewer, configurations=configurations)
                    # Mark all related rows as Done
                    df.loc[
                        (df['Action'].str.lower() == 'add') &
                        (df['OTA'].str.strip() == ota) &
                        (df['URL'].str.strip() == url) &
                        (df['Done'] == False),
                        'Done'
                    ] = True

            elif action == 'remove':
                config_reader.remove_url(ota, url)
                # Mark the row as Done
                df.at[index, 'Done'] = True

            elif action == 'update':
                # Find the existing URL entry
                existing_entry = config_reader.get_url_entry(ota, url)
                if not existing_entry:
                    print(f"URL '{url}' does not exist under OTA '{ota}'. Cannot perform Update action.")
                    continue
                # Aggregate all schedules for this OTA and URL
                schedules = []
                related_rows = df[
                    (df['Action'].str.lower() == 'update') &
                    (df['OTA'].str.strip() == ota) &
                    (df['URL'].str.strip() == url) &
                    (df['Done'] == False)
                ]
                for _, related_row in related_rows.iterrows():
                    freq_type = related_row['Frequency_Type'].strip().lower() if not pd.isna(related_row['Frequency_Type']) else None
                    sched = {
                        'frequency_type': freq_type,
                        'days_in_future': int(related_row['Days_In_Future']) if not pd.isna(related_row['Days_In_Future']) else None
                    }
                    if freq_type in ['daily', 'every_other_day']:
                        sched['interval'] = int(related_row['Interval']) if not pd.isna(related_row['Interval']) else 1
                    if freq_type == 'weekly':
                        sched['occurrences_per_week'] = int(related_row['Occurrences_Per_Week']) if not pd.isna(related_row['Occurrences_Per_Week']) else 1
                    if freq_type == 'monthly':
                        sched['occurrences_per_month'] = int(related_row['Occurrences_Per_Month']) if not pd.isna(related_row['Occurrences_Per_Month']) else 1
                    if freq_type in ['twice_a_day', 'three_times_a_day']:
                        sched['times_per_day'] = int(related_row['Times_Per_Day']) if not pd.isna(related_row['Times_Per_Day']) else 1
                    if freq_type == 'every_other_week':
                        sched['run_day'] = related_row['Run_Day'].strip().lower() if not pd.isna(related_row['Run_Day']) else 'monday'
                    sched['extract_hours'] = bool(related_row['Extract_Hours']) if not pd.isna(related_row['Extract_Hours']) else False
                    schedules.append(sched)
                # Prepare configurations
                configurations = [{
                    'adults': int(row['Adults']) if not pd.isna(row['Adults']) else None,
                    'language': language,
                    'schedules': schedules
                }]
                # Update the URL
                config_reader.update_url(ota=ota, url=url, viewer=viewer, configurations=configurations)
                # Mark all related rows as Done
                df.loc[
                    (df['Action'].str.lower() == 'update') &
                    (df['OTA'].str.strip() == ota) &
                    (df['URL'].str.strip() == url) &
                    (df['Done'] == False),
                    'Done'
                ] = True

            else:
                print(f"Unknown action '{row['Action']}' at row {index + 2}. Skipping.")

        # Save the updated configuration
        config_reader.save_config()

        # Save the updated CSV file with Done flags updated
        df.to_csv(csv_file, index=False)
        print(f"CSV file '{csv_file}' updated successfully.")

