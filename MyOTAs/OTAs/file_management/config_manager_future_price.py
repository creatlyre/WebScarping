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
    
    def is_schedule_due(self, schedule: Dict[str, Any]) -> bool:
        """
        Check if today's date is greater than or equal to the schedule's next_run date.

        Args:
            schedule: Schedule dictionary containing 'next_run'.

        Returns:
            True if today >= next_run, False otherwise.
        """
        next_run_str = schedule.get('next_run')
        if not next_run_str:
            print("No 'next_run' specified for the schedule.")
            return False

        try:
            next_run_date = datetime.datetime.strptime(next_run_str, '%Y-%m-%d').date()
        except ValueError:
            print(f"Invalid date format for next_run: {next_run_str}")
            return False

        today = datetime.date.today()
        return today >= next_run_date
    

    def get_highest_order_schedule(self, schedules: List[Dict[str, Any]]):
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

    
    def is_run_day(self, test_date: datetime.datetime, schedule: Dict[str, Any]) -> bool:
        """
        Generalized "is this date a valid run day?" check,
        mirroring the logic from should_run_today but for arbitrary test_date.
        """
        day = test_date.day
        weekday = test_date.weekday()  # Monday=0, Sunday=6
        month_length = calendar.monthrange(test_date.year, test_date.month)[1]

        frequency_type = schedule.get('frequency_type', '').lower()

        if frequency_type == "daily":
            interval = schedule.get('interval', 1)
            # If day=1 -> 0 % interval == 0, etc.
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
            # Check if it's an "even" ISO week
            iso_week_number = test_date.isocalendar()[1]
            if iso_week_number % 2 == 0 and weekday == run_day_num:
                return True

        elif frequency_type == "monthly":
            occurrences_per_month = schedule.get('occurrences_per_month', 1)
            run_days = self.get_monthly_run_days(occurrences_per_month, month_length)
            if day in run_days:
                return True

        elif frequency_type in ["twice_a_day", "three_times_a_day"]:
            # Always a valid run day (the time-of-day logic is handled externally).
            return True

        return False

    # -------------------------------------------------------------------------
    #                        CALCULATE NEXT RUN
    # -------------------------------------------------------------------------
    def calculate_next_run_date(self, schedules: Dict[str, Any]) -> Optional[str]:
        """
        Calculate the *next* day in the future that matches the schedule's frequency.

        Return the date string in YYYY-MM-DD format if found,
        or None if no valid date is found within a certain range.
        """
        # Starting from tomorrow (or you can start from "today")
        today = datetime.datetime.today()      
        day = today.day
        weekday = today.weekday()  # Monday=0, Sunday=6
        month_length = calendar.monthrange(today.year, today.month)[1]

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

                today = datetime.datetime.now()
                start_date = today + datetime.timedelta(days=1)

                # We'll search up to 365 days out. Adjust as desired.
                for i in range(0, 365):
                    test_date = start_date + datetime.timedelta(days=i)
                    # If the test_date matches the frequency logic, that's our next run.
                    if self.is_run_day(test_date, schedule):
                        return test_date.strftime('%Y-%m-%d')

        return None

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
    

    def add_url(self, ota: str, url: str, viewer: str, city: str, configurations: List[Dict[str, Any]]):
        """
        Add a new URL configuration under a specific OTA.
        """
        self.add_ota(ota)  # Ensure OTA exists
        ota_entry = self.config_data['OTAs'][ota]

        # Check if URL already exists
        for url_entry in ota_entry['urls']:
            if url_entry['url'] == url:
                print(f"URL '{url}' already exists under OTA '{ota}'. Adding configurations.")
                # Update viewer if needed
                url_entry['viewer'] = viewer
                url_entry['city'] = city
                for new_config in configurations:
                    self.add_or_update_configuration(url_entry, new_config)
                return

        new_url_entry = {
            'url': url,
            'viewer': viewer,
            'city': city,
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
            
    def update_next_last_run(
        self,
        ota: str,
        url: str,
        adults: int,
        language: str,
        frequency_type: str,
        next_run: Optional[str],
        last_run: Optional[str]
    ) -> None:
        """
        Update the next_run and last_run fields for a specific schedule
        under a given OTA, URL, adults, language, and frequency_type.

        Args:
            ota: The OTA name (e.g., "GYG").
            url: The URL under that OTA to update.
            adults: The 'adults' field of the configuration to match.
            language: The 'language' field of the configuration to match.
            frequency_type: The frequency_type of the schedule to update (e.g., "daily", "weekly").
            next_run: The new next_run date (YYYY-MM-DD string) or None if no update.
            last_run: The new last_run date (YYYY-MM-DD string) or None if no update.
        """
        try:
            # Access the specific OTA entry
            ota_entry = self.config_data['OTAs'][ota]
        except KeyError:
            print(f"OTA '{ota}' not found in configuration.")
            return

        # Iterate through URLs to find the matching one
        url_found = False
        for url_entry in ota_entry.get('urls', []):
            if url_entry.get('url') == url:
                url_found = True
                break

        if not url_found:
            print(f"URL '{url}' not found under OTA '{ota}'. Cannot update.")
            return

        # Iterate through configurations to find the matching one
        config_found = False
        for config in url_entry.get('configurations', []):
            if config.get('adults') == adults and config.get('language') == language:
                config_found = True
                break

        if not config_found:
            print(f"No configuration found for adults={adults} and language='{language}' under OTA '{ota}', URL '{url}'.")
            return

        # Iterate through schedules to find the matching frequency_type
        schedule_found = False
        for schedule in config.get('schedules', []):
            if schedule.get('frequency_type') == frequency_type:
                # Update next_run and last_run if provided
                if next_run is not None:
                    schedule['next_run'] = next_run
                if last_run is not None:
                    schedule['last_run'] = last_run
                schedule_found = True
                print(
                    f"Updated schedule for OTA='{ota}', URL='{url}', adults={adults}, "
                    f"language='{language}', frequency='{frequency_type}': "
                    f"next_run='{next_run}', last_run='{last_run}'."
                )
                break

        if not schedule_found:
            print(
                f"No schedule with frequency_type='{frequency_type}' found under OTA='{ota}', "
                f"URL='{url}', adults={adults}, language='{language}'."
            )
            return

        # Save the updated configuration back to the YAML file
        self.save_config()
        
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


    def update_url(self, ota: str, url: str, viewer: str, city: str, configurations: List[Dict[str, Any]]):
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
                url_entry['city'] = city
                for new_config in configurations:
                    self.add_or_update_configuration(url_entry, new_config)
                print(f"Updated URL '{url}' under OTA '{ota}'.")
                return

        print(f"URL '{url}' not found under OTA '{ota}'. Cannot perform Update action.")


    def add_or_update_configuration(self, url_entry: Dict[str, Any], new_config: Dict[str, Any]):
            """
            Add or update a configuration in a URL entry.
            If a configuration with the same adults and language exists, merge or update the schedules.
            If a schedule with the same frequency_type exists, update it with new values.
            Also, set/refresh next_run and ensure last_run is present.
            """
            existing_configs = url_entry.get('configurations', [])
            found_match = False

            for config in existing_configs:
                # Check if same "adults" and "language"
                if config.get('adults') == new_config.get('adults') and config.get('language') == new_config.get('language'):
                    found_match = True

                    # Merge or update schedules
                    existing_schedules = config.get('schedules', [])
                    new_schedules = new_config.get('schedules', [])

                    for new_schedule in new_schedules:
                        frequency_type = new_schedule.get('frequency_type')
                        existing_schedule = self.get_schedule_by_frequency_type(existing_schedules, frequency_type)

                        if existing_schedule:
                            # Update existing schedule fields
                            existing_schedule.update(new_schedule)
                        else:
                            # Add new schedule
                            existing_schedules.append(new_schedule)

                    # After merging schedules, recalc next_run for each schedule
                    for schedule in existing_schedules:
                        # Ensure last_run key is present (if not, create it as None)
                        if 'last_run' not in schedule:
                            schedule['last_run'] = None

                        # Calculate and update next_run
                        next_date = self.calculate_next_run_date(schedule)
                        schedule['next_run'] = next_date or None

                    break  # end for config loop

            if not found_match:
                # Configuration not found, add new
                new_schedules = new_config.get('schedules', [])
                for schedule in new_schedules:
                    # Add next_run/last_run for each new schedule
                    if 'last_run' not in schedule:
                        schedule['last_run'] = None
                    schedule['next_run'] = self.calculate_next_run_date(schedule)

                existing_configs.append(new_config)

    def get_schedule_by_frequency_type(self, schedules: List[Dict[str, Any]], frequency_type: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a schedule by frequency type from a list of schedules.
        """
        for schedule in schedules:
            if schedule.get('frequency_type') == frequency_type:
                return schedule
        return None

    def is_schedule_in_list(self, schedule: Dict[str, Any], schedule_list: List[Dict[str, Any]]) -> bool:
        """
        Check if a schedule is already in the schedule list.
        """
        for existing_schedule in schedule_list:
            if existing_schedule == schedule:
                return True
        return False
    

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
    
    
    def process_csv(self, csv_file):
        """
        Process the CSV file to add, remove, or update configurations.
        """
        df = pd.read_csv(csv_file)
        df['URL'] = df['URL'].map(lambda x: x.split('?ranking_uuid')[0] if '?ranking_uuid' in x else x)
        # Iterate over each action
        actions = ['add', 'update', 'remove']
        for action in actions:
            action_df = df[(df['Action'].str.lower() == action) & (df['Done'] == False)]
            if action_df.empty:
                continue
            if action == 'remove':
                for index, row in action_df.iterrows():
                    ota = row['OTA'].strip()
                    url = row['URL'].strip()
                    url = url.split('?ranking_uuid')[0] if '?ranking_uuid' in url else url
                    self.remove_url(ota, url)
                    df.at[index, 'Done'] = True
            else:
                # For 'add' and 'update', we need to group by OTA, URL, Adults, Language
                grouped = action_df.groupby(['OTA', 'URL', 'Adults', 'Language'])
                for (ota, url, adults, language), group in grouped:
                    ota = ota.strip()
                    url = url.strip()
                    viewer = group['Viewer'].iloc[0].strip()
                    city = group['City'].iloc[0].strip()
                    adults = int(adults) if not pd.isna(adults) else None
                    language = language.strip() if not pd.isna(language) else None
                    # Aggregate schedules
                    schedules = []
                    for _, row in group.iterrows():
                        frequency_type = row['Frequency_Type'].strip().lower() if not pd.isna(row['Frequency_Type']) else None
                        schedule = {
                            'frequency_type': frequency_type,
                            'days_in_future': int(row['Days_In_Future']) if not pd.isna(row['Days_In_Future']) else None
                        }
                        if frequency_type in ['daily', 'every_other_day']:
                            schedule['interval'] = int(row['Interval']) if not pd.isna(row['Interval']) else 1
                        if frequency_type == 'weekly':
                            schedule['occurrences_per_week'] = int(row['Occurrences_Per_Week']) if not pd.isna(row['Occurrences_Per_Week']) else 1
                        if frequency_type == 'monthly':
                            schedule['occurrences_per_month'] = int(row['Occurrences_Per_Month']) if not pd.isna(row['Occurrences_Per_Month']) else 1
                        if frequency_type in ['twice_a_day', 'three_times_a_day']:
                            schedule['times_per_day'] = int(row['Times_Per_Day']) if not pd.isna(row['Times_Per_Day']) else 1
                        if frequency_type == 'every_other_week':
                            schedule['run_day'] = row['Run_Day'].strip().lower() if not pd.isna(row['Run_Day']) else 'monday'
                        schedule['extract_hours'] = bool(row['Extract_Hours']) if 'Extract_Hours' in row and not pd.isna(row['Extract_Hours']) else False
                        schedules.append(schedule)
                    # Prepare configuration
                    configuration = {
                        'adults': adults,
                        'language': language,
                        'schedules': schedules
                    }
                    configurations = [configuration]
                    # Call add_url or update_url
                    if action == 'add':
                        self.add_url(ota=ota, url=url, viewer=viewer, city=city, configurations=configurations)
                    elif action == 'update':
                        self.update_url(ota=ota, url=url, viewer=viewer, city=city, configurations=configurations)
                    # Mark all related rows as Done
                    indices = group.index
                    df.loc[indices, 'Done'] = True
        # Save the updated configuration
        self.save_config()

        # Save the updated CSV file with Done flags updated
        df.to_csv(csv_file, index=False)
        print(f"CSV file '{csv_file}' updated successfully.")
