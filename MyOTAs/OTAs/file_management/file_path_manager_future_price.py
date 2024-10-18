import os
import sys

# Set the current directory to the script location
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the root directory (project directory) to sys.path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)

from file_management.file_path_manager import FilePathManager
import datetime

class FilePathManagerFuturePrice(FilePathManager):
        def __init__(self, site, city, adults, language, manual_overdrive_date=False, manual_date='2024-09-30'):
            super().__init__(site, city, manual_overdrive_date, manual_date)
            self.adults = adults
            self.language = language
            self.output = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/{self.site}/future_price'
            self.link_file_path = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Resource/LinksFuturePrice_GYG.json'
            self.extraction_date = datetime.datetime.now().strftime('%Y-%m-%d %H:00:00')
            self.extraction_date_save_format = f"{self.extraction_date.replace(' ', '_').replace(':','-')}_{self.language}_{self.adults}"
            # Set the path of the local file
            # Azure Storage containers and blob name
            self.container_name_raw = f"raw/future_price/{self.site}"
            self.container_name_refined = f"refined/future_price/{self.site}"
            self.output_file_path = f"{self.output}/{self.extraction_date_save_format}_future_price.xlsx" # AKA output_file_path
            self.blob_name = fr'{self.extraction_date_save_format}_future_price.xlsx'
#