import datetime
import os

class FilePathManager:
    def __init__(self, site, city, manual_overdrive_date=False, manual_date='2024-09-30'):
        self.site = site
        self.city = city
        self.date_today = datetime.date.today().strftime("%Y-%m-%d")
        if manual_overdrive_date:
            self.date_today = manual_date # For fixed date testing

        # Define the file paths
        self.output = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/{self.site}/Daily'
        self.archive_folder = fr'{self.output}/Archive'
        self.file_path_done = fr'{self.output}/{self.date_today}-DONE-{self.site}.csv'
        self.file_path_done_city = fr'{self.output}/{self.date_today}-{self.city}-{self.site}.csv'
        self.file_path_output = fr"{self.output}/{self.site} - {self.date_today}.xlsx"
        self.link_file = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Resource/{self.site}_links.csv'
        self.logs_path = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Logs/{self.site}'
        self.storage_account_name = "storagemyotas"
        self.storage_account_key = "vyHHUXSN761ELqivtl/U3F61lUY27jGrLIKOyAplmE0krUzwaJuFVomDXsIc51ZkFWMjtxZ8wJiN+AStbsJHjA=="
        # Local file path
        self.local_file_path = f"{self.output}/{self.site} - {self.date_today}.xlsx"
        self.file_path_csv_operator = fr"G:\.shortcut-targets-by-id\1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2\MyOTAs\Pliki firmowe\Operators_{self.site}.csv"
        self.file_path_xlsx_operator = fr"G:\.shortcut-targets-by-id\1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2\MyOTAs\Pliki firmowe\Operators_{self.site}.xlsx"
        # Azure Storage containers and blob name
        self.container_name_raw = f"raw/daily/{self.site}"
        self.container_name_refined = f"refined/daily/{self.site}"
        self.blob_name = fr'{self.site} - {self.date_today}.xlsx'
        self.config_path = 'resources/config.json'
        self.alerts_csv_file_path = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Resource/urls_price_alerts.csv'

        # Logs processed path
        self.file_path_logs_processed = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Logs/files_processed/{self.blob_name.split(".")[0]}'

    def get_file_paths(self):
        return {
        'output': self.output,
        'archive_folder': self.archive_folder,
        'file_path_done': self.file_path_done,
        'file_path_done_city': self.file_path_done_city,
        'file_path_output': self.file_path_output,
        'link_file': self.link_file,
        'logs_path': self.logs_path,
        'local_file_path': self.local_file_path,
        'container_name_raw': self.container_name_raw,
        'container_name_refined': self.container_name_refined,
        'blob_name': self.blob_name,
        'file_path_logs_processed': self.file_path_logs_processed,
        "storage_account_name": self.storage_account_name,
        "storage_account_key": self.storage_account_key,
        "date_today": self.date_today,
        'file_path_csv_operator': self.file_path_csv_operator,
        'file_path_xlsx_operator': self.file_path_xlsx_operator,
        'config_path': self.config_path
    }

class ConnectorsSQL_OTA:
    def __init__(self) -> None:
        self.USERNAME = "azureadmin"
        self.PASSWORD = "brudnyHarry!66"
        self.API_KEY_SCRAPERAPI = '8c36bc42cd11c738c1baad3e2000b40c' # https://dashboard.scraperapi.com/
        self.API_KEY_ZENROWS = '56ed5b7f827aa5c258b3f6d3f57d36999aa949e8' # https://app.zenrows.com/buildera
        self.API_KEY_BOTOSLAW1_RATES = 'acfed48df1159d37fa4305e5e95c234f'
        self.API_KEY_BOTOSLAW2_RATES = '49b0ef06a9d57046eac0a36aafdd76e7'

class DetermineDebugRun:
    def __init__(self, check_for_debug=True):
        self.local_path = os.getcwd()
        # Initialize a debug attribute based on the condition
        self.debug = "wojciech" in self.local_path.lower() and check_for_debug

class AzureConfigs_OTA:
    def __init__(self):
        # Azure Config
        self.AZURE_TENANT_ID = "39ffbbb3-2e77-41c7-94df-5b52eef42062"
        self.AZURE_CLIENT_ID = "7ef340dd-3d92-4e3f-9b4c-9d62889f4989"
        self.AZURE_CLIENT_SECRET = "R3n8Q~sUUU190SpcqSEisohpi_-aTfB7Yi1pQdd2"
        self.AZURE_GROUP_ID = "1c766061-77ce-42d6-aca4-30612864e5f7"
