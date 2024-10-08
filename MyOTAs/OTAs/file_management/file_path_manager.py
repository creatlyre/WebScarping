import datetime

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
    }

class Connectors_Color:
    def __init__(self) -> None:
        self.USERNAME = "azureadmin"
        self.PASSWORD = "brudnyHarry!66"
        # Define color themes
        self.PRIMARY_BLUE = '#00AEEF'   # Medium to bright cyan blue
        self.DARK_BLUE = '#0073B1'      # Dark blue
        self.LIGHT_GREEN = '#DFF0D8'    # Light green for highlights
