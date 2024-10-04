import logging
import os
import datetime

class LoggerManager:
    def __init__(self, file_manager, application = "daily"):
        self.logs_path = file_manager.logs_path
        self.ensure_log_folder_exists()  # Ensure log folder exists

        # Create logger objects for error, info, and done logs
        self.logger_err = logging.getLogger(f'Error_logger')
        self.logger_err.setLevel(logging.DEBUG)

        self.logger_info = logging.getLogger(f'Info_logger')
        self.logger_info.setLevel(logging.DEBUG)

        self.logger_done = logging.getLogger(f'Done_logger')
        self.logger_done.setLevel(logging.DEBUG)

        # Create handlers
        self.ch = logging.StreamHandler()
        self.ch.setLevel(logging.DEBUG)

        # Dynamically create paths for each log type based on current year/month
        current_log_path = self.get_current_log_path()
        self.fh_error = logging.FileHandler(os.path.join(current_log_path, f'{application}_error_logs.log'))
        self.fh_error.setLevel(logging.DEBUG)

        self.fh_info = logging.FileHandler(os.path.join(current_log_path, f'{application}_info_logs.log'))
        self.fh_info.setLevel(logging.INFO)

        self.fh_done = logging.FileHandler(os.path.join(current_log_path, f'{application}_done_logs.log'))
        self.fh_done.setLevel(logging.INFO)

        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Add formatter to handlers
        self.ch.setFormatter(formatter)
        self.fh_error.setFormatter(formatter)
        self.fh_info.setFormatter(formatter)
        self.fh_done.setFormatter(formatter)

        # Add handlers to loggers
        self.logger_err.addHandler(self.ch)
        self.logger_err.addHandler(self.fh_error)

        self.logger_info.addHandler(self.ch)
        self.logger_info.addHandler(self.fh_info)

        self.logger_done.addHandler(self.ch)
        self.logger_done.addHandler(self.fh_done)

    def get_current_log_path(self):
        """Returns the path for the current year's and month's logs."""
        now = datetime.datetime.now()
        year = now.strftime('%Y')
        month = now.strftime('%m')
        log_folder = os.path.join(self.logs_path, year, month)

        if not os.path.exists(log_folder):
            os.makedirs(log_folder)

        return log_folder

    def ensure_log_folder_exists(self):
        """Ensures the main logs folder exists."""
        if not os.path.exists(self.logs_path):
            os.makedirs(self.logs_path)
# %