import logging
import os
import datetime

class LoggerManager:
    def __init__(self, file_manager, application = "daily"):
        self.logs_path = file_manager.logs_path
        self.ensure_log_folder_exists()  # Ensure log folder exists

         # Dynamically create paths for each log type based on current year/month
        current_log_path = self.get_current_log_path()
        
        # Define log files for each logger
        self.error_log_file = os.path.join(current_log_path, f'{application}_error_logs.log')
        self.info_log_file = os.path.join(current_log_path, f'{application}_info_logs.log')
        self.done_log_file = os.path.join(current_log_path, f'{application}_done_logs.log')

        # Set up loggers
        self.logger_err = self.get_or_create_logger('Error_logger', self.error_log_file, level=logging.DEBUG)
        self.logger_info = self.get_or_create_logger('Info_logger', self.info_log_file, level=logging.DEBUG)
        self.logger_done = self.get_or_create_logger('Done_logger', self.done_log_file, level=logging.INFO)

    def get_or_create_logger(self, logger_name, log_file, level=logging.INFO):
        """Creates or retrieves a logger, ensuring no duplicate handlers."""
        logger = logging.getLogger(logger_name)
        if not logger.hasHandlers():  # Check if the logger already has handlers
            logger.setLevel(level)

            # File handler
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(level)

            # Stream handler (console output)
            stream_handler = logging.StreamHandler()
            stream_handler.setLevel(level)

            # Formatter
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)
            stream_handler.setFormatter(formatter)

            # Add handlers to logger
            logger.addHandler(file_handler)
            logger.addHandler(stream_handler)

            # Optional: Disable propagation to prevent duplicates in root logger
            logger.propagate = False
        
        return logger

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

    def close_logger(self):
        """Closes all handlers for each logger to release resources."""
        for logger in [self.logger_err, self.logger_info, self.logger_done]:
            for handler in logger.handlers[:]:
                handler.close()
                logger.removeHandler(handler)
        print("All loggers closed successfully.")