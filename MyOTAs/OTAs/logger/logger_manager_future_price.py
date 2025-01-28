
import os
import sys

# Set the current directory to the script location
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the root directory (project directory) to sys.path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)
import logging
from logger.logger_manager import LoggerManager

class LoggerManagerFuturePrice(LoggerManager):
    def __init__(self, file_manager, application="future_price"):
        super().__init__(file_manager, application)
        
        # Use the formatter from the base class
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Set up the log path
        current_log_path = self.get_current_log_path()

        # Create and configure the 'Statistics_logger'
        self.logger_statistics = logging.getLogger('Statistics_logger')
        self.logger_statistics.setLevel(logging.DEBUG)

        # Create a stream handler for the logger
        self.ch = logging.StreamHandler()  # Add this line to define the missing attribute
        self.ch.setLevel(logging.DEBUG)
        self.ch.setFormatter(formatter)

        # Create a file handler for the logger
        self.fh_statistics = logging.FileHandler(os.path.join(current_log_path, f'{application}_statistics_logs.log'))
        self.fh_statistics.setLevel(logging.INFO)
        self.fh_statistics.setFormatter(formatter)

        # Add handlers to the logger
        self.logger_statistics.addHandler(self.ch)
        self.logger_statistics.addHandler(self.fh_statistics)

        
    
    
# %%