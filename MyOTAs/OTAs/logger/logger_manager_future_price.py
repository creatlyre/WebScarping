
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

        current_log_path = self.get_current_log_path()
        self.logger_statistics = logging.getLogger('Statistics_logger')
        self.logger_statistics.setLevel(logging.DEBUG)
        self.fh_statistics = logging.FileHandler(os.path.join(current_log_path, f'{application}_statistics_logs.log'))
        self.fh_statistics.setLevel(logging.INFO)

        self.logger_statistics.addHandler(self.ch)
        self.logger_statistics.addHandler(self.fh_statistics)
        self.fh_statistics.setFormatter(formatter)

    def close_logger(self):
        super().close_logger()
        for handler in self.logger_statistics.handlers[:]:
            handler.close()
            self.logger_statistics.removeHandler(handler)
        
    
    
# %%