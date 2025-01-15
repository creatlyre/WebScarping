import os
import sys

# Set the current directory to the script location
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the root directory (project directory) to sys.path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)

from file_management.file_path_manager_future_price import FilePathManagerFuturePrice
from file_management.config_manager_future_price import ConfigReader

SITES = ['GYG', 'Viator', 'Other']
def main():
    """
    Main function to execute the CSV processing.
    """
    # Define paths
    
    file_manager = FilePathManagerFuturePrice('Update Config Future Price', 'N/A', 'N/A', 'N/A')  

    # Initialize ConfigReader
    config_reader = ConfigReader(file_manager.config_file_path)
    for SITE in SITES:
        urls = config_reader.get_urls_by_ota(SITE)
        

if __name__ == "__main__":
    main()