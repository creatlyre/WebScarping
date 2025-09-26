# MyOTAs Scraper Project

## 1. Project Overview

This project is a collection of Python scripts designed to scrape data from various Online Travel Agencies (OTAs), including **Viator** and **GetYourGuide**. The primary goal is to gather information on daily listings, tour operators, and future pricing. The collected data is then processed, transformed, and uploaded to Azure Blob Storage for further analysis and use.

The project is structured to be modular, with a centralized `utils.py` module that contains shared configurations, API keys, and utility functions to promote code reuse and maintainability.

## 2. Core Functionalities

- **Daily Data Scraping:** Automatically collects daily tour and activity data from Viator.
- **Operator Information Retrieval:** Extracts detailed information about tour operators (suppliers) from Viator.
- **Future Pricing Analysis:** Gathers pricing data for future dates from both Viator and GetYourGuide.
- **Azure Integration:** Manages the lifecycle of an Azure Virtual Machine and uploads the scraped data to Azure Blob Storage.

## 3. Scripts Description

Below is a breakdown of the main Python scripts and their roles within the project:

- **`utils.py`**
  - A shared utility module that contains common components used across the other scripts. This includes:
    - API keys and configurations for external services.
    - A generic `ZenRowsScraper` class for handling web scraping requests.
    - Helper functions for logging, data transformation, and uploading files to Azure Blob Storage.

- **`Viator_daily.py`**
  - The main script for performing daily data scraping from Viator. It uses the `ZenRowsScraper` from `utils.py` to fetch data concurrently, processes it, and uploads the results to Azure.

- **`Viator_GetOperator.py`**
  - Responsible for scraping information about tour operators from Viator. It identifies and extracts supplier details for various listings.

- **`Viator_FuturePrice.py`**
  - Collects future pricing data from Viator. This script is used to analyze how prices for tours and activities change over time.

- **`_GYG_future_price.py`**
  - Similar to the Viator future price scraper, this script gathers future pricing information from GetYourGuide.

- **`Azure_stopVM.py`**
  - A utility script for managing the Azure Virtual Machine where the scrapers are likely executed. It includes functionality to stop the VM, helping to manage costs.

## 4. Setup and Configuration

To run these scripts, you need to configure the following in the `MyOTAs/utils.py` file:

- **API Keys:**
  - `API_KEY_ZENROWS`: Your API key for the ZenRows web scraping service.
  - `API_KEY_FIXER`: Your API key for the Fixer.io currency conversion service.

- **Azure Credentials:**
  - `STORAGE_ACCOUNT_NAME`: The name of your Azure Storage account.
  - `STORAGE_ACCOUNT_KEY`: The access key for your Azure Storage account.
  - The `Azure_stopVM.py` script also requires Azure service principal credentials (`clientId`, `clientSecret`, `subscriptionId`, `tenantId`).

- **File Paths:**
  - Ensure that the file paths defined in `utils.py` and the other scripts match your local environment setup.

## 5. How to Run the Scripts

Each script is designed to be executed directly from the command line.

- **To run the daily Viator scraper:**
  ```bash
  python MyOTAs/Viator_daily.py
  ```

- **To get operator information from Viator:**
  ```bash
  python MyOTAs/Viator_GetOperator.py
  ```

- **To get future prices from Viator or GetYourGuide:**
  ```bash
  python MyOTAs/Viator_FuturePrice.py
  python MyOTAs/_GYG_future_price.py
  ```