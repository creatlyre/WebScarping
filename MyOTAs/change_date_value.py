import pandas as pd
import re

# Specify your file name
file_name = "Viator - 2025-03-01.xlsx"

output_viator = fr'G:/.shortcut-targets-by-id/1ER8hilqZ2TuX2C34R3SMAtd1Xbk94LE2/MyOTAs/Baza Excel/Viator/Daily/{file_name}'
# Use regex to extract the date from the file name
extracted_date = file_name.split('.')[0].split()[-1]


# Read all sheets from the Excel file into a dictionary of DataFrames
sheets_dict = pd.read_excel(output_viator, sheet_name=None)

# Iterate over each sheet and update the "Data zestawienia" column
for sheet_name, df in sheets_dict.items():
    if "Data zestawienia" in df.columns:
        df["Data zestawienia"] = extracted_date
    else:
        print(f"Warning: 'Data zestawienia' column not found in sheet '{sheet_name}'.")

# Save the updated sheets into a new Excel file
with pd.ExcelWriter(output_viator) as writer:
    for sheet_name, df in sheets_dict.items():
        df.to_excel(writer, sheet_name=sheet_name, index=False)

print(f"Processing complete. Updated file saved as '{output_viator}'.")
