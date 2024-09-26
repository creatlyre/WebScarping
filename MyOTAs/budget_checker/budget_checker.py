
import pandas as pd
import streamlit as st
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import datetime
import uuid
import json
from io import StringIO


config = json.load(open("config.json"))
# Azure Storage credentials (replace these with your actual credentials)
AZURE_STORAGE_CONNECTION_STRING = config['AZURE_STORAGE_CONNECTION_STRING']
CONTAINER_NAME = config['CONTAINER_NAME']
SAFUTUREPRICE_CONNECTION_STRING = config['SAFUTUREPRICE_CONNECTION_STRING']
SAFUTUREPRICE_CONTAINER_NAME = config['SAFUTUREPRICE_CONTAINER_NAME']
BLOB_NAME = 'LinksFuturePrice_GYG.csv'

def main():\
    # Custom CSS to match the color scheme of your webpage and improve button alignment
    st.markdown("""
    <style>
    .main {
        background-color: #083D77;  /* Dark Blue background */
    }
    .stTextInput label, .stNumberInput label, .stSelectbox label, .stMultiselect label {
        color: #EDEDEB;  /* Light text color */
        font-size: 1.2rem;  /* Increase font size */
    }
    .stTextInput div, .stNumberInput div, .stSelectbox div, .stMultiselect div {
        font-size: 1.1rem;  /* Increase input font size */
    }
    .stButton button {
        background-color: #FF6F61;  /* Button color */
        color: #FFFFFF;  /* Button text color */
        border-radius: 5px;
        width: 100%;
        padding: 10px 20px;  /* Increase padding for bigger button */
        font-size: 1.1rem;  /* Increase font size */
    }
    .stButton button:hover {
        background-color: #FF5733;  /* Button hover color */
        color: #FFFFFF;
    }
    .stTable {
        color: #EDEDEB;  /* Table text color */
        background-color: #2C3E50;  /* Table background color */
    }
    h1, h2, h3, h4, h5, h6 {
        color: #EDEDEB;  /* Header text color */
        font-size: 2rem;  /* Increase heading size */
    }
    .stSelectbox, .stMultiselect, .stNumberInput {
        width: 100%;  /* Ensure dropdowns and inputs are full-width */
        padding: 10px;  /* Increase padding for larger inputs */
        font-size: 1.1rem;  /* Increase input font size */
    }
    .stTextInput input, .stNumberInput input, .stSelectbox select, .stMultiselect select {
        font-size: 1.1rem;  /* Ensure input values are larger */
    }
    .remove-btn {
        display: flex;
        justify-content: center;
        align-items: center;
        height: 100%;
    }
    .css-1y0tads {
        padding-top: 20px; /* Adjust top padding */
        padding-bottom: 20px; /* Adjust bottom padding */
    }
    </style>
    """, unsafe_allow_html=True)

    # Constants
    time_per_request = 6  # seconds
    machine_price_per_sec = 0.001851851851851852  # EUR per second
    # Define the mapping of refresh frequency options to their numerical values
    refresh_frequency_options = {
        'Daily': 30,
        'Every Other Day': 15,
        'Three Times a Week': 12,
        'Twice a Week': 8,
        'Weekly': 4,
        'Three Times a Month': 3,
        'Twice a Month': 2,
        'Monthly': 1,
        'Custom': None
    }
    # Function to calculate the cost for given parameters
    def calculate_cost(days_future, lang_count, adults_count, refresh_frequency_in_month, time_per_request, machine_price_per_sec):
        request = days_future * lang_count * adults_count * refresh_frequency_in_month
        time_single_run = request * time_per_request
        cost = time_single_run * machine_price_per_sec
        return cost

    # Initialize session state to keep track of activities and positions
    if 'activities' not in st.session_state:
        st.session_state.activities = {
            'Activity 1': {
                'name': 'Activity 1',
                'positions': [{'days_future': 1, 'lang_count': 1, 'adults_count': 1, 'refresh_frequency': 'Daily', 'refresh_frequency_num': 30, 'cost': 0}]
            }
        }
    if 'page' not in st.session_state:
        st.session_state.page = 'main'
    if 'main_df' not in st.session_state:
        st.session_state.main_df = pd.DataFrame()


    # Function to add a new activity
    def add_new_activity():
        new_activity_index = len(st.session_state.activities) + 1
        new_activity_key = f'Activity {new_activity_index}'
        st.session_state.activities[new_activity_key] = {
            'name': new_activity_key,
            'positions': [{'days_future': 1, 'lang_count': 1, 'adults_count': 1, 'refresh_frequency': 'Daily','refresh_frequency_num': 30, 'cost': 0}]
        }

    # Sidebar for adding new activity and displaying activity names and costs
    st.sidebar.title('Add New Activity')
    if st.sidebar.button('Add Activity to Monitor'):
        add_new_activity()

    # Function to display and manage positions for a given activity
    def manage_activity_positions(activity_key, data):
        st.markdown(f"## {data['name']}")

        # Option to rename the activity
        new_name = st.text_input(f'Rename {data["name"]}', value=data['name'], key=f'rename_{activity_key}')
        if new_name:
            data['name'] = new_name

        if st.button(f'Add Position to {data["name"]}'):
            data['positions'].append({'days_future': 1, 'lang_count': 1, 'adults_count': 1, 'refresh_frequency': 'Daily','refresh_frequency_num': 30, 'cost': 0})

        total_cost = 0
        total_days_covered = 0
        table_data = []
        positions_to_remove = []

        for i, position in enumerate(data['positions']):
            with st.expander(f'Position {i + 1}', expanded=True):
                cols = st.columns([1, 2, 2, 2, 1])

                with cols[0]:
                    days_future = st.number_input('Days', min_value=1, max_value=365, value=position['days_future'], key=f'{activity_key}_days_future_{i}', help="This represents the number of days in the future for which predictions or computations are being made.")
                with cols[1]:
                    # Multi-select dropdown for Language Count
                    languages_options = ['English', 'Spanish', 'Portuguese', 'French', 'Polish', 'German', 'Italian', 'Dutch', 'Greek', 'Czech']
                    languages_selected = st.multiselect('Select Languages', languages_options, key=f'{activity_key}_lang_count_{i}', help="Select the languages.")

                    # Calculate the number of languages based on the selected options
                    lang_count = len(languages_selected)
                    # Create a string of the selected languages
                    languages_selected_str = ', '.join(languages_selected)
                    # lang_count = st.number_input('Language Count', min_value=1, max_value=10, value=position['lang_count'], key=f'{activity_key}_lang_count_{i}', help="This stands for the number of languages the computations or requests will cover.")
                with cols[2]:
                    # Replace the number input for 'Adults Count' with a multi-select dropdown
                    adults_options = ['1 Adult', '2 Adults', '3 Adults', '4 Adults', '5 Adults', '6 Adults', '7 Adults', '8 Adults', '9 Adults', '10 Adults']
                    adults_selection = st.multiselect('Select Adults', adults_options, key=f'{activity_key}_adults_count_{i}', help="Select the number of adults for which you would like to collect data.")
                    # Calculate the total number of adults based on the selection
                    adults_count = len(adults_selection)
                    # Create a string of the selected values
                    adults_selected = ', '.join(adults_selection)
                    # adults_count = st.number_input('Adults Count', min_value=1, max_value=10, value=position['adults_count'], key=f'{activity_key}_adults_count_{i}', help="This represents the count of adults (1 = one variation of adults eq. 2,3,4 etc) for whom the requests are being made.")
                with cols[3]:
                    refresh_frequency = st.selectbox('Frequency', list(refresh_frequency_options.keys()), index=list(refresh_frequency_options.keys()).index(position['refresh_frequency']), key=f'{activity_key}_refresh_frequency_{i}', help='This indicates how often the requests need to be refreshed per month. ')
                    if refresh_frequency == 'Custom':
                        custom_refresh_frequency = st.number_input('Enter Custom Refresh Frequency', min_value=1, max_value=90, value=1, key=f'{activity_key}_custom_refresh_frequency_{i}', help='Enter a custom refresh frequency per month.')
                        refresh_frequency_in_month = custom_refresh_frequency
                    else:
                        refresh_frequency_in_month = refresh_frequency_options[refresh_frequency]
                with cols[4]:
                    remove_button = st.button('X', key=f'{activity_key}_remove_{i}', help='Remove this position')
                    if remove_button:
                        positions_to_remove.append(i)

                # Calculate additional days to be considered
                additional_days = max(0, days_future - total_days_covered)
                total_days_covered = max(total_days_covered, days_future)

                cost = calculate_cost(additional_days, lang_count, adults_count, refresh_frequency_in_month, time_per_request, machine_price_per_sec)
                st.session_state.activities[activity_key]['positions'][i]['cost'] = cost
                total_cost += cost

                table_data.append({
                    'Activity Name': data['name'],
                    'Days in Future': days_future,
                    'Language Count': lang_count,
                    'Languages Selected': languages_selected_str,
                    'Adults Count': adults_count,
                    'Adults Selected': adults_selected,  
                    'Refresh Frequency per Month': refresh_frequency,
                    'refresh_frequency_num': refresh_frequency_in_month,
                    'Cost (EUR)': cost
                })

        # Remove positions marked for removal
        if positions_to_remove:
            for idx in sorted(positions_to_remove, reverse=True):
                del data['positions'][idx]

        # Return the table data
        return table_data
    # Function to upload the summary DataFrame to Azure Blob Storage
    def upload_to_azure_blob(df, blob_name):
        try:
            blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
            blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)

            csv_data = df.to_csv(index=False)
            blob_client.upload_blob(csv_data, overwrite=True)
            st.success("File successfully uploaded!")
        except Exception as e:
            st.error(f"Failed to upload file to Azure Blob Storage: {e}")
    # Function to download CSV file from Azure Blob Storage

    def download_csv_from_blob(container_name, blob_name, connection_string):
         # Create the BlobServiceClient object
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Get the BlobClient
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        
        # Download the blob to a pandas DataFrame
        csv_data = blob_client.download_blob().readall().decode('utf-8')
        df = pd.read_csv(StringIO(csv_data))
        
        return df
# Function to update session state from configuration DataFrame
    def update_session_state_from_configuration(df_configuration):
        activities = {}
        for index, row in df_configuration.iterrows():
            activity_name = row['Activity Name']
            if activity_name not in activities:
                activities[activity_name] = {
                    'name': activity_name,
                    'positions': []
                }
            activities[activity_name]['positions'].append({
                'days_future': row['Days in Future'],
                'lang_count': row['Language Count'],
                'adults_count': row['Adults Count'],
                'refresh_frequency': row['Refresh Frequency per Month'],
                'refresh_frequency_num': row['refresh_frequency_num'],
                'cost': row['Cost (EUR)_x']
            })
        
        st.session_state.activities = activities

    # Define the dialog function
    @st.experimental_dialog("UID")
    def company_configuration():
        st.write('Please provide your unique identifier to load your configuration settings')
        uid = st.text_input("Unique Identifier")

        if st.button("OK"):
            if uid:
                st.session_state.formSubmit = {
                            'uidText': uid
                        }
                st.session_state.show_popup = False
                st.experimental_rerun()
            else:
                st.warning("Please fill in all fields.")

        if st.button("Cancel"):
            st.session_state.show_popup = False
            st.experimental_rerun()
    # Function for login page
    def login():
        st.title("Login")
        uid = st.text_input("Enter your UID")
        if st.button("Login"):
            if uid:
                st.session_state.uid = uid
                st.session_state.logged_in = True
                st.experimental_rerun()
            else:
                st.warning("Please enter a UID to login.")
    #Main Screen
    if st.session_state.page == 'main':
        
        # Initialize session state variables
        if 'show_popup' not in st.session_state:
            st.session_state.show_popup = False

        if 'formSubmit' not in st.session_state:
            st.session_state.formSubmit = None
        
        if 'config_loaded' not in st.session_state:
            st.session_state.config_loaded = False
        
        total_all_activities_cost = 0
        all_table_data = []
        for activity_key, data in st.session_state.activities.items():
            table_data = manage_activity_positions(activity_key, data)
            all_table_data.extend(table_data)
            total_all_activities_cost += sum(pos['cost'] for pos in data['positions'])

        # Display the table of positions
        if all_table_data:
            st.markdown("### Positions Table")
            temp_df = pd.DataFrame(all_table_data)
            df_filtered = temp_df.drop(columns=['Activity Name', 'refresh_frequency_num', 'Language Count', 'Adults Count'])
            st.table(df_filtered)

        # Sidebar: Display activity names and costs
        st.sidebar.markdown("### Activities and Costs")
        for activity_key, activity_data in st.session_state.activities.items():
            st.sidebar.write(f"{activity_data['name']}: {sum(pos['cost'] for pos in activity_data['positions']):.4f} EUR")

        # Display total cost for all activities
        st.sidebar.markdown("### Total Cost for All Activities")
        st.sidebar.write(f'{total_all_activities_cost:.4f} EUR')
        
         # Button to load configuration
        if st.button("Load My Configuration"):
            st.session_state.show_popup = True
            company_configuration()

        if st.session_state.formSubmit and not st.session_state.config_loaded:
            df_configuration = download_csv_from_blob(SAFUTUREPRICE_CONTAINER_NAME, BLOB_NAME, SAFUTUREPRICE_CONNECTION_STRING)
            uid = st.session_state['formSubmit']['uidText']
            st.table(df_configuration[df_configuration['uid'] == uid])
            if df_configuration is not None:
                # st.markdown("### Configuration Table")
                df_configuration = df_configuration[df_configuration['uid'] == uid]
                # st.table(df_configuration)
                # Update the session state with the new configuration data
                update_session_state_from_configuration(df_configuration)
                st.session_state.config_loaded = True  # Set the flag to avoid re-loading
                st.experimental_rerun()
            else:
                st.error("Failed to load data from blob storage.")

        if st.sidebar.button('Save and Continue'):
            st.session_state.main_df = pd.concat([st.session_state.main_df, temp_df]).drop_duplicates().reset_index(drop=True)
            st.session_state.page = 'summary'


        
    # Summary screen 
    elif st.session_state.page == 'summary':

        st.title('Summary of Activities')
        # Initialize session state variables
        if 'show_popup' not in st.session_state:
            st.session_state.show_popup = False

        if 'upload_status' not in st.session_state:
            st.session_state.upload_status = None

        if 'formSubmit' not in st.session_state:
            st.session_state.formSubmit = None

        summary_data = []
        all_urls_populated = True
        for activity_key, data in st.session_state.activities.items():
            url = st.text_input(f'URL for {data["name"]}', key=f'url_{activity_key}')
            activity_total_cost = sum(position['cost'] for position in data['positions'])
            summary_data.append({
                'Activity Name': data['name'],
                'Cost (EUR)': activity_total_cost,
                'URL': url
            })
            if not url:
                all_urls_populated = False
        df_summary = pd.DataFrame(summary_data)
        st.table(df_summary)
        # Display the main_df DataFrame from the main page
        st.markdown("### Detailed Positions Table")

        total_cost = sum(item['Cost (EUR)'] for item in summary_data)
        st.write(f'Total Cost: {total_cost:.4f} EUR')

        if st.sidebar.button('Back'):
            st.session_state.page = 'main'

        if st.button('Send to Verification'):
            if all_urls_populated:
                st.session_state.show_popup = True
            else:
                st.warning("Please fill in all URLs before sending to verification.")

        # Define the dialog function
        @st.experimental_dialog("Confirmation")
        def confirm_submission():
            st.write('Please confirm the details before sending:')
            confirmation_text = st.text_input("Provide company name")
            email_text = st.text_input("Provide your email")

            if st.button("OK"):
                if confirmation_text and email_text:
                    st.session_state.formSubmit = {
                        'confirmationText': confirmation_text,
                        'emailText': email_text
                    }
                    st.session_state.show_popup = False
                    st.experimental_rerun()
                    
                else:
                    st.warning("Please fill in all fields.")

            if st.button("Cancel"):
                st.session_state.show_popup = False
                st.experimental_rerun()

        # Trigger the dialog if the popup flag is set
        if st.session_state.show_popup:
            confirm_submission()

        # Process form submission
        if st.session_state.formSubmit:
            unique_filename = f"{datetime.date.today().strftime('%Y-%m-%d')}_{st.session_state['formSubmit']['confirmationText']}_future_price_request_{uuid.uuid4()}.csv"
            df_summary['Viewer'] = st.session_state['formSubmit']['confirmationText']
            df_summary['Email'] = st.session_state['formSubmit']['emailText']
            df_merged = pd.merge(st.session_state.main_df, df_summary, on='Activity Name', how='left')
            st.write("Request has been sent!")
            # Replace this with your upload function
            upload_to_azure_blob(df_merged, unique_filename)
            st.session_state.page = "main"
            del st.session_state.formSubmit
            
    return ""

if __name__ == "__main__":
    main()