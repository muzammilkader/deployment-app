import streamlit as st
import requests
import json
import os
import base64
import time

# --- Constants ---
# We'll create these directories if they don't exist
OUTPUT_DIR_CODES = 'dataset_codes'
OUTPUT_FILENAME_CODES = os.path.join(OUTPUT_DIR_CODES, 'dataset_codes.json')
OUTPUT_DIR_DATA = 'input_files'

# --- 1. Authentication Helper ---
# This function is refactored to take credentials as arguments
# instead of reading from a file.
def authenticate(base_url, username, password, client_name):
    """Authenticates to the API and returns a token."""
    auth_url = f"https://{base_url}/auth/login"
    auth_payload = json.dumps({
        "username": username,
        "password": password,
        "clientName": client_name
    })
    auth_headers = {'Content-Type': 'application/json'}
    
    try:
        auth_response = requests.post(auth_url, headers=auth_headers, data=auth_payload, timeout=10)
        auth_response.raise_for_status()  # Raise HTTPError for bad responses
        
        token = auth_response.text.strip()
        if token:
            return token
        else:
            raise ValueError("Token not found in response.")
    except requests.exceptions.HTTPError as err:
        st.error(f"Authentication failed: {err.response.status_code} - {err.response.text}")
        return None
    except Exception as e:
        st.error(f"Error during authentication: {e}")
        return None

# --- 2. Refactored Script Functions ---
# Each of your scripts has been converted into a function.
# All 'print' statements are replaced with 'st.write' to show output in the UI.

def pull_dataset_codes():
    """
    Refactored from '1 - Pull Dataset Codes List.py'.
    Fetches codes from SOURCE environment.
    """
    with st.status("Running: 1. Pull Dataset Codes...", expanded=True) as status:
        try:
            # Load secrets for SOURCE
            st.write("Loading source environment configuration...")
            base_url = st.secrets["source"]["url"]
            username = st.secrets["source"]["username"]
            password = st.secrets["source"]["password"]
            client_name = st.secrets["source"]["clientName"]
            
            # Step 1: Authenticate
            st.write(f"Authenticating to source: {base_url}...")
            token = authenticate(base_url, username, password, client_name)
            if not token:
                status.update(label="Authentication failed.", state="error")
                return

            st.write("Authentication successful.")
            
            # Step 2: Fetch Data
            os.makedirs(OUTPUT_DIR_CODES, exist_ok=True)
            fetch_url = f"https://{base_url}/dataset/list"
            headers = {
                'Content-Type': 'application/json',
                'X-KSYS-TOKEN': token
            }
            
            st.write("Fetching dataset list from API...")
            response = requests.post(fetch_url, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            st.write("Processing response...")
            
            if 'value' in data and 'values' in data['value']:
                codes = [entry['code'] for entry in data['value']['values']]
                with open(OUTPUT_FILENAME_CODES, 'w') as json_file:
                    json.dump(codes, json_file, indent=4)
                
                st.write(f"Found {len(codes)} codes.")
                status.update(label="Step 1: Pull Codes successful!", state="complete")
                st.success(f"Success! {len(codes)} codes saved to {OUTPUT_FILENAME_CODES}")
                st.session_state.step1_complete = True
            else:
                st.error("Expected 'value' or 'values' not found in response.")
                st.json(data)
                status.update(label="Step 1: Failed", state="error")

        except Exception as e:
            st.error(f"An error occurred: {e}")
            status.update(label="Step 1: Failed", state="error")

def get_dataset_data():
    """
    Refactored from '3 - Get Data.py'.
    Uses pulled codes to get data from SOURCE.
    """
    if not os.path.exists(OUTPUT_FILENAME_CODES):
        st.error("Error: Dataset codes file not found. Please run Step 1 first.")
        return
        
    with st.status("Running: 2. Get Dataset Data...", expanded=True) as status:
        try:
            # Load codes
            with open(OUTPUT_FILENAME_CODES, 'r') as file:
                dataset_codes = json.load(file)
            st.write(f"Found {len(dataset_codes)} codes to fetch.")
            
            # Load secrets for SOURCE
            st.write("Loading source environment configuration...")
            base_url = st.secrets["source"]["url"]
            username = st.secrets["source"]["username"]
            password = st.secrets["source"]["password"]
            client_name = st.secrets["source"]["clientName"]

            # Step 1: Authenticate
            st.write(f"Authenticating to source: {base_url}...")
            token = authenticate(base_url, username, password, client_name)
            if not token:
                status.update(label="Authentication failed.", state="error")
                return
            st.write("Authentication successful.")
            
            # Step 2: Fetch Data for each code
            os.makedirs(OUTPUT_DIR_DATA, exist_ok=True)
            fetch_url_base = f"https://{base_url}/dataset/get/"
            headers = {'Content-Type': 'application/json', 'X-KSYS-TOKEN': token}
            
            total_codes = len(dataset_codes)
            progress_bar = st.progress(0, text="Fetching data...")
            
            for i, dataset_code in enumerate(dataset_codes):
                st.write(f"Fetching: {dataset_code}...")
                response = requests.get(fetch_url_base + dataset_code, headers=headers)
                
                if response.status_code == 200:
                    data = response.json()
                    if 'value' in data:
                        value = data['value']
                        result = {
                            "code": value.get("code"),
                            "bodyMeta": value.get("bodyMeta"),
                            "body": value.get("body"),
                            "inputs": value.get("inputs")
                        }
                        filename = os.path.join(OUTPUT_DIR_DATA, f"{dataset_code.replace(' ', '_')}.json")
                        with open(filename, 'w') as json_file:
                            json.dump(result, json_file, indent=4)
                    else:
                        st.warning(f"'value' not found in response for {dataset_code}.")
                else:
                    st.warning(f"Failed to fetch {dataset_code}: {response.status_code} - {response.text}")
                
                # Update progress
                progress_bar.progress((i + 1) / total_codes, text=f"Fetching data... ({i+1}/{total_codes})")
                
            status.update(label="Step 2: Get Data successful!", state="complete")
            st.success(f"Success! Data for {total_codes} datasets saved to '{OUTPUT_DIR_DATA}' folder.")
            st.session_state.step2_complete = True

        except Exception as e:
            st.error(f"An error occurred: {e}")
            status.update(label="Step 2: Failed", state="error")


def run_search_replace(replacements):
    """
    Refactored from '4 - Snowflake - Search and Replace.py'.
    Performs find/replace on files in 'input_files'.
    """
    if not os.path.exists(OUTPUT_DIR_DATA):
        st.error("Error: 'input_files' directory not found. Please run Step 2 first.")
        return

    with st.status("Running: Optional - Search & Replace...", expanded=True) as status:
        try:
            st.write(f"Applying {len(replacements)} replacement rules...")
            
            # Function to replace text in any string field
            def replace_text_in_string(item, replacements):
                if isinstance(item, str):
                    for old_value, new_value in replacements.items():
                        item = item.replace(old_value, new_value)
                return item

            # Function to recursively apply replacements
            def apply_replacements(item, replacements):
                if isinstance(item, dict):
                    return {key: apply_replacements(value, replacements) for key, value in item.items()}
                elif isinstance(item, list):
                    return [apply_replacements(value, replacements) for value in item]
                elif isinstance(item, str):
                    return replace_text_in_string(item, replacements)
                return item

            file_count = 0
            for filename in os.listdir(OUTPUT_DIR_DATA):
                if filename.endswith(".json"):
                    file_path = os.path.join(OUTPUT_DIR_DATA, filename)
                    
                    with open(file_path, 'r') as file:
                        data = json.load(file)
                    
                    # Apply replacements
                    data = apply_replacements(data, replacements)
                    
                    # Save the modified data back
                    with open(file_path, 'w') as file:
                        json.dump(data, file, indent=4)
                    file_count += 1
            
            st.write(f"Replacements completed for {file_count} files.")
            status.update(label="Search & Replace complete!", state="complete")
            st.success(f"Success! Replacements applied to {file_count} files.")

        except Exception as e:
            st.error(f"An error occurred: {e}")
            status.update(label="Search & Replace failed", state="error")

def run_encoding():
    """
    Refactored from '50 - Manually Encode.py'.
    Base64-encodes 'body' and 'bodyMeta' fields.
    """
    if not os.path.exists(OUTPUT_DIR_DATA):
        st.error("Error: 'input_files' directory not found. Please run Step 2 first.")
        return
        
    with st.status("Running: Optional - Base64 Encoding...", expanded=True) as status:
        try:
            st.write("Applying Base64 encoding...")

            # Function to encode a JSON object to base64
            def encode_json(data):
                json_str = json.dumps(data, separators=(',', ':'))
                return base64.b64encode(json_str.encode('utf-8')).decode('utf-8')

            file_count = 0
            for filename in os.listdir(OUTPUT_DIR_DATA):
                if filename.endswith(".json"):
                    file_path = os.path.join(OUTPUT_DIR_DATA, filename)
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)

                    # Handle body
                    if 'body' in data:
                        body_json = data['body']
                        if isinstance(data['body'], str):
                            try:
                                body_json = json.loads(data['body'])
                            except json.JSONDecodeError:
                                st.warning(f"Could not parse 'body' in {filename}, encoding as-is.")
                        data['body'] = encode_json(body_json)

                    # Handle bodyMeta
                    if 'bodyMeta' in data and isinstance(data['bodyMeta'], dict):
                        data['bodyMeta'] = encode_json(data['bodyMeta'])

                    # Write back to file
                    with open(file_path, 'w', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False, indent=4)
                    file_count += 1

            st.write(f"Encoding completed for {file_count} files.")
            status.update(label="Encoding complete!", state="complete")
            st.success(f"Success! Encoding applied to {file_count} files.")

        except Exception as e:
            st.error(f"An error occurred: {e}")
            status.update(label="Encoding failed", state="error")

def run_upsert():
    """
    Refactored from '6 - Dataset Upserts.py'.
    Upserts files from 'input_files' to DESTINATION.
    """
    if not os.path.exists(OUTPUT_DIR_DATA):
        st.error("Error: 'input_files' directory not found. Please run Step 2 first.")
        return
        
    with st.status("Running: 3. Upsert Datasets...", expanded=True) as status:
        try:
            # Load secrets for DESTINATION
            st.write("Loading destination environment configuration...")
            base_url = st.secrets["destination"]["url"]
            username = st.secrets["destination"]["username"]
            password = st.secrets["destination"]["password"]
            client_name = st.secrets["destination"]["clientName"]
            
            # Step 1: Authenticate
            st.write(f"Authenticating to destination: {base_url}...")
            token = authenticate(base_url, username, password, client_name)
            if not token:
                status.update(label="Authentication failed.", state="error")
                return
            st.write("Authentication successful.")

            # Step 2: Iterate and upsert
            files_to_upsert = [f for f in os.listdir(OUTPUT_DIR_DATA) if f.endswith(".json")]
            total_files = len(files_to_upsert)
            st.write(f"Found {total_files} JSON files to upsert...")
            
            url = f"https://{base_url}/dataset/upsert"
            headers = {'Content-Type': 'application/json', 'X-KSYS-TOKEN': token}
            
            progress_bar = st.progress(0, text="Upserting datasets...")
            success_count = 0
            fail_count = 0
            
            for i, filename in enumerate(files_to_upsert):
                file_path = os.path.join(OUTPUT_DIR_DATA, filename)
                with open(file_path, 'r') as file:
                    data = json.load(file)
                
                payload = json.dumps(data)
                
                # Send the upsert request
                response = requests.post(url, headers=headers, data=payload)
                
                if response.status_code == 200:
                    st.write(f"Success: {filename}")
                    success_count += 1
                else:
                    st.warning(f"Failed: {filename} - {response.status_code} - {response.text}")
                    fail_count += 1
                
                # Update progress
                progress_bar.progress((i + 1) / total_files, text=f"Upserting... ({i+1}/{total_files})")
                
            st.write("--- Upsert Operation Complete ---")
            st.success(f"Successfully upserted: {success_count}")
            st.error(f"Failed to upsert: {fail_count}")
            status.update(label="Step 3: Upsert complete!", state="complete")

        except Exception as e:
            st.error(f"An error occurred: {e}")
            status.update(label="Step 3: Failed", state="error")


# --- 3. Streamlit UI ---
st.set_page_config(page_title="Deployment Tools", layout="wide")

# Initialize session state
if 'step1_complete' not in st.session_state:
    st.session_state.step1_complete = False
if 'step2_complete' not in st.session_state:
    st.session_state.step2_complete = False

# --- Sidebar Navigation ---
st.sidebar.title("Deployment Workflows")
page = st.sidebar.radio("Select a workflow:", 
    ("Dataset Deployment", "App Deployment (Coming Soon)"))

if page == "Dataset Deployment":
    st.title("🚚 Dataset Deployment Workflow")
    st.info("Run these steps in order. This tool will pull data from the **Source** environment and upsert it to the **Destination** environment.")

    # --- Workflow Steps ---
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.header("Step 1: Pull Codes")
        st.write("Fetches the list of dataset codes from the **Source** environment.")
        if st.button("▶️ Run Step 1: Pull Dataset Codes"):
            st.session_state.step1_complete = False
            st.session_state.step2_complete = False
            pull_dataset_codes()

    with col2:
        st.header("Step 2: Get Data")
        st.write("Uses the codes from Step 1 to download all dataset JSON files from **Source**.")
        if st.button("▶️ Run Step 2: Get Dataset Data", disabled=not st.session_state.step1_complete):
            st.session_state.step2_complete = False
            get_dataset_data()

    with col3:
        st.header("Step 3: Upsert Data")
        st.write("Uploads the downloaded (and transformed) data to the **Destination** environment.")
        if st.button("▶️ Run Step 3: Upsert Datasets", disabled=not st.session_state.step2_complete):
            run_upsert()
    
    if not st.session_state.step1_complete:
        st.warning("Complete Step 1 to enable Step 2.")
    if not st.session_state.step2_complete:
        st.warning("Complete Step 2 to enable Step 3.")
        
    st.divider()

    # --- Optional Transforms ---
    st.header("Optional Transforms")
    st.write("Apply these transformations *after* Step 2 and *before* Step 3.")

    with st.expander("❄️ Snowflake Search & Replace"):
        st.write("Replaces strings in all downloaded `.json` files. Useful for changing environment-specific values.")
        
        # Use session state to persist user's inputs
        if 'replacements' not in st.session_state:
            st.session_state.replacements = {
                'KURTOSYS_RPT_STG.NRC.': 'KURTOSYS_RPT_PRD.NRC.',
                'snowflake_ntam_staging': 'snowflake_ntam_prod'
            }

        r_col1, r_col2 = st.columns(2)
        with r_col1:
            st.text_input("Find 1", value='KURTOSYS_RPT_STG.NRC.', key="find1")
            st.text_input("Find 2", value='snowflake_ntam_staging', key="find2")
        with r_col2:
            st.text_input("Replace 1", value='KURTOSYS_RPT_PRD.NRC.', key="replace1")
            st.text_input("Replace 2", value='snowflake_ntam_prod', key="replace2")
        
        if st.button("Apply Search & Replace", disabled=not st.session_state.step2_complete):
            # Gather replacements from UI
            replacements = {
                st.session_state.find1: st.session_state.replace1,
                st.session_state.find2: st.session_state.replace2,
            }
            # Filter out empty entries
            replacements = {k: v for k, v in replacements.items() if k}
            
            run_search_replace(replacements)

    with st.expander("🔒 Base64 Encoding"):
        st.write("Encodes the `body` and `bodyMeta` fields of all `.json` files. Run this if your destination API expects encoded data.")
        if st.button("Apply Base64 Encoding", disabled=not st.session_state.step2_complete):
            run_encoding()

elif page == "App Deployment (Coming Soon)":
    st.title("📱 App Deployment")
    st.write("This section is under construction. You can add your app deployment scripts here in the same way.")