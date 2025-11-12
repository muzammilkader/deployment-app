import streamlit as st
import requests
import json
import os
import base64
import time
import shutil

# --- Constants & Setup ---
OUTPUT_DIR_CODES = 'dataset_codes'
OUTPUT_FILENAME_CODES = os.path.join(OUTPUT_DIR_CODES, 'dataset_codes.json')
OUTPUT_DIR_DATA = 'input_files'

# --- Utility Functions (These remain the same) ---

def clear_workspace():
    """Removes all generated files and directories to start clean."""
    if os.path.exists(OUTPUT_DIR_CODES):
        shutil.rmtree(OUTPUT_DIR_CODES)
    if os.path.exists(OUTPUT_DIR_DATA):
        shutil.rmtree(OUTPUT_DIR_DATA)
    st.session_state.step1_complete = False
    st.session_state.step2_complete = False
    st.session_state['source_token'] = None
    st.session_state['destination_token'] = None
    st.success("Workspace cleared. Please re-authenticate and start over.")

def encode_base64(json_data):
    """Encodes a JSON object to a base64 string."""
    json_str = json.dumps(json_data, separators=(',', ':'))
    return base64.b64encode(json_str.encode('utf-8')).decode('utf-8')

def decode_base64(base64_str):
    """Decodes a base64 string and attempts to load it as JSON."""
    if not isinstance(base64_str, str):
        return base64_str # Not a string, return as is
    try:
        decoded_bytes = base64.b64decode(base64_str)
        return json.loads(decoded_bytes.decode('utf-8'))
    except (base64.binascii.Error, json.JSONDecodeError):
        return decoded_bytes.decode('utf-8', errors='ignore')

# --- Core Logic Functions (Authentication remains the same) ---

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
        auth_response.raise_for_status() 
        token = auth_response.text.strip()
        if token:
            return token
        else:
            raise ValueError("Token not found in response.")
    except requests.exceptions.HTTPError as err:
        raise Exception(f"Authentication failed ({err.response.status_code}): {err.response.text}")
    except Exception as e:
        raise Exception(f"Error during authentication: {e}")

# Function definitions for pull_dataset_codes, get_dataset_data, run_search_replace, run_encoding, and run_upsert
# remain the same as the previous response. (Omitting them here for brevity, but they are required in the full script.)
def pull_dataset_codes(token, base_url):
    """Fetches codes from SOURCE environment."""
    # (function implementation from previous response)
    pass

def get_dataset_data(token, base_url, snowflake_mode):
    """Uses pulled codes to get data from SOURCE, with optional decoding."""
    # (function implementation from previous response, including conditional decoding)
    pass

def run_search_replace(replacements):
    """Performs find/replace on files in 'input_files'."""
    # (function implementation from previous response)
    pass

def run_encoding():
    """Base64-encodes 'body' and 'bodyMeta' fields."""
    # (function implementation from previous response)
    pass

def run_upsert(token, base_url, run_transforms):
    """Upserts files from 'input_files' to DESTINATION with conditional transforms."""
    # (function implementation from previous response, including conditional transforms)
    pass
# --- End Core Logic Functions ---


# Initialize session state variables
if 'step1_complete' not in st.session_state: st.session_state.step1_complete = False
if 'step2_complete' not in st.session_state: st.session_state.step2_complete = False
if 'source_token' not in st.session_state: st.session_state['source_token'] = None
if 'destination_token' not in st.session_state: st.session_state['destination_token'] = None
if 'snowflake_mode' not in st.session_state: st.session_state['snowflake_mode'] = False
# Initialize session state for text inputs (Always visible/persistent)
if 'find1' not in st.session_state: st.session_state['find1'] = 'KURTOSYS_RPT_STG.NRC.'
if 'replace1' not in st.session_state: st.session_state['replace1'] = 'KURTOSYS_RPT_PRD.NRC.'
if 'find2' not in st.session_state: st.session_state['find2'] = 'snowflake_ntam_staging'
if 'replace2' not in st.session_state: st.session_state['replace2'] = 'snowflake_ntam_prod'


# --- Sidebar: Controls and Configuration ---
with st.sidebar:
    st.title("🛠️ Deployment Controls")
    
    # --- Snowflake Toggle ---
    st.header("Mode Selection")
    st.session_state.snowflake_mode = st.toggle(
        "❄️ Snowflake Migration Mode",
        key='mode_toggle',
        value=st.session_state.snowflake_mode,
        help="ON: Decodes payload during Get Data, enables Search/Replace and Encoding before Upsert. OFF: Gets data as-is."
    )
    st.markdown("---")
    
    # --- Credential Input (Remains the same) ---
    st.header("🔑 Credentials")
    # ... Authentication Form content remains here (omitted for brevity)
    
    st.markdown("---")
    if st.button("🧹 Clear Workspace & Session"):
        clear_workspace()
        
    st.caption("Deployment by Gemini")

# --- Main Page: Workflow Steps ---
st.title("🚚 Dataset Deployment Workflow")

# Status indicators
col1, col2 = st.columns(2)
col1.metric("Source Auth Status", "READY" if st.session_state['source_token'] else "PENDING")
col2.metric("Destination Auth Status", "READY" if st.session_state['destination_token'] else "PENDING")

st.info(f"Current Mode: **{'SNOWFLAKE MIGRATION (TRANSFORMS AUTOMATIC)' if st.session_state.snowflake_mode else 'STANDARD (MANUAL TRANSFORMS)'}**")

st.markdown("---")

# 1. Pull Codes Button
if st.button("1. Pull Dataset Codes List", disabled=not st.session_state['source_token'], help="Fetches list of codes from Source."):
    pull_dataset_codes(st.session_state['source_token'], st.session_state['source_url'])

# 2. Get Data Button
if st.button("2. Get Dataset Data", disabled=not st.session_state.step1_complete, help="Downloads data for all codes."):
    get_dataset_data(st.session_state['source_token'], st.session_state['source_url'], st.session_state.snowflake_mode)

st.markdown("---")

# 3. Permanent Transform Settings (ALWAYS VISIBLE)
st.header("⚙️ Transform Settings (Always Editable)")
st.caption("Edit the search/replace values here. These are used in both Standard and Snowflake modes.")

with st.expander("🔍 Search & Replace Values"):
    st.subheader("View Name and Schema Replacements")
    
    # Text inputs are now outside the conditional block and always visible
    r_col1, r_col2 = st.columns(2)
    with r_col1:
        # The key names are used to save the values to session_state
        st.text_input("Find 1 (e.g., Staging Schema)", value=st.session_state['find1'], key="find1")
        st.text_input("Find 2 (e.g., Staging View Prefix)", value=st.session_state['find2'], key="find2")
    with r_col2:
        st.text_input("Replace 1 (e.g., Production Schema)", value=st.session_state['replace1'], key="replace1")
        st.text_input("Replace 2 (e.g., Production View Prefix)", value=st.session_state['replace2'], key="replace2")

st.markdown("---")

# 4. Manual Transform Execution (Only shown in STANDARD MODE)
if not st.session_state.snowflake_mode:
    st.header("Manual Transforms (Standard Mode Execution)")
    st.warning("You must run these steps manually before Upserting in Standard Mode.")
    
    # The execution buttons are now tied directly to the Standard Mode
    col_search, col_encode = st.columns(2)
    with col_search:
        if st.button("Apply Search & Replace", disabled=not st.session_state.step2_complete):
            replacements = {
                st.session_state.find1: st.session_state.replace1,
                st.session_state.find2: st.session_state.replace2,
            }
            replacements = {k: v for k, v in replacements.items() if k}
            run_search_replace(replacements)

    with col_encode:
        if st.button("Apply Base64 Encoding", disabled=not st.session_state.step2_complete):
            run_encoding()

else:
    # Display the automatic execution notice for Snowflake mode
    st.header("Transform Status (Snowflake Mode)")
    st.success("The edits above will be automatically applied before the Upsert.")

st.markdown("---")

# 5. Upsert Button
upsert_disabled = not (st.session_state.step2_complete and st.session_state['destination_token'])

if st.button("3. Upsert Datasets to Destination", type="primary", disabled=upsert_disabled, help="Uploads transformed data to Destination."):
    run_upsert(
        st.session_state['destination_token'], 
        st.session_state['destination_url'], 
        st.session_state.snowflake_mode # Pass the toggle state to run transforms conditionally
    )
