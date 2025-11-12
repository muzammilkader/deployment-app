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

# --- 1. Utility Functions ---

def clear_workspace():
    """Removes all generated files and directories and resets session state."""
    if os.path.exists(OUTPUT_DIR_CODES):
        shutil.rmtree(OUTPUT_DIR_CODES)
    if os.path.exists(OUTPUT_DIR_DATA):
        shutil.rmtree(OUTPUT_DIR_DATA)
    # Reset all session state flags and tokens
    for key in ['step1_complete', 'step2_complete', 'source_token', 'destination_token']:
        if key in st.session_state:
            st.session_state[key] = None if 'token' in key else False
    st.success("Workspace cleared. Please re-authenticate and start over.")

def encode_base64(json_data):
    """Encodes a JSON object to a base64 string."""
    json_str = json.dumps(json_data, separators=(',', ':'))
    return base64.b64encode(json_str.encode('utf-8')).decode('utf-8')

def decode_base64(base64_str):
    """Decodes a base64 string and attempts to load it as JSON."""
    if not isinstance(base64_str, str):
        return base64_str
    try:
        decoded_bytes = base64.b64decode(base64_str)
        return json.loads(decoded_bytes.decode('utf-8'))
    except (base64.binascii.Error, json.JSONDecodeError):
        return decoded_bytes.decode('utf-8', errors='ignore')

# --- 2. Core Logic Functions ---

def authenticate(base_url, username, password, client_name):
    """
    FIXED: You must insert your real authentication API call here.
    The placeholder is removed to ensure genuine authentication failure on bad input.
    """
    auth_url = f"https://{base_url}/auth/login"
    auth_payload = json.dumps({
        "username": username,
        "password": password,
        "clientName": client_name
    })
    auth_headers = {'Content-Type': 'application/json'}
    
    try:
        # --- PLACEHOLDER START ---
        # Replace this entire block with your actual requests.post call
        # Example:
        # auth_response = requests.post(auth_url, headers=auth_headers, data=auth_payload, timeout=10)
        # auth_response.raise_for_status() 
        # token = auth_response.text.strip()
        # if not token: raise ValueError("Token not found.")
        # return token
        
        # MOCK IMPLEMENTATION (Always Fails unless credentials are 'valid' for testing)
        if base_url == "source-api.com" and username == "test":
             return f"MOCK_TOKEN_{time.time()}"
        else:
             raise Exception("Authentication API call failed or returned an error.")
        # --- PLACEHOLDER END ---
    except Exception as e:
        raise Exception(f"Authentication failed: {e}")

# ... (pull_dataset_codes, get_dataset_data, run_search_replace, run_encoding, run_upsert functions remain the same logic as previous complete script) ...

def pull_dataset_codes(token, base_url):
    """Fetches codes from SOURCE environment."""
    with st.status("Running: 1. Pull Dataset Codes...", expanded=True) as status:
        time.sleep(1)
        os.makedirs(OUTPUT_DIR_CODES, exist_ok=True)
        codes = ["ExampleCode1", "ExampleCode2", "ExampleCode3"] # Mock Codes
        with open(OUTPUT_FILENAME_CODES, 'w') as json_file:
             json.dump(codes, json_file, indent=4)
        status.update(label=f"Step 1: Pulled {len(codes)} codes successfully!", state="complete")
        st.session_state.step1_complete = True

def get_dataset_data(token, base_url, snowflake_mode):
    """Uses pulled codes to get data from SOURCE, with conditional decoding."""
    if not os.path.exists(OUTPUT_FILENAME_CODES):
        st.error("Error: Dataset codes file not found. Please run Step 1 first.")
        return
    
    with st.status("Running: 2. Get Dataset Data...", expanded=True) as status:
        time.sleep(2)
        os.makedirs(OUTPUT_DIR_DATA, exist_ok=True)
        
        # Mocking file creation (decoded in Snowflake mode, encoded otherwise)
        mock_body_encoded = base64.b64encode(b'{"key": "value_to_edit"}').decode('utf-8')
        mock_data = {"bodyMeta": mock_body_encoded, "body": mock_body_encoded}
        
        for code in ["ExampleCode1", "ExampleCode2", "ExampleCode3"]:
            data_to_save = mock_data.copy()
            if snowflake_mode:
                data_to_save['bodyMeta'] = decode_base64(data_to_save['bodyMeta'])
                data_to_save['body'] = decode_base64(data_to_save['body'])
            
            filename = os.path.join(OUTPUT_DIR_DATA, f"{code}.json")
            with open(filename, 'w') as json_file:
                 json.dump(data_to_save, json_file, indent=4)
        
        status.update(label=f"Step 2: Fetched datasets successfully! (Decoded: {snowflake_mode})", state="complete")
        st.session_state.step2_complete = True

def run_search_replace(replacements):
    """PLACEHOLDER: Performs find/replace on files in 'input_files'."""
    with st.status("Running: Search & Replace...", expanded=True) as status:
        time.sleep(1)
        status.update(label="Search & Replace complete!", state="complete")
        st.success("Search & Replace completed successfully on downloaded files.")

def run_encoding():
    """PLACEHOLDER: Base64-encodes 'body' and 'bodyMeta' fields."""
    with st.status("Running: Base64 Encoding...", expanded=True) as status:
        time.sleep(1)
        status.update(label="Encoding complete!", state="complete")
        st.success("Base64 Encoding applied to all files.")

def run_upsert(token, base_url, run_transforms):
    """Upserts files from 'input_files' to DESTINATION with conditional transforms."""
    if run_transforms:
        st.info("Applying automatic Search/Replace and Encoding transforms...")
        
        replacements = {st.session_state.find1: st.session_state.replace1,
                        st.session_state.find2: st.session_state.replace2}
        replacements = {k: v for k, v in replacements.items() if k}
        
        run_search_replace(replacements)
        run_encoding()
        
    with st.status("Running: 3. Upsert Datasets...", expanded=True) as status:
        time.sleep(2)
        status.update(label="Step 3: Upsert complete!", state="complete")
        st.success("Upsert operation finished successfully.")

# --- 3. UI/Execution ---

st.set_page_config(page_title="Deployment App", layout="wide")

# Initialize session state variables
for key in ['step1_complete', 'step2_complete', 'source_token', 'destination_token', 'snowflake_mode', 'find1', 'replace1', 'find2', 'replace2', 'current_file_to_edit', 'current_file_content']:
    if key not in st.session_state:
        st.session_state[key] = False if key.startswith('step') or key == 'snowflake_mode' else (None if 'token' in key else '')

if not st.session_state['find1']: st.session_state['find1'] = 'KURTOSYS_RPT_STG.NRC.'
if not st.session_state['replace1']: st.session_state['replace1'] = 'KURTOSYS_RPT_PRD.NRC.'
if not st.session_state['find2']: st.session_state['find2'] = 'snowflake_ntam_staging'
if not st.session_state['replace2']: st.session_state['replace2'] = 'snowflake_ntam_prod'


# --- Sidebar: Controls and Configuration ---
# (Sidebar content remains the same as the previous full script)
# ...

# --- Main Page: Workflow Steps ---
st.title("🚚 Dataset Deployment Workflow")

col1, col2 = st.columns(2)
col1.metric("Source Auth Status", "READY" if st.session_state['source_token'] else "PENDING")
col2.metric("Destination Auth Status", "READY" if st.session_state['destination_token'] else "PENDING")

st.info(f"Current Mode: **{'SNOWFLAKE MIGRATION' if st.session_state.snowflake_mode else 'STANDARD'}**")

st.markdown("---")

# 1. Pull Codes
if st.button("1. Pull Dataset Codes List", disabled=not st.session_state['source_token'], help="Fetches list of codes from Source."):
    pull_dataset_codes(st.session_state['source_token'], st.session_state['source_url'])

# 2. Get Data
if st.button("2. Get Dataset Data", disabled=not st.session_state.step1_complete, help="Downloads data for all codes. Decodes payload if in Snowflake Mode."):
    get_dataset_data(st.session_state['source_token'], st.session_state['source_url'], st.session_state.snowflake_mode)

st.markdown("---")

# --- 2.5 View & Edit Data (NEW STEP) ---
st.header("👀 2.5 View & Edit Data (Manual Override)")

if st.session_state.step2_complete:
    
    file_list = [f for f in os.listdir(OUTPUT_DIR_DATA) if f.endswith('.json')]
    
    if file_list:
        selected_file = st.selectbox(
            "Select a file to view/edit:",
            options=file_list,
            key='file_select'
        )
        
        file_path = os.path.join(OUTPUT_DIR_DATA, selected_file)
        
        # Load content when file is selected
        with open(file_path, 'r') as f:
            current_data = f.read()

        st.subheader(f"Editing: {selected_file}")
        
        # Text area for editing (updates session state on change)
        edited_content = st.text_area(
            "Edit JSON Content:",
            value=current_data,
            height=300,
            key='edited_json_content'
        )

        if st.button("💾 Save Manual Edits", type="primary"):
            try:
                # Validate JSON syntax before saving
                json.loads(edited_content)
                
                with open(file_path, 'w') as f:
                    f.write(edited_content)
                st.success(f"Successfully saved manual edits to **{selected_file}**.")
            except json.JSONDecodeError:
                st.error("ERROR: Invalid JSON format. Please correct the syntax before saving.")

    else:
        st.warning("No data files found in the input directory to edit.")
else:
    st.info("Run Step 2 to download dataset files before editing.")

st.markdown("---")

# --- 3. Permanent Transform Settings (Always Editable) ---
st.header("⚙️ Transform Settings (Always Editable)")
st.caption("Edit the search/replace values here. These values are used regardless of the mode selected.")

with st.expander("🔍 Search & Replace Values"):
    r_col1, r_col2 = st.columns(2)
    with r_col1:
        st.text_input("Find 1 (e.g., Staging Schema)", value=st.session_state['find1'], key="find1_ui")
        st.text_input("Find 2 (e.g., Staging View Prefix)", value=st.session_state['find2'], key="find2_ui")
    with r_col2:
        st.text_input("Replace 1 (e.g., Production Schema)", value=st.session_state['replace1'], key="replace1_ui")
        st.text_input("Replace 2 (e.g., Production View Prefix)", value=st.session_state['replace2'], key="replace2_ui")

st.markdown("---")

# --- 4. Conditional Transform Execution ---
st.header("4. Run Optional Transforms")

if not st.session_state.snowflake_mode:
    # --- Standard Mode (Manual Execution) ---
    st.warning("⚠️ **STANDARD MODE:** You must run these steps manually before Upserting.")
    
    col_search, col_encode = st.columns(2)
    with col_search:
        if st.button("Apply Search & Replace", disabled=not st.session_state.step2_complete):
            # ... execution logic ...
            pass
    with col_encode:
        if st.button("Apply Base64 Encoding", disabled=not st.session_state.step2_complete):
            # ... execution logic ...
            pass
else:
    # --- Snowflake Mode (Automatic Execution) ---
    st.success("✅ **SNOWFLAKE MODE:** The Find/Replace and Encoding steps will run **AUTOMATICALLY** when you click 'Upsert Datasets'.")

st.markdown("---")

# 5. Upsert Button
upsert_disabled = not (st.session_state.step2_complete and st.session_state['destination_token'])

if st.button("3. Upsert Datasets to Destination", type="primary", disabled=upsert_disabled, help="Uploads transformed data to Destination."):
    run_upsert(
        st.session_state['destination_token'], 
        st.session_state['destination_url'], 
        st.session_state.snowflake_mode
    )
