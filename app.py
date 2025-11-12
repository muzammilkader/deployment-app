import streamlit as st
import requests
import json
import os
import base64
import time
import shutil
import re # Added for search/replace

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
    if not isinstance(base64_str, str): return base64_str
    try:
        decoded_bytes = base64.b64decode(base64_str)
        return json.loads(decoded_bytes.decode('utf-8'))
    except (base64.binascii.Error, json.JSONDecodeError):
        return decoded_bytes.decode('utf-8', errors='ignore')

def _recursive_replace(obj, replacements):
    """Recursively replaces strings in dictionaries and lists."""
    if isinstance(obj, dict):
        return {k: _recursive_replace(v, replacements) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_recursive_replace(elem, replacements) for elem in obj]
    elif isinstance(obj, str):
        for find, replace in replacements.items():
            if find and replace:
                 obj = obj.replace(find, replace)
        return obj
    return obj

# --- 2. Core Logic Functions ---

def authenticate(base_url, username, password, client_name):
    """
    CRITICAL: Replace the MOCK implementation below with your actual API call.
    """
    # MOCK SETUP: Allows testing if the credentials match 'source-api.com' and 'test'
    if base_url == "source-api.com" and username == "test":
         return f"MOCK_TOKEN_{time.time()}"
    
    auth_url = f"https://{base_url}/auth/login"
    auth_payload = json.dumps({
        "username": username,
        "password": password,
        "clientName": client_name
    })
    auth_headers = {'Content-Type': 'application/json'}
    
    try:
        ### YOUR IMPLEMENTATION LOGIC GOES HERE ###
        # Example:
        # auth_response = requests.post(auth_url, headers=auth_headers, data=auth_payload, timeout=10)
        # auth_response.raise_for_status() 
        # token = auth_response.text.strip()
        # if not token: raise ValueError("Token not found.")
        # return token
        
        raise Exception("Authentication API not implemented. (See comments in script.)")
    except Exception as e:
        raise Exception(f"Authentication failed: {e}")


def pull_dataset_codes(token, base_url):
    """Fetches codes from SOURCE environment."""
    with st.status("Running: 1. Pull Dataset Codes...", expanded=True) as status:
        ### YOUR IMPLEMENTATION LOGIC GOES HERE ###
        time.sleep(1) 
        os.makedirs(OUTPUT_DIR_CODES, exist_ok=True)
        
        # MOCK data for testing the next step:
        codes = ["ExampleCode1", "ExampleCode2", "ExampleCode3"] 
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
        ### YOUR IMPLEMENTATION LOGIC GOES HERE ###
        time.sleep(2)
        os.makedirs(OUTPUT_DIR_DATA, exist_ok=True)
        
        # MOCK data for testing (Replace this with your actual API calls)
        mock_body_encoded = base64.b64encode(b'{"key": "value_to_edit", "schema": "KURTOSYS_RPT_STG.NRC."}').decode('utf-8')
        mock_data = {"bodyMeta": mock_body_encoded, "body": mock_body_encoded}
        
        for code in ["ExampleCode1", "ExampleCode2", "ExampleCode3"]:
            data_to_save = mock_data.copy()
            
            if snowflake_mode:
                # Decodes the payload for manual editing
                data_to_save['bodyMeta'] = decode_base64(data_to_save['bodyMeta'])
                data_to_save['body'] = decode_base64(data_to_save['body'])
            
            filename = os.path.join(OUTPUT_DIR_DATA, f"{code}.json")
            with open(filename, 'w') as json_file:
                 json.dump(data_to_save, json_file, indent=4)
        
        status.update(label=f"Step 2: Fetched datasets successfully!", state="complete")
        st.session_state.step2_complete = True


def run_search_replace(replacements):
    """Performs find/replace on files in 'input_files'."""
    if not os.path.exists(OUTPUT_DIR_DATA):
        st.error("Error: 'input_files' directory not found.")
        return
    
    file_list = [f for f in os.listdir(OUTPUT_DIR_DATA) if f.endswith('.json')]
    
    with st.status("Running: Search & Replace...", expanded=True) as status:
        for filename in file_list:
            file_path = os.path.join(OUTPUT_DIR_DATA, filename)
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Apply recursive replacement to the entire JSON structure
            new_data = _recursive_replace(data, replacements)
            
            with open(file_path, 'w') as f:
                json.dump(new_data, f, indent=4)
            status.write(f"Processed: {filename}")
        
        status.update(label="Search & Replace complete!", state="complete")
        st.success("Search & Replace completed successfully on downloaded files.")

def run_encoding():
    """Base64-encodes 'body' and 'bodyMeta' fields."""
    if not os.path.exists(OUTPUT_DIR_DATA):
        st.error("Error: 'input_files' directory not found.")
        return
        
    file_list = [f for f in os.listdir(OUTPUT_DIR_DATA) if f.endswith('.json')]
    
    with st.status("Running: Base64 Encoding...", expanded=True) as status:
        for filename in file_list:
            file_path = os.path.join(OUTPUT_DIR_DATA, filename)
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Only encode if the content is NOT already encoded (i.e., it is a dictionary/JSON)
            if isinstance(data.get('bodyMeta'), dict):
                data['bodyMeta'] = encode_base64(data['bodyMeta'])
            if isinstance(data.get('body'), dict):
                data['body'] = encode_base64(data['body'])
            
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=4)
            status.write(f"Encoded: {filename}")
        
        status.update(label="Encoding complete!", state="complete")
        st.success("Base64 Encoding applied to all files.")

def run_upsert(token, base_url, run_transforms):
    """Upserts files from 'input_files' to DESTINATION with conditional transforms."""
    if run_transforms:
        st.info("Applying automatic Search/Replace and Encoding transforms...")
        
        # Pull latest values from the editable UI fields
        replacements = {st.session_state.find1_ui: st.session_state.replace1_ui,
                        st.session_state.find2_ui: st.session_state.replace2_ui}
        replacements = {k: v for k, v in replacements.items() if k}
        
        run_search_replace(replacements)
        run_encoding()
        
    with st.status("Running: 3. Upsert Datasets...", expanded=True) as status:
        ### YOUR IMPLEMENTATION LOGIC GOES HERE ###
        time.sleep(2)
        status.update(label="Step 3: Upsert complete!", state="complete")
        st.success("Upsert operation finished successfully.")


# --- 3. UI/Execution ---

st.set_page_config(page_title="Deployment App", layout="wide")

# Initialize session state variables
for key in ['step1_complete', 'step2_complete', 'source_token', 'destination_token', 'snowflake_mode']:
    if key not in st.session_state:
        st.session_state[key] = False if key.startswith('step') or key == 'snowflake_mode' else None
for key in ['find1', 'replace1', 'find2', 'replace2', 'find1_ui', 'replace1_ui', 'find2_ui', 'replace2_ui']:
    if key not in st.session_state: st.session_state[key] = ''

# Set initial default values for the UI fields (to be picked up by the 'value' attribute)
if not st.session_state['find1']: st.session_state['find1'] = 'KURTOSYS_RPT_STG.NRC.'
if not st.session_state['replace1']: st.session_state['replace1'] = 'KURTOSYS_RPT_PRD.NRC.'
if not st.session_state['find2']: st.session_state['find2'] = 'snowflake_ntam_staging'
if not st.session_state['replace2']: st.session_state['replace2'] = 'snowflake_ntam_prod'


# --- Sidebar: Controls and Configuration ---
with st.sidebar:
    st.title("🛠️ Deployment Controls")
    
    # --- Mode Selection ---
    st.header("Mode Selection")
    st.session_state.snowflake_mode = st.toggle(
        "❄️ **Snowflake Migration Mode**",
        key='mode_toggle',
        value=st.session_state.snowflake_mode,
        help="ON: Decodes on GET, runs Search/Replace and Encoding automatically on UPSERT. OFF: Manual transforms required."
    )
    st.markdown("---")
    
    # --- Credential Input ---
    st.header("🔑 Credentials")
    
    with st.form("auth_form"):
        st.subheader("Source Environment")
        s_url = st.text_input("Source URL", placeholder="e.g. source-api.com", key='s_url_input')
        s_user = st.text_input("Source Username", key='s_user_input')
        s_pass = st.text_input("Source Password", type="password", key='s_pass_input') 
        s_client = st.text_input("Source Client Name", key='s_client_input')

        st.subheader("Destination Environment")
        d_url = st.text_input("Destination URL", placeholder="e.g. dest-api.com", key='d_url_input')
        d_user = st.text_input("Destination Username", key='d_user_input_dest')
        d_pass = st.text_input("Destination Password", type="password", key='d_pass_input_dest')
        d_client = st.text_input("Destination Client Name", key='d_client_input_dest')
        
        auth_submitted = st.form_submit_button("Authenticate All")
        
        if auth_submitted:
            # Authenticate Source
            try:
                st.session_state['source_token'] = authenticate(s_url, s_user, s_pass, s_client)
                st.session_state['source_url'] = s_url 
                st.success("✅ Source Auth Successful!")
            except Exception as e:
                st.session_state['source_token'] = None
                st.error(f"❌ Source Auth Failed: {e}")
            
            # Authenticate Destination
            try:
                st.session_state['destination_token'] = authenticate(d_url, d_user, d_pass, d_client)
                st.session_state['destination_url'] = d_url
                st.success("✅ Destination Auth Successful!")
            except Exception as e:
                st.session_state['destination_token'] = None
                st.error(f"❌ Destination Auth Failed: {e}")
            
    st.markdown("---")
    if st.button("🧹 Clear Workspace & Session"):
        clear_workspace()
        
    st.caption("Deployment tool running via Streamlit")

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

# --- 2.5 View & Edit Data (Manual Override) ---
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
        # Note: The key names are explicit 'find1_ui', etc., to ensure they update correctly.
        st.text_input("Find 1 (e.g., Staging Schema)", value=st.session_state['find1'], key="find1_ui")
        st.text_input("Find 2 (e.g., Staging View Prefix)", value=st.session_state['find2'], key="find2_ui")
    with r_col2:
        st.text_input("Replace 1 (e.g., Production Schema)", value=st.session_state['replace1'], key="replace1_ui")
        st.text_input("Replace 2 (e.g., Production View Prefix)", value=st.session_state['replace2'], key="replace2_ui")

st.markdown("---")

# 4. Conditional Transform Execution
st.header("4. Run Optional Transforms")

if not st.session_state.snowflake_mode:
    # --- Standard Mode (Manual Execution) ---
    st.warning("⚠️ **STANDARD MODE:** You must run these steps manually before Upserting.")
    
    col_search, col_encode = st.columns(2)
    with col_search:
        if st.button("Apply Search & Replace", disabled=not st.session_state.step2_complete):
            # The function uses the values entered in the expander above (find1_ui, etc.)
            replacements = {st.session_state.find1_ui: st.session_state.replace1_ui,
                            st.session_state.find2_ui: st.session_state.replace2_ui}
            replacements = {k: v for k, v in replacements.items() if k}
            run_search_replace(replacements)
    with col_encode:
        if st.button("Apply Base64 Encoding", disabled=not st.session_state.step2_complete):
            run_encoding()
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
