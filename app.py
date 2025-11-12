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
        return base64_str
    try:
        decoded_bytes = base64.b64decode(base64_str)
        return json.loads(decoded_bytes.decode('utf-8'))
    except (base64.binascii.Error, json.JSONDecodeError):
        return decoded_bytes.decode('utf-8', errors='ignore')


# --- 2. Core Logic Functions ---

def authenticate(base_url, username, password, client_name):
    """PLACEHOLDER: Authenticates to the API and returns a token."""
    # Implement your actual requests.post call here
    # Example:
    # auth_url = f"https://{base_url}/auth/login"
    # response = requests.post(auth_url, ...)
    # response.raise_for_status()
    # return response.text.strip()
    
    # Placeholder return for UI testing:
    if "fail" in username.lower():
        raise Exception("Simulated Authentication Failure")
    return f"MOCK_TOKEN_{time.time()}" 

def pull_dataset_codes(token, base_url):
    """Fetches codes from SOURCE environment."""
    with st.status("Running: 1. Pull Dataset Codes...", expanded=True) as status:
        # Implementation of your /dataset/list endpoint call goes here
        # On success:
        # codes = [...] # list of codes
        # with open(OUTPUT_FILENAME_CODES, 'w') as json_file:
        #     json.dump(codes, json_file, indent=4)
        
        # Placeholder success:
        time.sleep(1)
        os.makedirs(OUTPUT_DIR_CODES, exist_ok=True)
        codes = ["ExampleCode1", "ExampleCode2"]
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
        # Implementation of your /dataset/get/ endpoint call goes here
        # For each dataset:
        
        # result = {'bodyMeta': data_from_api.get("bodyMeta"), ...}
        
        # --- CONDITIONAL DECODING LOGIC ---
        # if snowflake_mode:
        #     if result.get('bodyMeta'):
        #         result['bodyMeta'] = decode_base64(result['bodyMeta'])
        #     if result.get('body'):
        #         result['body'] = decode_base64(result['body'])
        # --- END CONDITIONAL DECODING ---
        
        # Save file:
        # filename = os.path.join(OUTPUT_DIR_DATA, f"{dataset_code.replace(' ', '_')}.json")
        # with open(filename, 'w') as json_file:
        #     json.dump(result, json_file, indent=4)
        
        # Placeholder success:
        time.sleep(2)
        status.update(label=f"Step 2: Fetched datasets successfully!", state="complete")
        st.session_state.step2_complete = True


def run_search_replace(replacements):
    """PLACEHOLDER: Performs find/replace on files in 'input_files'."""
    # Implementation of your recursive search and replace function logic goes here
    with st.status("Running: Search & Replace...", expanded=True) as status:
        time.sleep(1)
        status.update(label="Search & Replace complete!", state="complete")
        st.success("Search & Replace completed successfully on downloaded files.")

def run_encoding():
    """PLACEHOLDER: Base64-encodes 'body' and 'bodyMeta' fields."""
    # Implementation of your file encoding logic goes here
    with st.status("Running: Base64 Encoding...", expanded=True) as status:
        time.sleep(1)
        status.update(label="Encoding complete!", state="complete")
        st.success("Base64 Encoding applied to all files.")


def run_upsert(token, base_url, run_transforms):
    """Upserts files from 'input_files' to DESTINATION with conditional transforms."""
    if run_transforms:
        st.info("Applying automatic Search/Replace and Encoding transforms...")
        
        # Define replacements using the always-editable session state values
        replacements = {st.session_state.find1: st.session_state.replace1,
                        st.session_state.find2: st.session_state.replace2}
        replacements = {k: v for k, v in replacements.items() if k}
        
        # Run Transforms automatically
        run_search_replace(replacements)
        run_encoding()
        
    with st.status("Running: 3. Upsert Datasets...", expanded=True) as status:
        # Implementation of your final /dataset/upsert endpoint call goes here
        # For each file:
        #   response = requests.post(upsert_url, headers=headers, data=file_content)
        
        # Placeholder success:
        time.sleep(2)
        status.update(label="Step 3: Upsert complete!", state="complete")
        st.success("Upsert operation finished successfully.")


# --- 3. UI/Execution ---

st.set_page_config(page_title="Deployment App", layout="wide")

# Initialize session state variables (Find/Replace values are initialized once)
if 'step1_complete' not in st.session_state: st.session_state.step1_complete = False
if 'step2_complete' not in st.session_state: st.session_state.step2_complete = False
if 'source_token' not in st.session_state: st.session_state['source_token'] = None
if 'destination_token' not in st.session_state: st.session_state['destination_token'] = None
if 'snowflake_mode' not in st.session_state: st.session_state['snowflake_mode'] = False
if 'find1' not in st.session_state: st.session_state['find1'] = 'KURTOSYS_RPT_STG.NRC.'
if 'replace1' not in st.session_state: st.session_state['replace1'] = 'KURTOSYS_RPT_PRD.NRC.'
if 'find2' not in st.session_state: st.session_state['find2'] = 'snowflake_ntam_staging'
if 'replace2' not in st.session_state: st.session_state['replace2'] = 'snowflake_ntam_prod'


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
        s_url = st.text_input("Source URL", placeholder="e.g. source-api.com")
        s_user = st.text_input("Source Username")
        s_pass = st.text_input("Source Password", type="password") 
        s_client = st.text_input("Source Client Name")

        st.subheader("Destination Environment")
        d_url = st.text_input("Destination URL", placeholder="e.g. dest-api.com")
        d_user = st.text_input("Destination Username", key='d_user_input')
        d_pass = st.text_input("Destination Password", type="password", key='d_pass_input')
        d_client = st.text_input("Destination Client Name", key='d_client_input')
        
        auth_submitted = st.form_submit_button("Authenticate All")
        
        if auth_submitted:
            try:
                st.session_state['source_token'] = authenticate(s_url, s_user, s_pass, s_client)
                st.session_state['source_url'] = s_url 
                st.success("✅ Source Auth Successful!")
            except Exception as e:
                st.session_state['source_token'] = None
                st.error(f"❌ Source Auth Failed: {e}")
            
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

# 3. Permanent Transform Settings (Always Editable)
st.header("⚙️ Transform Settings (Always Editable)")
st.caption("Edit the search/replace values here. These values are used regardless of the mode selected.")

with st.expander("🔍 Search & Replace Values"):
    st.subheader("View Name and Schema Replacements")
    
    r_col1, r_col2 = st.columns(2)
    with r_col1:
        st.text_input("Find 1 (e.g., Staging Schema)", value=st.session_state['find1'], key="find1")
        st.text_input("Find 2 (e.g., Staging View Prefix)", value=st.session_state['find2'], key="find2")
    with r_col2:
        st.text_input("Replace 1 (e.g., Production Schema)", value=st.session_state['replace1'], key="replace1")
        st.text_input("Replace 2 (e.g., Production View Prefix)", value=st.session_state['replace2'], key="replace2")

st.markdown("---")

# 4. Conditional Transform Execution
st.header("4. Run Optional Transforms")

if not st.session_state.snowflake_mode:
    # --- Standard Mode (Manual Execution) ---
    st.warning("⚠️ **STANDARD MODE:** You must run these steps manually before Upserting.")
    
    col_search, col_encode = st.columns(2)
    with col_search:
        if st.button("Apply Search & Replace", disabled=not st.session_state.step2_complete):
            replacements = {st.session_state.find1: st.session_state.replace1,
                            st.session_state.find2: st.session_state.replace2}
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
