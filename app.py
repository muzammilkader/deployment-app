# app.py
import streamlit as st
import requests
import json
import os
import time
import shutil
from typing import Optional

# -------------------------
# Config / Constants
# -------------------------
OUTPUT_DIR = "input_files"   # saved dataset JSONs for editing
CODES_FILE = "dataset_codes.json"

# -------------------------
# Helper functions
# -------------------------
def ensure_dirs():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

def save_local_dataset(code: str, payload: dict):
    ensure_dirs()
    path = os.path.join(OUTPUT_DIR, f"{code}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    st.session_state.local_files[code] = path

def load_local_dataset(code: str) -> Optional[dict]:
    ensure_dirs()
    path = os.path.join(OUTPUT_DIR, f"{code}.json")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

def try_auth_endpoints(base_url: str, username: str, password: str, client_name: str, timeout=12) -> str:
    """
    Try expected auth endpoints and return token string.
    Tries common endpoints and token formats:
      - POST https://{base_url}/auth/login
      - POST https://{base_url}/authenticate
      - POST https://{base_url}/auth
    Expects either JSON {"token": "..."} or text token in body.
    Raises Exception if all attempts fail.
    """
    candidate_paths = ["/auth/login", "/authenticate", "/auth"]
    payload = {"username": username, "password": password, "clientName": client_name}
    headers = {"Content-Type": "application/json"}

    last_err = None
    for p in candidate_paths:
        url = f"https://{base_url.rstrip('/')}{p}"
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
        except Exception as e:
            last_err = e
            continue
        # Accept 200 or 201
        if resp.status_code in (200, 201):
            # Try parse JSON token
            try:
                data = resp.json()
                # common shapes: {"token": "xyz"} or {"access_token": "xyz"} or {"data": {"token": "..."}}
                for key in ("token", "access_token", "accessToken"):
                    if key in data and isinstance(data[key], str):
                        return data[key]
                # look for token nested
                if isinstance(data, dict):
                    for v in data.values():
                        if isinstance(v, str) and len(v) > 10:
                            return v
            except ValueError:
                # not JSON, maybe raw token text
                text = resp.text.strip().strip('"')
                if text:
                    return text
        else:
            last_err = Exception(f"{resp.status_code} {resp.text[:200]}")
    raise Exception(f"Authentication failed for all endpoints. Last error: {last_err}")

def headers_for(token: str):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def fetch_dataset_codes(token: str, base_url: str) -> list:
    """
    Calls GET /datasets (or /data/datasets) to list dataset codes.
    Returns list of dicts or raises Exception.
    """
    candidate_paths = ["/datasets", "/data/datasets", "/api/datasets"]
    for p in candidate_paths:
        url = f"https://{base_url.rstrip('/')}{p}"
        try:
            resp = requests.get(url, headers=headers_for(token), timeout=30)
        except Exception:
            continue
        if resp.status_code == 200:
            try:
                data = resp.json()
                # If API returns dict like {"items":[...]} try to normalize
                if isinstance(data, dict) and "items" in data and isinstance(data["items"], list):
                    return data["items"]
                if isinstance(data, list):
                    return data
                # some APIs return {"datasets": [...]}
                for k in ("datasets", "data"):
                    if isinstance(data.get(k), list):
                        return data[k]
                # Fallback: if dict of codes
                return [data]
            except ValueError:
                raise Exception("Dataset listing returned invalid JSON.")
        # else try next candidate
    raise Exception("Failed to fetch dataset codes from known endpoints.")

def fetch_dataset(token:str, base_url:str, code:str) -> dict:
    """
    GET dataset by code. Tries common URL patterns.
    """
    candidate_paths = [f"/datasets/{code}", f"/data/datasets/{code}", f"/api/datasets/{code}"]
    for p in candidate_paths:
        url = f"https://{base_url.rstrip('/')}{p}"
        try:
            resp = requests.get(url, headers=headers_for(token), timeout=30)
        except Exception:
            continue
        if resp.status_code == 200:
            try:
                return resp.json()
            except ValueError:
                raise Exception("Dataset endpoint returned invalid JSON.")
        elif resp.status_code == 404:
            continue
        else:
            # if other error code, keep trying other patterns
            continue
    raise Exception(f"Failed to fetch dataset '{code}' from API.")

def upsert_dataset(token:str, base_url:str, code:str, payload:dict) -> dict:
    """
    Attempts to upsert a dataset. Tries PUT /datasets/{code}, POST /datasets, POST /datasets/{code}/upsert
    Returns API JSON on success.
    """
    attempts = []
    # 1) PUT /datasets/{code}
    url_put = f"https://{base_url.rstrip('/')}/datasets/{code}"
    try:
        r = requests.put(url_put, headers=headers_for(token), json=payload, timeout=30)
        if r.status_code in (200,201):
            return try_parse_json_or_text(r)
        attempts.append((url_put, r.status_code, r.text[:200]))
    except Exception as e:
        attempts.append((url_put, "err", str(e)))

    # 2) POST /datasets
    url_post = f"https://{base_url.rstrip('/')}/datasets"
    try:
        r = requests.post(url_post, headers=headers_for(token), json=payload, timeout=30)
        if r.status_code in (200,201):
            return try_parse_json_or_text(r)
        attempts.append((url_post, r.status_code, r.text[:200]))
    except Exception as e:
        attempts.append((url_post, "err", str(e)))

    # 3) POST /datasets/{code}/upsert
    url_post2 = f"https://{base_url.rstrip('/')}/datasets/{code}/upsert"
    try:
        r = requests.post(url_post2, headers=headers_for(token), json=payload, timeout=30)
        if r.status_code in (200,201):
            return try_parse_json_or_text(r)
        attempts.append((url_post2, r.status_code, r.text[:200]))
    except Exception as e:
        attempts.append((url_post2, "err", str(e)))

    raise Exception(f"Upsert attempts failed: {attempts}")

def delete_dataset(token:str, base_url:str, code:str) -> dict:
    """
    DELETE /datasets/{code}
    """
    url = f"https://{base_url.rstrip('/')}/datasets/{code}"
    r = requests.delete(url, headers=headers_for(token), timeout=30)
    if r.status_code in (200,204):
        return {"status": "deleted", "code": code}
    raise Exception(f"Delete failed ({r.status_code}): {r.text}")

def try_parse_json_or_text(resp: requests.Response):
    try:
        return resp.json()
    except ValueError:
        return {"status_text": resp.text.strip()}

def clear_local_files():
    if os.path.exists(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)
    st.session_state.local_files = {}

# -------------------------
# Page state init
# -------------------------
if "local_files" not in st.session_state:
    st.session_state.local_files = {}
if "dataset_codes_raw" not in st.session_state:
    st.session_state.dataset_codes_raw = []  # raw objects from API
if "dataset_codes_order" not in st.session_state:
    st.session_state.dataset_codes_order = []  # list of codes (strings) maintain order
if "selected_code" not in st.session_state:
    st.session_state.selected_code = None
if "edits_saved" not in st.session_state:
    st.session_state.edits_saved = {}
if "deploy_checks" not in st.session_state:
    st.session_state.deploy_checks = {}  # code -> bool
if "delete_checks" not in st.session_state:
    st.session_state.delete_checks = {}

st.set_page_config(page_title="Dataset Deployment", layout="wide")
st.title("Dataset Deployments")

# -------------------------
# Sidebar: Environment + Auth + Find/Replace
# -------------------------
with st.sidebar:
    st.header("🔧 Environment & Mode")
    base_url = st.text_input("API Environment (one URL)", value=st.session_state.get("base_url","api-us.kurtosys.app"))
    st.session_state.base_url = base_url.strip()

    snowflake_mode = st.checkbox("❄️ Snowflake Migration Mode (editable body)", value=st.session_state.get("snowflake_mode", True))
    st.session_state.snowflake_mode = snowflake_mode

    st.markdown("---")
    st.header("🔑 Authentication (Source / Destination)")
    st.write("Only the **credentials** differ — one base URL is used for both.")

    with st.form("auth_form"):
        st.subheader("Source Credentials")
        s_user = st.text_input("Source Username", value=st.session_state.get("s_user",""))
        s_pass = st.text_input("Source Password", type="password", value=st.session_state.get("s_pass",""))
        s_client = st.text_input("Source Client Name", value=st.session_state.get("s_client",""))

        st.subheader("Destination Credentials")
        d_user = st.text_input("Destination Username", value=st.session_state.get("d_user",""))
        d_pass = st.text_input("Destination Password", type="password", value=st.session_state.get("d_pass",""))
        d_client = st.text_input("Destination Client Name", value=st.session_state.get("d_client",""))

        submitted = st.form_submit_button("Authenticate Both")
        if submitted:
            # store these values in session for persistence
            st.session_state.update({
                "s_user": s_user, "s_pass": s_pass, "s_client": s_client,
                "d_user": d_user, "d_pass": d_pass, "d_client": d_client
            })
            try:
                st.info("Authenticating source...")
                st.session_state.source_token = try_auth_endpoints(base_url, s_user, s_pass, s_client)
                st.success("Source authenticated.")
            except Exception as e:
                st.session_state.source_token = None
                st.error(f"Source auth failed: {e}")

            try:
                st.info("Authenticating destination...")
                st.session_state.destination_token = try_auth_endpoints(base_url, d_user, d_pass, d_client)
                st.success("Destination authenticated.")
            except Exception as e:
                st.session_state.destination_token = None
                st.error(f"Destination auth failed: {e}")

    st.markdown("---")
    st.header("🔍 Find & Replace (editable)")
    # These are editable by default (user demanded)
    find1 = st.text_input("Find 1 (e.g. staging schema)", value=st.session_state.get("find1","KURTOSYS_RPT_STG.NRC."))
    replace1 = st.text_input("Replace 1 (e.g. prod schema)", value=st.session_state.get("replace1","KURTOSYS_RPT_PRD.NRC."))
    find2 = st.text_input("Find 2 (e.g. staging view)", value=st.session_state.get("find2","snowflake_ntam_staging"))
    replace2 = st.text_input("Replace 2 (e.g. prod view)", value=st.session_state.get("replace2","snowflake_ntam_prod"))

    st.session_state.find1 = find1
    st.session_state.replace1 = replace1
    st.session_state.find2 = find2
    st.session_state.replace2 = replace2

    st.markdown("---")
    if st.button("🧹 Clear local saved files"):
        clear_local_files()
        st.success("Local saved files cleared.")

# -------------------------
# Main: Controls (top)
# -------------------------
col1, col2, col3 = st.columns([2,2,1])
with col1:
    if st.button("1️⃣ Pull Dataset Codes (list from source)", disabled=not st.session_state.get("source_token")):
        try:
            raw = fetch_dataset_codes(st.session_state.source_token, st.session_state.base_url)
            st.session_state.dataset_codes_raw = raw
            # Normalize codes list: try to extract 'code' or 'id' or 'name'
            codes = []
            for item in raw:
                if isinstance(item, dict):
                    for key in ("code","datasetCode","id","name"):
                        if key in item:
                            codes.append(str(item[key]))
                            break
                    else:
                        # if no known key, dump repr
                        codes.append(json.dumps(item))
                else:
                    codes.append(str(item))
            st.session_state.dataset_codes_order = codes
            # reset checks
            st.session_state.deploy_checks = {c: False for c in codes}
            st.session_state.delete_checks = {c: False for c in codes}
            st.success(f"Pulled {len(codes)} dataset codes.")
        except Exception as e:
            st.error(f"Failed to pull dataset codes: {e}")

with col2:
    if st.button("2️⃣ Fetch selected dataset(s) data from source", disabled=not st.session_state.get("source_token")):
        if not st.session_state.dataset_codes_order:
            st.warning("No dataset codes available. Pull codes first.")
        else:
            fetched = 0
            errors = []
            for code in st.session_state.dataset_codes_order:
                try:
                    data = fetch_dataset(st.session_state.source_token, st.session_state.base_url, code)
                    # If Snowflake mode -> leave body as editable JSON (no encoding)
                    # Save the dataset payload as fetched so user can edit (body not encoded)
                    save_local_dataset(code, data)
                    fetched += 1
                except Exception as e:
                    errors.append((code, str(e)))
            st.success(f"Fetched {fetched} datasets. {len(errors)} errors.")
            if errors:
                st.error(f"Errors for {len(errors)} datasets — check logs.")
                for c, msg in errors[:5]:
                    st.write(f"{c}: {msg}")

with col3:
    # quick indicators
    src_status = "READY" if st.session_state.get("source_token") else "PENDING"
    dst_status = "READY" if st.session_state.get("destination_token") else "PENDING"
    st.metric("Source Auth", src_status)
    st.metric("Destination Auth", dst_status)

st.markdown("---")

# -------------------------
# Dataset List & selection with checkboxes
# -------------------------
st.header("Dataset Inventory")
if not st.session_state.dataset_codes_order:
    st.info("No dataset list available. Click 'Pull Dataset Codes' to load the list from source.")
else:
    st.write("Select datasets to Deploy/Delete. Use the checkboxes then press the bulk action buttons below.")
    # We'll render a table-like list with checkboxes
    codes = st.session_state.dataset_codes_order
    # ensure checks dict contains keys
    for c in codes:
        st.session_state.deploy_checks.setdefault(c, False)
        st.session_state.delete_checks.setdefault(c, False)

    # Render in a scrollable container
    with st.container():
        for c in codes:
            cols = st.columns([3,1,1,1])
            cols[0].write(f"**{c}**")
            # show a small "local saved" or "not fetched" label
            if c in st.session_state.local_files:
                cols[0].caption("Local copy available")
            else:
                cols[0].caption("No local copy")

            st.session_state.deploy_checks[c] = cols[1].checkbox("Deploy", value=st.session_state.deploy_checks[c], key=f"deploy_{c}")
            st.session_state.delete_checks[c] = cols[2].checkbox("Delete", value=st.session_state.delete_checks[c], key=f"delete_{c}")
            # quick action to open editor for this dataset
            if cols[3].button("Open", key=f"open_{c}"):
                st.session_state.selected_code = c
                st.experimental_rerun()

    st.markdown("---")

    # Bulk actions
    bcol1, bcol2, bcol3 = st.columns(3)
    with bcol1:
        if st.button("📤 Bulk Upsert Selected (deploy)", disabled=not st.session_state.get("destination_token")):
            # iterate deploy checks
            results = []
            errors = []
            for code, should in st.session_state.deploy_checks.items():
                if not should: continue
                # load local saved edits if exist, else load last fetched
                payload = load_local_dataset(code)
                if payload is None:
                    st.warning(f"Skipping {code}: no local copy fetched.")
                    continue
                try:
                    # For snowflake mode we assume body is JSON and editable; upsert payload as-is
                    res = upsert_dataset(st.session_state.destination_token, st.session_state.base_url, code, payload)
                    results.append((code, res))
                except Exception as e:
                    errors.append((code, str(e)))
            st.success(f"Upsert attempts done. Success: {len(results)}. Errors: {len(errors)}")
            if errors:
                for c, msg in errors[:5]:
                    st.error(f"{c}: {msg}")

    with bcol2:
        if st.button("🗑️ Bulk Delete Selected"):
            results = []
            errors = []
            for code, should in st.session_state.delete_checks.items():
                if not should: continue
                try:
                    res = delete_dataset(st.session_state.destination_token, st.session_state.base_url, code)
                    results.append((code, res))
                except Exception as e:
                    errors.append((code, str(e)))
            st.success(f"Delete attempts done. Success: {len(results)}. Errors: {len(errors)}")
            if errors:
                for c, msg in errors[:5]:
                    st.error(f"{c}: {msg}")

    with bcol3:
        if st.button("🔄 Refresh local list (show saved files)"):
            # Will show session_state.local_files content below
            pass

# -------------------------
# Editor / Inspector panel
# -------------------------
st.markdown("---")
st.header("Dataset Inspector & Editor")

selected = st.session_state.get("selected_code")
if not selected:
    # show a quick list of local files with open buttons
    if st.session_state.local_files:
        st.info("Open a dataset from the inventory above or choose one of the locally saved files.")
        for k, v in st.session_state.local_files.items():
            cols = st.columns([3,1,1])
            cols[0].write(f"**{k}** — {v}")
            if cols[1].button("Open", key=f"open_local_{k}"):
                st.session_state.selected_code = k
                st.experimental_rerun()
            if cols[2].button("Delete local copy", key=f"del_local_{k}"):
                try:
                    os.remove(v)
                except Exception:
                    pass
                st.session_state.local_files.pop(k, None)
                st.success("Deleted local copy.")
                st.experimental_rerun()
    else:
        st.info("No local dataset fetched yet. Use 'Fetch selected dataset(s) data from source' to pull dataset payloads for editing.")

else:
    st.subheader(f"Editing: {selected}")
    # load payload (either local saved or fetch fresh from source)
    payload = load_local_dataset(selected)
    if payload is None:
        # attempt to fetch live (fallback)
        try:
            payload = fetch_dataset(st.session_state.source_token, st.session_state.base_url, selected)
            save_local_dataset(selected, payload)
            payload = load_local_dataset(selected)
            st.success("Fetched live copy to local store for editing.")
        except Exception as e:
            st.error(f"Cannot load dataset: {e}")
            payload = None

    if payload:
        # Render metadata and body
        # We allow editing only in Snowflake mode (as requested). In standard mode, it's read-only.
        editable = bool(st.session_state.snowflake_mode)

        # Show top-level keys, but allow full raw JSON editing for simplicity and power.
        raw_json = json.dumps(payload, indent=2, ensure_ascii=False)
        if editable:
            edited = st.text_area("Edit full dataset JSON (body + metadata). Save to local before upsert.", value=raw_json, height=450, key=f"editor_{selected}")
            col_save, col_reload = st.columns([1,1])
            if col_save.button("💾 Save Edits", key=f"save_{selected}"):
                # validate JSON
                try:
                    parsed = json.loads(edited)
                    save_local_dataset(selected, parsed)
                    st.success("Local edits saved.")
                    st.session_state.edits_saved[selected] = time.time()
                except json.JSONDecodeError as je:
                    st.error(f"Invalid JSON: {je}")
            if col_reload.button("🔁 Reload from local disk", key=f"reload_{selected}"):
                payload = load_local_dataset(selected)
                st.experimental_rerun()
        else:
            st.code(raw_json, language="json")
            st.info("Standard mode: dataset body is not editable. To edit, enable Snowflake Migration Mode in the sidebar.")

        # quick actions for single dataset
        a_col1, a_col2, a_col3 = st.columns([1,1,1])
        if a_col1.button("📤 Upsert this dataset", disabled=not st.session_state.get("destination_token")):
            payload_to_send = load_local_dataset(selected) or payload
            try:
                res = upsert_dataset(st.session_state.destination_token, st.session_state.base_url, selected, payload_to_send)
                st.success(f"Upsert succeeded for {selected}")
                st.json(res)
            except Exception as e:
                st.error(f"Upsert failed: {e}")

        if a_col2.button("🗑 Delete this dataset on destination", disabled=not st.session_state.get("destination_token")):
            try:
                res = delete_dataset(st.session_state.destination_token, st.session_state.base_url, selected)
                st.success(f"Deleted {selected} on destination.")
                st.json(res)
            except Exception as e:
                st.error(f"Delete failed: {e}")

        if a_col3.button("📥 Re-fetch live from source"):
            try:
                payload_live = fetch_dataset(st.session_state.source_token, st.session_state.base_url, selected)
                save_local_dataset(selected, payload_live)
                st.success("Refreshed local copy from source.")
                st.experimental_rerun()
            except Exception as e:
                st.error(f"Fetch failed: {e}")

# -------------------------
# Footer: quick diagnostics
# -------------------------
st.markdown("---")
st.subheader("Diagnostics & Local files")
st.write("Local files directory:", os.path.abspath(OUTPUT_DIR))
if st.session_state.local_files:
    st.write("Locally saved datasets:")
    for k, v in st.session_state.local_files.items():
        st.write(f"- {k}: {v}")
else:
    st.write("No local dataset files saved yet.")

st.caption("Notes: This app tries common endpoints for authentication and CRUD. If your API uses different paths or auth response formats, adjust the endpoint paths in the helper functions (try_auth_endpoints, fetch_dataset_codes, fetch_dataset, upsert_dataset).")
