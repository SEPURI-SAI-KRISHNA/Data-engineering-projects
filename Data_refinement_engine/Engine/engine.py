import streamlit as st
import pandas as pd
import json
import sys
import importlib
from ai_engine import AIEngine

# Reload to ensure updates (if AI writes new code)
import transformations, validations, derivations

# ==========================================
# SAFE RELOAD MECHANISM
# ==========================================
# 1. Force Python to check the disk for file changes
importlib.invalidate_caches()

# 2. Reload modules to ensure new AI functions appear immediately
#    We wrap this in try/except to prevent the KeyError crash on Python 3.12
try:
    if 'transformations' in sys.modules:
        importlib.reload(transformations)
    if 'validations' in sys.modules:
        importlib.reload(validations)
    if 'derivations' in sys.modules:
        importlib.reload(derivations)
except (KeyError, ImportError, AttributeError):
    # If reload fails slightly during the write-process, it's okay.
    # The next script run will catch it.
    pass

# ==========================================
# 0. INTERNAL CONFIGURATION (SECRETS)
# ==========================================
# ⚠️ SET YOUR CREDENTIALS HERE INTERNALLY
INTERNAL_API_KEY = ""  # <--- REPLACE WITH YOUR ACTUAL API KEY
INTERNAL_BASE_URL = ""  # Change if using local LLM or different provider

# ==========================================
# 1. THE REGISTRY (Dynamic)
# ==========================================
FUNCTION_REGISTRY = AIEngine.scan_registry()


# ==========================================
# 2. THE ENGINE (Logic Execution)
# ==========================================
def apply_mapping_to_record(raw_record, mapping_config):
    rich_record = {}

    for raw_key, value in raw_record.items():
        config = mapping_config.get(raw_key)
        if not config or config["is_key_ignored"]:
            continue

        target_key = config["rich_key"]

        # 1. Apply Transformations
        current_value = value
        for step in config["transformation"]:
            current_value = AIEngine.execute_function("transformation", step["name"], current_value,
                                                      step["parameter_to_function"])

        rich_record[target_key] = current_value

        # 2. Apply Derivations
        for step in config["derivation"]:
            derived_val = AIEngine.execute_function("derivation", step["name"], current_value,
                                                    step["parameter_to_function"])
            new_key = step["parameter_to_function"].get("target_field_name", f"{target_key}_derived")
            rich_record[new_key] = derived_val

    return rich_record


# ==========================================
# 3. UI HELPER: DYNAMIC ITEM BUILDER
# ==========================================
# ... (Imports and Config remain same)

# ==========================================
# 3. UI HELPER: DYNAMIC ITEM BUILDER
# ==========================================
# ==========================================
# 3. UI HELPER: DYNAMIC ITEM BUILDER
# ==========================================
# ==========================================
# 3. UI HELPER: DYNAMIC ITEM BUILDER
# ==========================================
# ==========================================
# 3. UI HELPER: DYNAMIC ITEM BUILDER
# ==========================================
def render_step_manager(step_type, config_list):
    current_registry = AIEngine.scan_registry()
    available_funcs = current_registry[step_type]

    # --- THINKING BOX ---
    with st.expander(f"🧠 AI Thinking Box ({step_type})", expanded=False):

        # State keys for this specific step type
        query_key = f"ai_q_{step_type}"
        confirm_key = f"confirm_gen_{step_type}"
        code_key = f"generated_code_{step_type}"

        # --- CALLBACK TO RESET STATE ---
        # This runs BEFORE the script reruns, preventing the API Error
        def clear_ai_state():
            st.session_state[query_key] = ""
            if confirm_key in st.session_state: del st.session_state[confirm_key]
            if code_key in st.session_state: del st.session_state[code_key]

        # Layout: Input Box (Wide) + Refresh Button (Small)
        col_input, col_reset = st.columns([8, 1])

        with col_input:
            user_query = st.text_input(f"Describe what you want to do...", key=query_key)

        with col_reset:
            st.write("")
            st.write("")
            # USE ON_CLICK HERE - This is the fix
            st.button("🔄", key=f"reset_{step_type}", on_click=clear_ai_state, help="Clear input and reset AI")

        # Action Buttons Area
        c1, c2 = st.columns([1, 4])

        # 1. THE SEARCH BUTTON
        with c1:
            ask_clicked = st.button("Ask AI", key=f"ai_btn_{step_type}")

            if ask_clicked:
                if not user_query:
                    st.warning("⚠️ Please describe a task first.")
                elif not INTERNAL_API_KEY or "sk-..." in INTERNAL_API_KEY:
                    st.error("❌ Internal Key not set")
                else:
                    with st.spinner("Analyzing Library & Intent..."):
                        # Reset previous states
                        st.session_state[confirm_key] = False
                        if code_key in st.session_state: del st.session_state[code_key]

                        # Call Engine
                        result = AIEngine.ask_llm_for_function(user_query, step_type, INTERNAL_API_KEY,
                                                               INTERNAL_BASE_URL, force_generation=False)

                        if result["status"] == "found":
                            st.success(f"Found match: `{result['function_name']}`")
                        elif result["status"] == "confirmation_needed":
                            st.warning("No existing function found.")
                            st.session_state[confirm_key] = True
                        elif result["status"] == "error":
                            st.error(result['message'])

        # 2. THE CONFIRMATION BUTTON (Only appears if no match found)
        with c2:
            if st.session_state.get(confirm_key, False):
                st.write("Generate new code?")
                if st.button("🚀 Yes, Generate", key=f"force_gen_{step_type}"):
                    with st.spinner("Generating Code..."):
                        result = AIEngine.ask_llm_for_function(user_query, step_type, INTERNAL_API_KEY,
                                                               INTERNAL_BASE_URL, force_generation=True)

                        if result["status"] == "generated":
                            st.session_state[code_key] = result["code"]
                            st.session_state[confirm_key] = False
                            st.rerun()

        # 3. CODE PREVIEW & SAVE
        if st.session_state.get(code_key):
            st.info("Proposed Logic:")
            st.code(st.session_state[code_key], language="python")

            if st.button("✅ Approve & Save", key=f"save_{step_type}"):
                success = AIEngine.append_function_to_file(step_type, st.session_state[code_key])
                if success:
                    st.success("Saved! Reloading...")
                    del st.session_state[code_key]
                    st.rerun()
                else:
                    st.error("Write failed.")

    # --- ADD NEW STEP FORM (Standard UI) ---
    with st.container(border=True):
        c1, c2 = st.columns([3, 1])
        with c1:
            selected_func = st.selectbox(f"Select Function", list(available_funcs.keys()), key=f"sel_{step_type}")
        with c2:
            st.write("")

        required_params = available_funcs[selected_func]
        captured_params = {}

        if required_params:
            cols = st.columns(len(required_params))
            for i, param in enumerate(required_params):
                with cols[i]:
                    captured_params[param] = st.text_input(f"{param}", key=f"in_{step_type}_{param}")

        if st.button(f"Add Step", key=f"btn_add_{step_type}", use_container_width=True):
            new_step = {
                "name": selected_func,
                "parameter_to_function": captured_params
            }
            config_list.append(new_step)
            st.rerun()

    # --- LIST & REMOVE ---
    if config_list:
        st.caption(f"Active {step_type} sequence:")
        for i, step in enumerate(config_list):
            c1, c2 = st.columns([5, 1])
            with c1:
                params_txt = f"({step['parameter_to_function']})" if step['parameter_to_function'] else ""
                st.code(f"{i + 1}. {step['name']} {params_txt}", language="bash")
            with c2:
                if st.button("🗑️", key=f"del_{step_type}_{i}"):
                    config_list.pop(i)
                    st.rerun()


# ==========================================
# 4. DATA LOADER & SETUP
# ==========================================
# ==========================================
# 4. DATA LOADER & SETUP
# ==========================================
st.set_page_config(layout="wide", page_title="Schema Builder Pro")

# --- SIDEBAR: DATA SOURCE ONLY ---
st.sidebar.title("📁 Data Source")
uploaded_file = st.sidebar.file_uploader("Upload Raw Data", type=["csv", "json"])


def load_data(file):
    # Reset file pointer to beginning (crucial for re-reads)
    file.seek(0)
    if file.name.endswith('.csv'):
        df = pd.read_csv(file)
        return df.to_dict(orient='records')
    elif file.name.endswith('.json'):
        content = json.load(file)
        if isinstance(content, list):
            return content
        elif isinstance(content, dict):
            return [content]
    return []


# Initialize default dataset if completely empty
if 'dataset' not in st.session_state:
    st.session_state.dataset = [
        {"customer_name": "  JAMES BOND_007 ", "email": "James.Bond@mi6.uk", "salary": 50000.556,
         "region_id": "eu-west"}]

# --- LOGIC FIX: Only reload if file changed ---
if uploaded_file:
    # check if we already loaded this specific file
    if 'current_loaded_file' not in st.session_state or st.session_state.current_loaded_file != uploaded_file.name:
        data = load_data(uploaded_file)
        if data:
            st.session_state.dataset = data
            st.session_state.current_loaded_file = uploaded_file.name

            # Reset selection ONLY when a NEW file is loaded
            if 'selected_field' in st.session_state:
                del st.session_state['selected_field']

            # Reset config mapping for the new file (optional, but safer)
            st.session_state.mapping_config = {}
            st.rerun()

raw_record_sample = st.session_state.dataset[0]

if 'mapping_config' not in st.session_state:
    st.session_state.mapping_config = {}

for key in raw_record_sample:
    if key not in st.session_state.mapping_config:
        st.session_state.mapping_config[key] = {"is_key_ignored": False, "rich_key": key, "rich_key_type": "string",
                                                "transformation": [], "validation": [], "derivation": []}

if 'selected_field' not in st.session_state:
    st.session_state.selected_field = list(raw_record_sample.keys())[0]

# Safety check: Ensure selected field actually exists in current record
# (Fixes crash if switching from a file with 'id' to a file without 'id')
if st.session_state.selected_field not in raw_record_sample:
    st.session_state.selected_field = list(raw_record_sample.keys())[0]

# ==========================================
# 5. MAIN UI LAYOUT
# ==========================================

st.title("🧩 Advanced Schema Builder")
st.markdown(f"**Current Dataset:** {len(st.session_state.dataset)} record(s). Configuring **Row #1**.")
st.divider()

col_left, col_mid, col_right = st.columns([1, 1.8, 1.2])

# === LEFT: FIELD SELECTOR (SCROLLABLE) ===
with col_left:
    st.subheader("Raw Record")
    with st.container(height=500, border=True):
        for key in raw_record_sample:
            ignored = st.session_state.mapping_config[key]["is_key_ignored"]
            label = f"🚫 {key}" if ignored else f"✅ {key}"
            type_btn = "primary" if st.session_state.selected_field == key else "secondary"
            if st.button(label, key=f"btn_{key}", use_container_width=True, type=type_btn):
                st.session_state.selected_field = key
                st.rerun()

# === MIDDLE: LOGIC CONFIGURATION (SCROLLABLE) ===
with col_mid:
    field = st.session_state.selected_field
    config = st.session_state.mapping_config[field]

    st.subheader(f"Edit Logic: `{field}`")

    with st.container(height=500, border=True):
        # Meta Controls
        with st.expander("Field Settings", expanded=True):
            c1, c2, c3 = st.columns([1, 2, 1])
            with c1:
                if st.toggle("Ignore", value=config["is_key_ignored"]):
                    config["is_key_ignored"] = True
                else:
                    config["is_key_ignored"] = False
            with c2:
                config["rich_key"] = st.text_input("Renamed", value=config["rich_key"],
                                                   disabled=config["is_key_ignored"])
            with c3:
                config["rich_key_type"] = st.selectbox("Type", ["string", "int", "float", "bool"],
                                                       disabled=config["is_key_ignored"])

        if not config["is_key_ignored"]:
            tab_t, tab_v, tab_d = st.tabs(["⚡ Transform", "🛡️ Validate", "⚗️ Derive"])
            with tab_t:
                render_step_manager("transformation", config["transformation"])
            with tab_v:
                render_step_manager("validation", config["validation"])
            with tab_d:
                render_step_manager("derivation", config["derivation"])
        else:
            st.warning("Field is ignored.")

# === RIGHT: OUTPUT & JSON (SCROLLABLE) ===
with col_right:
    st.subheader("Results")

    rich_data_sample = apply_mapping_to_record(raw_record_sample, st.session_state.mapping_config)
    final_json = {"mapping": st.session_state.mapping_config}

    view_tab1, view_tab2 = st.tabs(["👁️ Data Preview", "⚙️ Schema JSON"])

    with view_tab1:
        with st.container(height=450, border=True):
            st.success("Refined Record")
            st.json(rich_data_sample)

    with view_tab2:
        with st.container(height=450, border=True):
            st.info("Config JSON")
            st.json(final_json)

    st.download_button("📥 Download Schema", data=json.dumps(final_json, indent=2), file_name="schema.json",
                       mime="application/json", use_container_width=True)

# ==========================================
# 6. BATCH PROCESSING
# ==========================================
st.markdown("---")
st.header("🚀 Batch Processing")
if st.button("Run Transformation on All Records", type="primary"):
    processed_data = []
    progress_bar = st.progress(0)
    total_records = len(st.session_state.dataset)
    for i, record in enumerate(st.session_state.dataset):
        processed_data.append(apply_mapping_to_record(record, st.session_state.mapping_config))
        progress_bar.progress((i + 1) / total_records)

    st.success(f"Processed {total_records} records!")
    st.dataframe(pd.DataFrame(processed_data).head())
    st.download_button("Download JSON", data=json.dumps(processed_data, indent=2), file_name="refined_data.json")