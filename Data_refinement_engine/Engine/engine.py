import importlib
import json
import os

import pandas as pd
import streamlit as st

import transformations
import validations
import derivations
import processor

# The AI can append new functions to the registry files while the app is
# running, so pick up disk changes on every rerun. processor goes last since
# it holds references to the registry classes.
importlib.invalidate_caches()
try:
    importlib.reload(transformations)
    importlib.reload(validations)
    importlib.reload(derivations)
    importlib.reload(processor)
except (KeyError, ImportError, AttributeError):
    pass  # mid-write race; the next rerun picks it up

from ai_engine import AIEngine

API_KEY = os.environ.get("OPENAI_API_KEY", "")
BASE_URL = os.environ.get("OPENAI_BASE_URL") or None

FUNCTION_REGISTRY = AIEngine.scan_registry()


def render_step_manager(step_type, config_list):
    current_registry = AIEngine.scan_registry()
    available_funcs = current_registry[step_type]

    with st.expander(f"🧠 AI Thinking Box ({step_type})", expanded=False):
        query_key = f"ai_q_{step_type}"
        confirm_key = f"confirm_gen_{step_type}"
        code_key = f"generated_code_{step_type}"

        # on_click callback so the reset happens before the rerun
        def clear_ai_state():
            st.session_state[query_key] = ""
            st.session_state.pop(confirm_key, None)
            st.session_state.pop(code_key, None)

        col_input, col_reset = st.columns([8, 1])
        with col_input:
            user_query = st.text_input("Describe what you want to do...", key=query_key)
        with col_reset:
            st.write("")
            st.write("")
            st.button("🔄", key=f"reset_{step_type}", on_click=clear_ai_state,
                      help="Clear input and reset AI")

        c1, c2 = st.columns([1, 4])

        with c1:
            if st.button("Ask AI", key=f"ai_btn_{step_type}"):
                if not user_query:
                    st.warning("Describe a task first.")
                elif not API_KEY:
                    st.error("Set the OPENAI_API_KEY environment variable.")
                else:
                    with st.spinner("Analyzing library & intent..."):
                        st.session_state[confirm_key] = False
                        st.session_state.pop(code_key, None)

                        result = AIEngine.ask_llm_for_function(
                            user_query, step_type, API_KEY, BASE_URL,
                            force_generation=False)

                        if result["status"] == "found":
                            st.success(f"Found match: `{result['function_name']}`")
                        elif result["status"] == "confirmation_needed":
                            st.warning("No existing function found.")
                            st.session_state[confirm_key] = True
                        elif result["status"] == "error":
                            st.error(result["message"])

        with c2:
            if st.session_state.get(confirm_key, False):
                st.write("Generate new code?")
                if st.button("🚀 Yes, Generate", key=f"force_gen_{step_type}"):
                    with st.spinner("Generating code..."):
                        result = AIEngine.ask_llm_for_function(
                            user_query, step_type, API_KEY, BASE_URL,
                            force_generation=True)
                        if result["status"] == "generated":
                            st.session_state[code_key] = result["code"]
                            st.session_state[confirm_key] = False
                            st.rerun()

        if st.session_state.get(code_key):
            st.info("Proposed logic:")
            st.code(st.session_state[code_key], language="python")

            if st.button("✅ Approve & Save", key=f"save_{step_type}"):
                if AIEngine.append_function_to_file(step_type, st.session_state[code_key]):
                    st.success("Saved! Reloading...")
                    del st.session_state[code_key]
                    st.rerun()
                else:
                    st.error("Write failed.")

    with st.container(border=True):
        c1, c2 = st.columns([3, 1])
        with c1:
            selected_func = st.selectbox("Select Function", list(available_funcs.keys()),
                                         key=f"sel_{step_type}")
        with c2:
            st.write("")

        required_params = available_funcs[selected_func]
        captured_params = {}
        if required_params:
            cols = st.columns(len(required_params))
            for i, param in enumerate(required_params):
                with cols[i]:
                    captured_params[param] = st.text_input(param, key=f"in_{step_type}_{param}")

        if st.button("Add Step", key=f"btn_add_{step_type}", use_container_width=True):
            config_list.append({"name": selected_func,
                                "parameter_to_function": captured_params})
            st.rerun()

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


st.set_page_config(layout="wide", page_title="Schema Builder")

st.sidebar.title("📁 Data Source")
uploaded_file = st.sidebar.file_uploader("Upload Raw Data", type=["csv", "json"])


def load_data(file):
    file.seek(0)
    if file.name.endswith('.csv'):
        return pd.read_csv(file).to_dict(orient='records')
    if file.name.endswith('.json'):
        content = json.load(file)
        if isinstance(content, list):
            return content
        if isinstance(content, dict):
            return [content]
    return []


if 'dataset' not in st.session_state:
    st.session_state.dataset = [
        {"customer_name": "  JAMES BOND_007 ", "email": "James.Bond@mi6.uk",
         "salary": 50000.556, "region_id": "eu-west"}]

# only reload when a different file is uploaded, otherwise every rerun
# would wipe the mapping config
if uploaded_file:
    if st.session_state.get('current_loaded_file') != uploaded_file.name:
        data = load_data(uploaded_file)
        if data:
            st.session_state.dataset = data
            st.session_state.current_loaded_file = uploaded_file.name
            st.session_state.pop('selected_field', None)
            st.session_state.mapping_config = {}
            st.rerun()

raw_record_sample = st.session_state.dataset[0]

if 'mapping_config' not in st.session_state:
    st.session_state.mapping_config = {}

for key in raw_record_sample:
    if key not in st.session_state.mapping_config:
        st.session_state.mapping_config[key] = {
            "is_key_ignored": False, "rich_key": key, "rich_key_type": "string",
            "transformation": [], "validation": [], "derivation": []}

if st.session_state.get('selected_field') not in raw_record_sample:
    st.session_state.selected_field = list(raw_record_sample.keys())[0]

st.title("🧩 Schema Builder")
st.markdown(f"**Current dataset:** {len(st.session_state.dataset)} record(s). Configuring **row #1**.")
st.divider()

col_left, col_mid, col_right = st.columns([1, 1.8, 1.2])

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

with col_mid:
    field = st.session_state.selected_field
    config = st.session_state.mapping_config[field]

    st.subheader(f"Edit Logic: `{field}`")

    with st.container(height=500, border=True):
        with st.expander("Field Settings", expanded=True):
            c1, c2, c3 = st.columns([1, 2, 1])
            with c1:
                config["is_key_ignored"] = st.toggle("Ignore", value=config["is_key_ignored"])
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

with col_right:
    st.subheader("Results")

    rich_data_sample, sample_errors = processor.apply_mapping_to_record(
        raw_record_sample, st.session_state.mapping_config)
    final_json = {"mapping": st.session_state.mapping_config}

    view_tab1, view_tab2 = st.tabs(["👁️ Data Preview", "⚙️ Schema JSON"])

    with view_tab1:
        with st.container(height=450, border=True):
            if sample_errors:
                st.error(f"{len(sample_errors)} issue(s) on this record")
                for err in sample_errors:
                    st.caption(f"`{err['field']}` · {err['category']}/{err['step']}: {err['error']}")
            else:
                st.success("Refined Record")
            st.json(rich_data_sample)

    with view_tab2:
        with st.container(height=450, border=True):
            st.info("Config JSON")
            st.json(final_json)

    st.download_button("📥 Download Schema", data=json.dumps(final_json, indent=2),
                       file_name="schema.json", mime="application/json",
                       use_container_width=True)

st.markdown("---")
st.header("🚀 Batch Processing")
if st.button("Run Transformation on All Records", type="primary"):
    processed_data = []
    all_errors = []
    progress_bar = st.progress(0)
    total_records = len(st.session_state.dataset)

    for i, record in enumerate(st.session_state.dataset):
        result, errors = processor.apply_mapping_to_record(record, st.session_state.mapping_config)
        processed_data.append(result)
        for err in errors:
            all_errors.append({"record": i, **err})
        progress_bar.progress((i + 1) / total_records)

    if all_errors:
        st.warning(f"Processed {total_records} records with {len(all_errors)} issue(s).")
        st.dataframe(pd.DataFrame(all_errors), use_container_width=True)
    else:
        st.success(f"Processed {total_records} records, no issues.")

    st.dataframe(pd.DataFrame(processed_data).head())
    st.download_button("Download JSON", data=json.dumps(processed_data, indent=2),
                       file_name="refined_data.json")
