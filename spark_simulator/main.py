import streamlit as st
import streamlit.components.v1 as components
import json
import time
import random
import pandas as pd
import numpy as np


# ==========================================
# 1. BACKEND LOGIC
# ==========================================

class Executor:
    def __init__(self, id):
        self.id = id
        self.data = pd.DataFrame(columns=["id", "category", "value"])
        self.status = "ALIVE"  # ALIVE, DEAD, STRAGGLER, OOM
        self.max_rows = 50  # Default Memory Limit
        self.speed = 1.0  # 1.0 = Normal, 0.2 = Straggler
        self.backup_data = pd.DataFrame()  # Hidden "Lineage" for recovery simulation

    def is_active(self):
        return self.status in ["ALIVE", "STRAGGLER"]

    def receive_data(self, new_df, append=True):
        if self.status == "DEAD": return False

        # Memory Check
        current_rows = len(self.data) if append else 0
        if current_rows + len(new_df) > self.max_rows:
            self.status = "OOM"
            self.data = pd.DataFrame(columns=["id", "category", "value"])
            return False

        if append:
            self.data = pd.concat([self.data, new_df], ignore_index=True)
        else:
            self.data = new_df

        # Update Backup (Simulating Source/Lineage)
        self.backup_data = self.data.copy()
        return True

    def recover(self):
        """Restores state from lineage (backup)"""
        if self.status in ["DEAD", "OOM"]:
            self.status = "ALIVE"
            # Simulate re-computation: Restore data
            self.data = self.backup_data.copy()
            return True
        return False

    def get_color(self):
        if self.status == "DEAD": return "#444444"  # Gray
        if self.status == "OOM": return "#FF4444"  # Red
        if self.status == "STRAGGLER": return "#FFA500"  # Orange
        return "#97C2FC"  # Blue

    def get_label(self):
        base = f"EXECUTOR {self.id}"
        if self.status == "DEAD": return f"{base}\n(CRASHED)"
        if self.status == "OOM": return f"{base}\n(OOM)"
        if self.status == "STRAGGLER": return f"{base}\n(SLOW IO)"
        return base


class Cluster:
    def __init__(self, num_executors=3):
        self.executors = {i: Executor(i) for i in range(1, num_executors + 1)}


class Driver:
    def __init__(self):
        self.cluster = Cluster(num_executors=3)
        self.logs = []
        self.memory = pd.DataFrame(columns=["id", "category", "value"])

    def log(self, message):
        timestamp = time.strftime("%H:%M:%S")
        self.logs.append(f"[{timestamp}] {message}")

    def run_job(self, operation, **kwargs):
        events = []
        active_nodes = [ex for ex in self.cluster.executors.values() if ex.is_active()]

        # --- SYSTEM OPS ---
        if operation == "kill":
            target = kwargs.get("target")
            self.cluster.executors[target].status = "DEAD"
            self.cluster.executors[target].data = pd.DataFrame()
            self.log(f"CHAOS: Executor {target} killed. Data Lost.")
            return []

        elif operation == "straggler":
            target = kwargs.get("target")
            self.cluster.executors[target].status = "STRAGGLER"
            self.cluster.executors[target].speed = 0.2
            self.log(f"PERFORMANCE: Executor {target} is now a Straggler (Slow Network/CPU).")
            return []

        elif operation == "set_limit":
            limit = kwargs.get("limit")
            for ex in self.cluster.executors.values(): ex.max_rows = limit
            self.log(f"CONFIG: Max RAM set to {limit} rows/node.")
            return []

        elif operation == "recover":
            self.log("SYSTEM: Triggering Lineage Re-computation...")
            restored_count = 0
            for ex in self.cluster.executors.values():
                if ex.recover():
                    restored_count += 1
                    # Visual: Driver -> Ex (Simulate re-sending task)
                    events.append({"from": 0, "to": ex.id, "label": "Recompute", "delay": ex.id * 300})

            if restored_count > 0:
                self.log(f"SUCCESS: {restored_count} nodes recovered and data restored.")
            else:
                self.log("INFO: No dead nodes to recover.")
            return events

        # --- DATA OPS ---

        # 1. LOAD
        elif operation == "load":
            source = kwargs.get("source", "random")

            if source == "custom":
                raw_data = kwargs.get("dataframe")
                self.log(f"Job: Load Custom CSV ({len(raw_data)} rows)")
            else:
                count = kwargs.get("count", 20)
                raw_data = pd.DataFrame({
                    "id": range(1, count + 1),
                    "category": [random.choice(['A', 'B', 'C']) for _ in range(count)],
                    "value": np.random.randint(10, 100, size=count)
                })
                self.log(f"Job: Load Random Data ({count} rows)")

            # Reset Cluster Data
            for ex in self.cluster.executors.values(): ex.data = pd.DataFrame()

            chunks = np.array_split(raw_data, len(active_nodes)) if active_nodes else []
            for i, ex in enumerate(active_nodes):
                chunk = chunks[i]
                success = ex.receive_data(chunk, append=False)

                # Delay Calculation: Slower if Straggler
                delay = i * 500 if ex.status != "STRAGGLER" else i * 2000

                if success:
                    events.append({"from": 0, "to": ex.id, "label": f"{len(chunk)} rows", "delay": delay})
                else:
                    self.log(f"CRITICAL: Executor {ex.id} OOM on Load!")
                    events.append({"from": 0, "to": ex.id, "label": "CRASH", "delay": delay})

        # 2. FILTER
        elif operation == "filter":
            col = kwargs.get("column")
            cond = kwargs.get("condition")
            val = kwargs.get("value")
            self.log(f"Job: Filter ({col} {cond} {val})")

            for ex in active_nodes:
                if ex.data.empty: continue
                if cond == "==":
                    filtered = ex.data[ex.data[col] == val]
                elif cond == ">":
                    filtered = ex.data[ex.data[col] > float(val)]
                elif cond == "<":
                    filtered = ex.data[ex.data[col] < float(val)]
                ex.receive_data(filtered, append=False)

        # 3. REPARTITION
        elif operation == "repartition":
            method = kwargs.get("method", "Random")
            col = kwargs.get("column", None)
            skew_target = kwargs.get("skew_target", None)

            log_msg = f"Job: Repartition ({method})"
            if method == "Hash": log_msg += f" by {col}"
            if skew_target: log_msg += f" [SKEW -> Ex {skew_target}]"
            self.log(log_msg)

            new_buffers = {id: [] for id in self.cluster.executors}

            for sender in active_nodes:
                if sender.data.empty: continue
                for _, row in sender.data.iterrows():
                    # Target Logic
                    if skew_target:
                        # 90% chance to go to skew target
                        if random.random() < 0.9:
                            target_id = skew_target
                        else:
                            target_id = random.choice([e.id for e in active_nodes])
                    elif method == "Hash" and col:
                        # Hash Partitioning
                        target_id = (sum(ord(c) for c in str(row[col])) % 3) + 1
                    else:
                        # Random
                        target_id = random.choice([e.id for e in active_nodes])

                    # Safety check if target is alive
                    if target_id not in [n.id for n in active_nodes]: target_id = sender.id

                    new_buffers[target_id].append(row)

                    if sender.id != target_id:
                        # Animation Speed Logic
                        delay = random.randint(0, 1000)
                        if sender.status == "STRAGGLER": delay += 2000  # Slower sender
                        events.append({"from": sender.id, "to": target_id, "label": ".", "delay": delay})

            # Apply
            for ex_id, buffer in new_buffers.items():
                target_ex = self.cluster.executors[ex_id]
                if not target_ex.is_active(): continue
                new_df = pd.DataFrame(buffer) if buffer else pd.DataFrame(columns=["id", "category", "value"])
                target_ex.receive_data(new_df, append=False)

        # 4. GROUP BY
        elif operation == "groupby":
            col = kwargs.get("column")
            agg = kwargs.get("agg")
            self.log(f"Job: GroupBy {col} ({agg})")

            # Shuffle Phase
            new_buffers = {id: [] for id in self.cluster.executors}
            map_target = {'A': 1, 'B': 2, 'C': 3}  # Simple hash map for demo

            for sender in active_nodes:
                if sender.data.empty: continue
                for _, row in sender.data.iterrows():
                    val = row.get(col)
                    # Attempt to route A->1, B->2 etc, fall back to random if key unknown
                    target_id = map_target.get(val, random.choice([e.id for e in active_nodes]))
                    if target_id in new_buffers:
                        new_buffers[target_id].append(row)
                        if sender.id != target_id:
                            events.append({"from": sender.id, "to": target_id, "label": str(val),
                                           "delay": random.randint(0, 800)})

            # Aggregation Phase
            for ex_id, buffer in new_buffers.items():
                target_ex = self.cluster.executors[ex_id]
                if not target_ex.is_active(): continue

                if buffer:
                    df_temp = pd.DataFrame(buffer)
                    if agg == "count":
                        res = df_temp.groupby(col).size().reset_index(name='count')
                    elif agg == "sum":
                        res = df_temp.groupby(col)['value'].sum().reset_index()
                    elif agg == "avg":
                        res = df_temp.groupby(col)['value'].mean().reset_index()
                    elif agg == "min":
                        res = df_temp.groupby(col)['value'].min().reset_index()
                    elif agg == "max":
                        res = df_temp.groupby(col)['value'].max().reset_index()
                    target_ex.receive_data(res, append=False)
                else:
                    target_ex.receive_data(pd.DataFrame(), append=False)

        # 5. COALESCE
        elif operation == "coalesce":
            target_num = kwargs.get("target_num", 2)
            self.log(f"Job: Coalesce (Shrink to {target_num} partitions)")

            # Logic: We simply empty the higher ID executors into the lower ones
            # Ex 3 -> Ex 1/2
            sender = self.cluster.executors[3]
            if sender.is_active() and not sender.data.empty:
                # Divide data among remaining
                chunks = np.array_split(sender.data, target_num)

                for i in range(target_num):
                    receiver_id = i + 1
                    if self.cluster.executors[receiver_id].is_active():
                        self.cluster.executors[receiver_id].receive_data(chunks[i], append=True)
                        events.append({"from": 3, "to": receiver_id, "label": "Merge", "delay": i * 500})

                sender.data = pd.DataFrame()

        # 6. ORDER BY (Sort)
        elif operation == "orderby":
            col = kwargs.get("column")
            ascending = kwargs.get("ascending")
            self.log(f"Job: OrderBy {col} (Global Sort)")

            # 1. Collect all valid data
            all_dfs = []
            for ex in active_nodes:
                if not ex.data.empty: all_dfs.append(ex.data)

            if not all_dfs: return []

            full_df = pd.concat(all_dfs).sort_values(by=col, ascending=ascending)

            # 2. Redistribute (Range Partitioning)
            chunks = np.array_split(full_df, len(active_nodes))

            for i, ex in enumerate(active_nodes):
                ex.receive_data(chunks[i], append=False)
                # Visual: Range shuffle
                events.append({"from": 1, "to": ex.id, "label": "Range", "delay": i * 400})

        # 7. COLLECT
        elif operation == "collect":
            self.log("Job: Collect (Fetch to Driver)")
            self.memory = pd.DataFrame()
            collected = []
            for ex in active_nodes:
                if not ex.data.empty:
                    collected.append(ex.data)
                    delay = ex.id * 400
                    events.append({"from": ex.id, "to": 0, "label": f"{len(ex.data)}", "delay": delay})

            if collected:
                self.memory = pd.concat(collected, ignore_index=True)
                self.log(f"Result: Driver received {len(self.memory)} rows")

        return events


# ==========================================
# 2. STREAMLIT SETUP
# ==========================================
st.set_page_config(layout="wide", page_title="Ultimate Spark Simulator")

if 'driver' not in st.session_state:
    st.session_state['driver'] = Driver()
    st.session_state['last_events'] = []

driver = st.session_state['driver']
st.title("⚡ Ultimate Spark Simulator")

# ==========================================
# 3. VISUALIZATION
# ==========================================
nodes_data = [{'id': 0, 'label': 'DRIVER', 'shape': 'ellipse', 'color': '#FFD700', 'size': 35}]
for ex in driver.cluster.executors.values():
    nodes_data.append({
        'id': ex.id, 'label': ex.get_label(), 'shape': 'box', 'color': ex.get_color(), 'size': 30
    })

vis_graph_html = f"""
<!DOCTYPE html>
<html>
<head>
  <script type="text/javascript" src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
  <style>#mynetwork {{width: 100%; height: 350px; border: 1px solid #ddd; background: white;}}</style>
</head>
<body>
<div id="mynetwork"></div>
<script type="text/javascript">
  var nodes = new vis.DataSet({json.dumps(nodes_data)});
  var edges = new vis.DataSet([
    {{from: 0, to: 1, arrows: 'to'}}, {{from: 0, to: 2, arrows: 'to'}}, {{from: 0, to: 3, arrows: 'to'}},
    {{from: 1, to: 2, dashes: true, color: '#eee'}}, {{from: 2, to: 3, dashes: true, color: '#eee'}}, {{from: 1, to: 3, dashes: true, color: '#eee'}}
  ]);
  var container = document.getElementById('mynetwork');
  var network = new vis.Network(container, {{ nodes: nodes, edges: edges }}, {{
    physics: {{ enabled: false }},
    layout: {{ hierarchical: {{ direction: 'UD', sortMethod: 'directed', levelSeparation: 120 }} }}
  }});

  function animateTraffic(start, end, label, delay) {{
    setTimeout(() => {{
        var sPos = network.getPositions([start])[start];
        var ePos = network.getPositions([end])[end];
        if(!sPos || !ePos) return; 

        var pId = 'p' + Math.random();
        nodes.add({{id: pId, shape: 'circle', label: label, size: 6, color: 'red', font:{{size:8, color:'black'}}, x: sPos.x, y: sPos.y, fixed: true}});

        var progress = 0;
        function step() {{
          progress += 0.02;
          if (progress >= 1) {{ nodes.remove(pId); return; }}
          nodes.update({{id: pId, x: sPos.x + (ePos.x - sPos.x)*progress, y: sPos.y + (ePos.y - sPos.y)*progress}});
          requestAnimationFrame(step);
        }}
        requestAnimationFrame(step);
    }}, delay);
  }}

  const events = {{events_json}};
  events.forEach(e => animateTraffic(e.from, e.to, e.label, e.delay));
</script>
</body>
</html>
"""
events_json = json.dumps(st.session_state['last_events'])
components.html(vis_graph_html.replace("{events_json}", events_json), height=370)

# ==========================================
# 4. MAIN CONTROLS (TABS)
# ==========================================
st.divider()
tabs = st.tabs(["🎮 Transformation Control", "⚡ Chaos & Tuning", "📜 Logs"])

with tabs[0]:  # OPERATIONS
    col_op = st.selectbox("Select Transformation",
                          ["Load Data", "Filter", "Repartition", "GroupBy", "OrderBy", "Coalesce", "Collect"])

    st.markdown("---")

    # --- LOAD ---
    if col_op == "Load Data":
        load_source = st.radio("Source", ["Random Generation", "Upload CSV"])
        if load_source == "Upload CSV":
            up_file = st.file_uploader("Upload CSV", type=['csv'])
            if st.button("Load File") and up_file:
                df = pd.read_csv(up_file)
                st.session_state['last_events'] = driver.run_job("load", source="custom", dataframe=df)
                st.rerun()
        else:
            rows = st.slider("Number of Rows", 10, 100, 30)
            if st.button("Generate & Load"):
                st.session_state['last_events'] = driver.run_job("load", count=rows)
                st.rerun()

    # --- REPARTITION ---
    elif col_op == "Repartition":
        c1, c2 = st.columns(2)
        with c1:
            method = st.radio("Method", ["Random (Round Robin)", "Hash (By Column)"])
        with c2:
            r_col = st.selectbox("Column", ["category", "id", "value"]) if "Hash" in method else None

        skew = st.checkbox("Simulate Data Skew (Force data to one node)")
        skew_target = st.selectbox("Skew Target Node", [1, 2, 3]) if skew else None

        if st.button("Run Shuffle"):
            st.session_state['last_events'] = driver.run_job("repartition",
                                                             method=("Hash" if "Hash" in method else "Random"),
                                                             column=r_col, skew_target=skew_target)
            st.rerun()

    # --- GROUP BY ---
    elif col_op == "GroupBy":
        c1, c2 = st.columns(2)
        with c1:
            g_col = st.selectbox("Group By Column", ["category", "value"])
        with c2:
            g_agg = st.selectbox("Aggregation", ["count", "sum", "avg", "min", "max"])

        if st.button("Run Aggregation"):
            st.session_state['last_events'] = driver.run_job("groupby", column=g_col, agg=g_agg)
            st.rerun()

    # --- ORDER BY ---
    elif col_op == "OrderBy":
        c1, c2 = st.columns(2)
        with c1:
            o_col = st.selectbox("Sort By", ["value", "id"])
        with c2:
            o_asc = st.checkbox("Ascending", True)

        if st.button("Run Global Sort"):
            st.session_state['last_events'] = driver.run_job("orderby", column=o_col, ascending=o_asc)
            st.rerun()

    # --- COALESCE ---
    elif col_op == "Coalesce":
        target = st.slider("Target Partitions", 1, 2, 2)
        st.info("Merges partitions to reduce overhead (Narrow Dependency if possible).")
        if st.button("Run Coalesce"):
            st.session_state['last_events'] = driver.run_job("coalesce", target_num=target)
            st.rerun()

    # --- FILTER ---
    elif col_op == "Filter":
        c1, c2, c3 = st.columns(3)
        with c1:
            f_col = st.selectbox("Col", ["category", "value"])
        with c2:
            f_cond = st.selectbox("Op", ["==", ">", "<"])
        with c3:
            f_val = st.text_input("Val", "50")
        if st.button("Apply Filter"):
            st.session_state['last_events'] = driver.run_job("filter", column=f_col, condition=f_cond, value=f_val)
            st.rerun()

    # --- COLLECT ---
    elif col_op == "Collect":
        st.warning("Trigger Action: Pulls all data to Driver.")
        if st.button("Run Collect"):
            st.session_state['last_events'] = driver.run_job("collect")
            st.rerun()

with tabs[1]:  # CHAOS
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("☠️ Fault Injection")
        k_target = st.selectbox("Kill Executor", [1, 2, 3])
        if st.button("Kill Node"):
            driver.run_job("kill", target=k_target)
            st.rerun()

        st.markdown("---")
        if st.button("🚑 Recover / Restart All"):
            st.session_state['last_events'] = driver.run_job("recover")
            st.rerun()

    with c2:
        st.subheader("🐢 Performance")
        s_target = st.selectbox("Make Straggler (Slow)", [1, 2, 3])
        if st.button("Apply Network Lag"):
            driver.run_job("straggler", target=s_target)
            st.rerun()

        st.markdown("---")
        mem_limit = st.slider("Max RAM (Rows/Node)", 5, 100, 50)
        if st.button("Set Memory Limit"):
            driver.run_job("set_limit", limit=mem_limit)
            st.rerun()

with tabs[2]:  # LOGS
    st.text_area("Driver Logs", value="\n".join(reversed(driver.logs)), height=200)

# ==========================================
# 5. DATA INSPECTOR
# ==========================================
st.markdown("### 🔍 Cluster Memory State")

# DRIVER
if not driver.memory.empty:
    st.success(f"Driver Result Buffer: {len(driver.memory)} rows")
    st.dataframe(driver.memory.head(5), use_container_width=True)

# EXECUTORS
cols = st.columns(3)
for i, (ex_id, executor) in enumerate(driver.cluster.executors.items()):
    with cols[i]:
        status_icon = "🟢" if executor.status == "ALIVE" else "🔴" if executor.status == "OOM" else "🐢" if executor.status == "STRAGGLER" else "💀"
        st.markdown(f"**{status_icon} Executor {ex_id}**")

        # Usage Bar
        usage = len(executor.data)
        limit = executor.max_rows
        st.progress(min(usage / limit, 1.0), text=f"{usage}/{limit} rows")

        if executor.status in ["DEAD", "OOM"]:
            st.error(f"Status: {executor.status}")
        else:
            if not executor.data.empty:
                st.dataframe(executor.data, hide_index=True, use_container_width=True)
            else:
                st.caption("Empty")