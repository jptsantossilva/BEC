import pandas as pd
import streamlit as st

import utils.database as database

st.header("Scheduled Jobs")
# st.write(f"You are logged in as {st.session_state.role}.")

# st.subheader("Signal Scheduler (UTC)")
st.caption("Enable or disable scheduled jobs. Schedule runs in UTC time zone.")
df_schedules = database.get_job_schedules()
if df_schedules.empty:
    st.info("No schedules found.")
else:
    editable = df_schedules.copy()
    editable["enabled"] = editable["enabled"].astype(bool)
    if "last_run" in editable.columns:
        last_run = pd.to_datetime(editable["last_run"], utc=True, errors="coerce")
        editable["last_run"] = last_run.dt.strftime("%Y-%m-%d %H:%M:%S UTC")

    def apply_schedule_changes():
        edits = st.session_state.get("signals_editor")
        if not isinstance(edits, dict):
            return
        edited_rows = edits.get("edited_rows", {})
        if not edited_rows:
            return
        for row_idx, changes in edited_rows.items():
            if "enabled" not in changes:
                continue
            name = editable.iloc[int(row_idx)]["name"]
            database.set_job_schedule_enabled(name, bool(changes["enabled"]))

    edited = st.data_editor(
        editable,
        width="content",
        num_rows="fixed",
        disabled=["name", "script", "script_args", "cadence", "last_run", "description"],
        column_config={
            "name": "Name",
            "last_run": "Last Run",
            "enabled": st.column_config.CheckboxColumn("Enabled"),
            "cadence": st.column_config.TextColumn("Cadence"),
            "description": st.column_config.TextColumn("Description"),
        },
        column_order=["name", "enabled", "cadence", "last_run", "description"],
        key="signals_editor",
        on_change=apply_schedule_changes,
    )
