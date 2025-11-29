from pathlib import Path

import json
import pandas as pd
import streamlit as st

BASE_DIR = Path(__file__).resolve().parents[1]
INCIDENTS_PATH = BASE_DIR / "data" / "metadata" / "incidents.csv"


def load_incidents():
    if not INCIDENTS_PATH.exists():
        return None
    df = pd.read_csv(INCIDENTS_PATH)
    return df


def main():
    st.title("🧠 Self-Healing Data Pipeline – Incidents Dashboard")

    df = load_incidents()
    if df is None or df.empty:
        st.info("No incidents logged yet. Run the pipeline first: `python -m src.pipeline_runner`")
        return

    # Basic stats
    st.subheader("Summary")

    total_runs = len(df)
    total_success = (df["status"].str.contains("success")).sum()
    total_failed = (df["status"].str.contains("failed")).sum()
    total_healed = (df["status"] == "healed_success").sum()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Runs", total_runs)
    col2.metric("Success", total_success)
    col3.metric("Failures", total_failed)
    col4.metric("Healed Success", total_healed)

    st.subheader("Run History")
    st.dataframe(df.sort_values("run_id", ascending=False), use_container_width=True)

    st.subheader("Filter by Stage / Status")
    stages = ["All"] + sorted(df["stage"].unique())
    statuses = ["All"] + sorted(df["status"].unique())

    c1, c2 = st.columns(2)
    stage_filter = c1.selectbox("Stage", stages)
    status_filter = c2.selectbox("Status", statuses)

    df_filtered = df.copy()
    if stage_filter != "All":
        df_filtered = df_filtered[df_filtered["stage"] == stage_filter]
    if status_filter != "All":
        df_filtered = df_filtered[df_filtered["status"] == status_filter]

    st.write("Filtered runs:")
    st.dataframe(df_filtered.sort_values("run_id", ascending=False), use_container_width=True)

    st.subheader("Inspect a Single Run")
    if not df.empty:
        selected_run_id = st.selectbox("Select run_id", df["run_id"].tolist())
        row = df[df["run_id"] == selected_run_id].iloc[0]

        st.write(f"### Run: `{row['run_id']}`")
        st.write(f"- Pipeline: `{row['pipeline_name']}`")
        st.write(f"- Stage: `{row['stage']}`")
        st.write(f"- Status: `{row['status']}`")
        if row["error_type"]:
            st.write(f"- Error: `{row['error_type']}` – {row['error_message']}")

        st.markdown("**Issues (JSON):**")
        st.code(row["issues_json"], language="json")

        st.markdown("**Healing Actions (JSON):**")
        st.code(row["healing_actions_json"], language="json")


if __name__ == "__main__":
    main()
