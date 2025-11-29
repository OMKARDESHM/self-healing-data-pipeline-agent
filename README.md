# self-healing-data-pipeline-agent
detects data quality issues &amp; drift → updates config → reruns pipeline successfully.
# 🧠 Self-Healing Data Pipeline Agent (with Incidents & Dashboard)

An intelligent **Self-Healing Data Pipeline** that automatically:

- Detects data quality issues & schema / data drift
- Updates configuration policies (like `max_null_fraction`, `required`, `row_count_min`)
- Reruns the pipeline successfully
- Logs every run as an **incident**
- Exposes a **Streamlit dashboard** to inspect failures and auto-healing actions

This is designed as a **recruiter-level project** for Data Engineering + Data Quality + Agentic Automation.

---

## 🚀 Tech Stack

- **Python 3.11**
- **DuckDB** – local analytics warehouse
- **Pandas** – ETL transformations
- **PyYAML** – config-driven pipelines
- **Rich** – beautiful logging
- **Streamlit** – incidents dashboard
- Runs cleanly in **GitHub Codespaces** via `.devcontainer`

---

## 🧩 High-Level Flow

1. **Baseline run (clean data)**  
   - Reads `data/raw/customers_v1.csv`
   - Enforces strict data quality rules
   - Builds initial drift profile (numeric means & std)

2. **Drifted / broken run**  
   - Switches to `data/raw/customers_v2_broken.csv`
   - Data quality fails due to:
     - High nulls in `age`
     - Invalid value `"thirty"`

3. **Self-Healing Agent**  
   - Reads the failure report
   - Automatically updates:
     - `columns.age.max_null_fraction`
     - (if needed) `required` flags for missing columns
     - (if needed) `quality.row_count_min`
   - Writes new config back to disk

4. **Recovered run**  
   - Reruns ETL + Quality + Drift
   - Pipeline now **succeeds** thanks to auto-tuned config

5. **Incidents Dashboard**  
   - All runs (success, failed, healed) are logged to `data/metadata/incidents.csv`
   - `app/dashboard.py` visualizes:
     - Run history
     - Status by stage
     - Issues JSON
     - Healing actions JSON

---

## 🛠️ Step 1 – Use This in GitHub Codespaces

1. Create a new GitHub repository.
2. Copy the entire structure & files from this project into your repo.
3. Commit & push.
4. On GitHub, click **Code → Codespaces → Create codespace on main**.
5. Wait for the container to build (it will install `requirements.txt` automatically).

---

## ▶️ Step 2 – Run the Self-Healing Demo

In the Codespaces terminal, from the project root:

```bash
python -m src.pipeline_runner
