#  Self-Healing Data Pipeline Agent (with Incidents & Dashboard)

An intelligent, self-healing data pipeline framework that:

-  Detects data quality issues & data/schema drift  
-  Automatically adjusts pipeline configuration (null tolerances, required-column flags, row counts)  
-  Re-runs pipelines post-fix  
-  Logs every run (success / failure / healing) as incidents  
-  Provides a Streamlit dashboard to visualize issues & healing actions  

Designed as a "recruiter-grade portfolio project" for Data Engineering + Data Quality + Agentic Automation.  

---

##  Why this matters

In real data engineering environments:  
- Data quality issues, schema drift, and upstream changes frequently break pipelines.  
- Manually fixing config, rerunning pipelines, and documenting failures is time-consuming and error-prone.  
- This project automates that entire “fail → heal → rerun → log” cycle — giving companies reliability, resilience, and observability.

---

##  Tech Stack

| Layer                            | Technology / Library                  |
|----------------------------------|---------------------------------------|
| Orchestration & Execution        | Python 3.11, plain scripts            |
| Data Storage / Warehouse         | DuckDB (local)                        |
| ETL & Transformations            | Pandas                                |
| Config Management                | YAML (PyYAML)                         |
| Data Quality & Drift Detection   | Custom rules + drift profiling        |
| Logging & Observability          | Rich (console logs), CSV incident log |
| Dashboard / UI                   | Streamlit                             |

Runs cleanly in "GitHub Codespaces" using '.devcontainer'.

---

## 🚀 Quick Start (Codespaces / Local)

```bash
git clone https://github.com/OMKARDESHM/self-healing-data-pipeline-agent.git
cd self-healing-data-pipeline-agent
pip install -r requirements.txt     # or let Codespaces handle this
python -m src.pipeline_runner       # Run full demo: baseline → broken run → auto-heal → final run
streamlit run app/dashboard.py      # Launch dashboard to view incidents & healing logs
