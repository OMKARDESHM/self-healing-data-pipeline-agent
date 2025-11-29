from pathlib import Path
from typing import Any, Dict, Optional
import json
import pandas as pd

from .logger import get_logger

logger = get_logger(__name__)

BASE_DIR = Path(__file__).resolve().parents[1]
INCIDENTS_PATH = BASE_DIR / "data" / "metadata" / "incidents.csv"


def _ensure_incidents_file():
    INCIDENTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not INCIDENTS_PATH.exists():
        df = pd.DataFrame(
            columns=[
                "run_id",
                "pipeline_name",
                "description",
                "stage",
                "status",
                "error_type",
                "error_message",
                "issues_json",
                "healing_actions_json",
            ]
        )
        df.to_csv(INCIDENTS_PATH, index=False)


def log_incident(
    run_id: str,
    pipeline_name: str,
    description: str,
    stage: str,
    status: str,
    error_type: Optional[str] = None,
    error_message: Optional[str] = None,
    issues: Optional[Dict[str, Any]] = None,
    healing_actions: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Append an incident/run record to incidents.csv
    """
    _ensure_incidents_file()

    issues_str = json.dumps(issues or {}, default=str)
    healing_str = json.dumps(healing_actions or {}, default=str)

    new_row = {
        "run_id": run_id,
        "pipeline_name": pipeline_name,
        "description": description,
        "stage": stage,
        "status": status,
        "error_type": error_type or "",
        "error_message": error_message or "",
        "issues_json": issues_str,
        "healing_actions_json": healing_str,
    }

    df = pd.read_csv(INCIDENTS_PATH)
    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    df.to_csv(INCIDENTS_PATH, index=False)

    logger.info(
        f"[bold blue]Incident logged[/bold blue]: "
        f"run_id={run_id}, stage={stage}, status={status}, pipeline={pipeline_name}"
    )
