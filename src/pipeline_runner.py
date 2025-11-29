from pathlib import Path
from typing import Dict, Any
from datetime import datetime

import yaml

from .logger import get_logger
from .etl_job import run_etl
from .data_quality_checks import enforce_data_quality, DataQualityError
from .drift_detector import detect_and_update_drift
from .self_healing_agent import apply_self_healing
from .incident_logger import log_incident

logger = get_logger(__name__)

BASE_DIR = Path(__file__).resolve().parents[1]
CONFIG_PATH = BASE_DIR / "config" / "pipeline_config.yml"
PIPELINE_NAME = "customers_pipeline"


def load_config() -> Dict[str, Any]:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def write_config(cfg: Dict[str, Any]) -> None:
    with open(CONFIG_PATH, "w") as f:
        yaml.safe_dump(cfg, f, sort_keys=False)


def reset_environment():
    """
    Clears warehouse + metadata profiles to simulate a fresh run.
    """
    warehouse = BASE_DIR / "data" / "warehouse.duckdb"
    metadata_dir = BASE_DIR / "data" / "metadata"

    if warehouse.exists():
        warehouse.unlink()

    if metadata_dir.exists():
        for p in metadata_dir.glob("*.json"):
            p.unlink()

    logger.info("[blue]Environment reset: warehouse + metadata cleared.[/blue]")


def run_single_pipeline(description: str) -> Dict[str, Any]:
    logger.info(f"\n[bold underline]=== RUN: {description} ===[/bold underline]")
    config = load_config()

    # Run ETL
    df = run_etl(config, BASE_DIR)

    # Data quality
    dq_report = enforce_data_quality(df, config)

    # Drift detection
    drift_report = detect_and_update_drift(df, config, BASE_DIR)

    return {"dq_report": dq_report, "drift_report": drift_report}


def _run_id(label: str) -> str:
    ts = datetime.utcnow().isoformat(timespec="seconds")
    return f"{label}-{ts}Z"


def main():
    logger.info("[bold green]Starting Self-Healing Data Pipeline demo[/bold green]")
    reset_environment()

    # ----- STEP 1: Baseline run with clean data -----
    cfg = load_config()
    cfg["source_path"] = "data/raw/customers_v1.csv"
    write_config(cfg)

    baseline_run_id = _run_id("baseline")
    try:
        baseline_result = run_single_pipeline("Baseline run with clean data (v1)")
        logger.info("[green]Baseline completed successfully.[/green]")

        log_incident(
            run_id=baseline_run_id,
            pipeline_name=PIPELINE_NAME,
            description="Baseline run with clean data (v1)",
            stage="baseline",
            status="success",
            error_type=None,
            error_message=None,
            issues=baseline_result.get("dq_report", {}),
            healing_actions=None,
        )
    except Exception as e:
        logger.exception("Unexpected failure during baseline run.")
        log_incident(
            run_id=baseline_run_id,
            pipeline_name=PIPELINE_NAME,
            description="Baseline run with clean data (v1)",
            stage="baseline",
            status="failed",
            error_type=type(e).__name__,
            error_message=str(e),
            issues={},
            healing_actions=None,
        )
        return

    # ----- STEP 2: Run with broken / drifted data (v2) -----
    cfg = load_config()
    cfg["source_path"] = "data/raw/customers_v2_broken.csv"
    write_config(cfg)

    broken_run_id = _run_id("drifted")
    issue_report = None

    try:
        _ = run_single_pipeline("Run with drifted/broken data (v2)")
        logger.info("[red]Unexpected: v2 data passed without issues (no healing needed).[/red]")

        # Log as success (no issues)
        log_incident(
            run_id=broken_run_id,
            pipeline_name=PIPELINE_NAME,
            description="Run with drifted/broken data (v2)",
            stage="drifted",
            status="success",
            error_type=None,
            error_message=None,
            issues={},
            healing_actions=None,
        )
        return

    except DataQualityError as dq_err:
        logger.warning("[yellow]As expected, data quality failed with v2 data.[/yellow]")
        issue_report = dq_err.report

        log_incident(
            run_id=broken_run_id,
            pipeline_name=PIPELINE_NAME,
            description="Run with drifted/broken data (v2)",
            stage="drifted",
            status="failed",
            error_type="DataQualityError",
            error_message=str(dq_err),
            issues=issue_report,
            healing_actions=None,
        )

    except Exception as e:
        logger.exception("Pipeline failed for an unexpected reason.")
        log_incident(
            run_id=broken_run_id,
            pipeline_name=PIPELINE_NAME,
            description="Run with drifted/broken data (v2)",
            stage="drifted",
            status="failed",
            error_type=type(e).__name__,
            error_message=str(e),
            issues={},
            healing_actions=None,
        )
        return

    # If we get here, we have a DataQualityError and issue_report
    if issue_report is None:
        logger.error("No issue report available; cannot apply self-healing.")
        return

    # ----- STEP 3: Self-Healing Agent updates config -----
    healing_result = apply_self_healing(issue_report, CONFIG_PATH)

    healing_run_id = _run_id("healing")
    log_incident(
        run_id=healing_run_id,
        pipeline_name=PIPELINE_NAME,
        description="Self-Healing Agent applied configuration updates",
        stage="healing",
        status="healing_actions_applied" if healing_result["changes"] else "no_changes",
        error_type=None,
        error_message=None,
        issues=issue_report,
        healing_actions=healing_result,
    )

    if not healing_result["changes"]:
        logger.info("No changes applied by Self-Healing Agent. Stopping demo.")
        return

    # ----- STEP 4: Re-run pipeline after self-healing -----
    final_run_id = _run_id("post_healing")

    try:
        final_result = run_single_pipeline("Re-run after Self-Healing Agent applied fixes")
        logger.info("[bold green]Demo succeeded: pipeline recovered after automatic config tuning![/bold green]")

        log_incident(
            run_id=final_run_id,
            pipeline_name=PIPELINE_NAME,
            description="Re-run after Self-Healing Agent applied fixes",
            stage="post_healing",
            status="healed_success",
            error_type=None,
            error_message=None,
            issues=final_result.get("dq_report", {}),
            healing_actions=healing_result,
        )

    except Exception as e:
        logger.exception("[bold red]Even after healing, pipeline failed. Check logs for details.[/bold red]")
        log_incident(
            run_id=final_run_id,
            pipeline_name=PIPELINE_NAME,
            description="Re-run after Self-Healing Agent applied fixes",
            stage="post_healing",
            status="failed_after_healing",
            error_type=type(e).__name__,
            error_message=str(e),
            issues={},
            healing_actions=healing_result,
        )
        return


if __name__ == "__main__":
    main()
