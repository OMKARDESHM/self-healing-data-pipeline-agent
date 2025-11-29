from typing import Dict, Any, Tuple

import pandas as pd

from .logger import get_logger

logger = get_logger(__name__)


class DataQualityError(Exception):
    def __init__(self, message: str, report: Dict[str, Any]):
        super().__init__(message)
        self.report = report


def run_data_quality(df: pd.DataFrame, config: Dict) -> Tuple[bool, Dict[str, Any]]:
    """
    Runs basic data quality checks:
    - Min row count
    - Required columns not fully null
    - max_null_fraction for configurable columns
    - Missing columns from config
    """
    quality_cfg = config.get("quality", {})
    columns_cfg = config["columns"]

    report: Dict[str, Any] = {
        "row_count": len(df),
        "null_fractions": {},
        "failed_checks": [],
    }

    # Row count check
    row_min = quality_cfg.get("row_count_min", 1)
    if len(df) < row_min:
        report["failed_checks"].append(
            {"type": "row_count", "message": f"Row count {len(df)} < minimum {row_min}"}
        )

    # Column-level checks
    for col, meta in columns_cfg.items():
        if col not in df.columns:
            report["failed_checks"].append(
                {"type": "missing_column", "column": col, "message": "Column not found in dataframe"}
            )
            continue

        null_fraction = df[col].isna().mean()
        report["null_fractions"][col] = float(null_fraction)

        if meta.get("required", False) and null_fraction > 0:
            report["failed_checks"].append(
                {
                    "type": "required_nulls",
                    "column": col,
                    "null_fraction": float(null_fraction),
                    "message": f"Required column '{col}' has nulls",
                }
            )

        max_null_fraction = meta.get("max_null_fraction", None)
        if max_null_fraction is not None and null_fraction > max_null_fraction:
            report["failed_checks"].append(
                {
                    "type": "null_fraction_exceeded",
                    "column": col,
                    "null_fraction": float(null_fraction),
                    "max_null_fraction": float(max_null_fraction),
                    "message": f"Column '{col}' null fraction {null_fraction:.2f} > allowed {max_null_fraction:.2f}",
                }
            )

    success = len(report["failed_checks"]) == 0

    if success:
        logger.info("[green]Data quality checks passed.[/green]")
    else:
        logger.warning("[yellow]Data quality checks failed.[/yellow]")
        for chk in report["failed_checks"]:
            logger.warning(f"- {chk['message']}")

    return success, report


def enforce_data_quality(df: pd.DataFrame, config: Dict) -> Dict[str, Any]:
    success, report = run_data_quality(df, config)
    if not success:
        raise DataQualityError("Data quality checks failed", report)
    return report
