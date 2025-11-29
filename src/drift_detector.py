import json
from pathlib import Path
from typing import Dict, Any

import pandas as pd

from .logger import get_logger

logger = get_logger(__name__)


def _numeric_columns(df: pd.DataFrame):
    return df.select_dtypes(include=["number", "Int64"]).columns.tolist()


def _build_profile(df: pd.DataFrame) -> Dict[str, Any]:
    profile: Dict[str, Any] = {"columns": {}}
    for col in _numeric_columns(df):
        series = df[col].dropna()
        if len(series) == 0:
            continue
        profile["columns"][col] = {
            "mean": float(series.mean()),
            "std": float(series.std() if len(series) > 1 else 0.0),
        }
    return profile


def detect_and_update_drift(df: pd.DataFrame, config: Dict, base_dir: Path) -> Dict[str, Any]:
    drift_cfg = config.get("drift", {})
    profile_path = base_dir / drift_cfg.get("profile_path", "data/metadata/reference_profile.json")
    tolerance = float(drift_cfg.get("mean_relative_tolerance", 0.5))

    result: Dict[str, Any] = {
        "mode": "",
        "drifted_columns": [],
        "details": {},
    }

    if not profile_path.exists():
        logger.info(f"No reference profile found. Creating new baseline at {profile_path}")
        profile = _build_profile(df)
        profile_path.parent.mkdir(parents=True, exist_ok=True)
        with open(profile_path, "w") as f:
            json.dump(profile, f, indent=2)
        result["mode"] = "baseline_created"
        return result

    with open(profile_path) as f:
        baseline = json.load(f)

    current = _build_profile(df)
    result["mode"] = "comparison"
    result["details"]["baseline"] = baseline
    result["details"]["current"] = current

    for col, stats in current["columns"].items():
        if col not in baseline["columns"]:
            continue

        base_mean = baseline["columns"][col]["mean"]
        curr_mean = stats["mean"]

        if base_mean == 0:
            continue

        rel_change = abs(curr_mean - base_mean) / abs(base_mean)
        if rel_change > tolerance:
            logger.warning(
                f"Drift detected in column '{col}': mean changed from {base_mean:.2f} to {curr_mean:.2f} "
                f"(relative change {rel_change:.2f} > tolerance {tolerance:.2f})"
            )
            result["drifted_columns"].append(
                {
                    "column": col,
                    "base_mean": base_mean,
                    "current_mean": curr_mean,
                    "relative_change": rel_change,
                }
            )

    if not result["drifted_columns"]:
        logger.info("No significant drift detected in numeric columns.")
    else:
        logger.warning("Significant drift detected in numeric columns.")

    return result
