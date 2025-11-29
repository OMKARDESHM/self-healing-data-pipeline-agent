from pathlib import Path
from typing import Dict, Any, List

import yaml

from .logger import get_logger

logger = get_logger(__name__)


def _load_yaml(path: Path) -> Dict[str, Any]:
    with open(path) as f:
        return yaml.safe_load(f)


def _write_yaml(path: Path, data: Dict[str, Any]) -> None:
    with open(path, "w") as f:
        yaml.safe_dump(data, f, sort_keys=False)


def _group_failed_checks(report: Dict[str, Any]):
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for chk in report.get("failed_checks", []):
        col = chk.get("column", "_global")
        grouped.setdefault(col, []).append(chk)
    return grouped


def apply_self_healing(
    issue_report: Dict[str, Any],
    config_path: Path,
    max_null_fraction_cap: float = 0.8,
    step_increase: float = 0.2,
) -> Dict[str, Any]:
    """
    Self-Healing strategies:

    1) For columns where null_fraction_exceeded:
       - Increase max_null_fraction by 'step_increase' (up to cap)
       - If no max_null_fraction defined, set it based on observed nulls.

    2) For missing_column checks:
       - Mark the column as required: false (soften contract).

    3) For row_count_min failures:
       - Lower 'quality.row_count_min' so the pipeline can proceed.

    Returns:
      {
        "changes": [list of human-readable messages],
        "updated_config": config_dict
      }
    """
    logger.info("[cyan]Self-Healing Agent activating...[/cyan]")
    config = _load_yaml(config_path)
    grouped = _group_failed_checks(issue_report)
    changes: List[str] = []

    # 3) Row count adjustment (global)
    for chk in issue_report.get("failed_checks", []):
        if chk["type"] == "row_count":
            observed_rows = issue_report.get("row_count", 0)
            quality_cfg = config.get("quality", {})
            prev_min = float(quality_cfg.get("row_count_min", 1))

            if observed_rows < prev_min:
                new_min = max(0, observed_rows)
                quality_cfg["row_count_min"] = int(new_min)
                config["quality"] = quality_cfg
                msg = (
                    f"Lowered quality.row_count_min from {prev_min:.0f} to {new_min:.0f} "
                    f"(observed_rows={observed_rows})"
                )
                logger.info("[magenta]" + msg + "[/magenta]")
                changes.append(msg)

    # 1) & 2) Column-level strategies
    for col, checks in grouped.items():
        if col == "_global":
            continue

        for chk in checks:
            ctype = chk["type"]

            # Strategy 1: null_fraction_exceeded
            if ctype == "null_fraction_exceeded":
                current_fraction = float(chk["null_fraction"])
                prev_max = float(
                    chk.get("max_null_fraction", config["columns"][col].get("max_null_fraction", 0.0))
                )

                # Increase tolerance but don't exceed cap
                new_max = min(max_null_fraction_cap, max(prev_max + step_increase, current_fraction + 0.05))

                config["columns"][col]["max_null_fraction"] = float(new_max)

                msg = (
                    f"Adjusted max_null_fraction for column '{col}' "
                    f"from {prev_max:.2f} to {new_max:.2f} "
                    f"(observed={current_fraction:.2f})"
                )
                logger.info("[magenta]" + msg + "[/magenta]")
                changes.append(msg)

            # Strategy 2: missing_column => soften required flag
            elif ctype == "missing_column":
                col_cfg = config["columns"].get(col, {})
                prev_required = bool(col_cfg.get("required", False))
                if prev_required:
                    col_cfg["required"] = False
                    config["columns"][col] = col_cfg
                    msg = (
                        f"Column '{col}' is missing in source; changed 'required' "
                        f"from True to False to avoid hard failure."
                    )
                    logger.info("[magenta]" + msg + "[/magenta]")
                    changes.append(msg)

    if not changes:
        logger.info("Self-Healing Agent found no config changes to apply.")
    else:
        _write_yaml(config_path, config)
        logger.info(f"[green]Self-Healing Agent wrote updated config to {config_path}[/green]")

    return {"changes": changes, "updated_config": config}
