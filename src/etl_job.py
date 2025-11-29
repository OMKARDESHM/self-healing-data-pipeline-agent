from pathlib import Path
from typing import Dict

import duckdb
import pandas as pd

from .logger import get_logger

logger = get_logger(__name__)


def load_config_paths(config: Dict, base_dir: Path) -> Dict:
    cfg = config.copy()
    cfg["warehouse_path"] = str(base_dir / cfg["warehouse_path"])
    cfg["source_path"] = str(base_dir / cfg["source_path"])

    drift_cfg = cfg.get("drift", {})
    if "profile_path" in drift_cfg:
        drift_cfg["profile_path"] = str(base_dir / drift_cfg["profile_path"])
        cfg["drift"] = drift_cfg

    return cfg


def run_etl(config: Dict, base_dir: Path):
    """
    Simple ETL:
    - Reads CSV
    - Casts columns according to config
    - Writes to DuckDB table
    - Returns transformed DataFrame
    """
    cfg = load_config_paths(config, base_dir)
    source_path = Path(cfg["source_path"])
    warehouse_path = Path(cfg["warehouse_path"])
    table_name = cfg["table_name"]
    columns_cfg = cfg["columns"]

    logger.info(f"Reading source CSV: [bold]{source_path}[/bold]")
    df = pd.read_csv(source_path)

    # Basic cleaning: strip column names
    df.columns = [c.strip() for c in df.columns]

    # Expected columns from config
    expected_cols = list(columns_cfg.keys())
    missing_cols = [c for c in expected_cols if c not in df.columns]

    if missing_cols:
        logger.warning(
            f"Source data is missing expected columns: {missing_cols}. "
            "Continuing with available columns; missing ones will be reported by data-quality layer."
        )

    present_expected = [c for c in expected_cols if c in df.columns]
    df = df[present_expected]

    # Type coercion based on config
    for col in present_expected:
        meta = columns_cfg[col]
        col_type = meta.get("type", "string")
        if col_type == "int":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        elif col_type == "float":
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif col_type == "string":
            df[col] = df[col].astype("string")
        else:
            logger.warning(f"Unknown type '{col_type}' for column '{col}', leaving as is.")

    # Write to DuckDB
    warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Writing data to DuckDB warehouse: [bold]{warehouse_path}[/bold], table: [bold]{table_name}[/bold]")

    con = duckdb.connect(database=str(warehouse_path), read_only=False)
    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df LIMIT 0;")
    con.execute(f"DELETE FROM {table_name};")
    con.register("df", df)
    con.execute(f"INSERT INTO {table_name} SELECT * FROM df;")
    con.close()

    logger.info(f"ETL completed successfully. Rows loaded: {len(df)}")
    return df
