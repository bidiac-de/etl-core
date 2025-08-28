from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping

import dask.dataframe as dd
import pandas as pd


def load_cfg(path: Path) -> Dict[str, Any]:
    """Load a job config JSON file."""
    text = path.read_text(encoding="utf-8")
    return json.loads(text)


def inject_filepaths(
    cfg: Dict[str, Any],
    mapping: Mapping[str, Path],
) -> None:
    """
    Inject filepaths per component type.

    Example mapping:
        {
            "read_csv": in_fp,
            "write_csv": out_fp,
            "read_excel": in_fp,
            "write_excel": out_fp,
            "read_json": in_fp,
            "write_json": out_fp,
            ...
        }
    """
    comps: Iterable[Dict[str, Any]] = cfg.get("components", [])
    for comp in comps:
        ctype = comp.get("comp_type")
        if not ctype:
            continue
        fp = mapping.get(ctype)
        if fp is not None:
            comp["filepath"] = str(fp)


def render_job_cfg_with_filepaths(
    cfg_path: Path,
    mapping: Mapping[str, Path],
) -> Dict[str, Any]:
    """
    Load a config and inject filepaths for the given component types.
    """
    cfg = load_cfg(cfg_path)
    inject_filepaths(cfg, mapping)
    return cfg


def data_path(*rel: str) -> Path:
    """
    Locate test data by walking up a few levels so tests are layout-agnostic.
    """
    here = Path(__file__).parent
    for _ in range(6):
        candidate = here.joinpath(*rel)
        if candidate.exists():
            return candidate
        here = here.parent
    raise FileNotFoundError(f"Could not locate test data: {'/'.join(rel)}")


def read_output(writer_type: str, strategy: str, out: Path) -> pd.DataFrame:
    """
    Read output into a pandas DataFrame regardless of writer type/strategy.
    """
    if writer_type == "write_csv":
        return pd.read_csv(out)

    if writer_type == "write_excel":
        return pd.read_excel(out)

    if writer_type == "write_json":
        if strategy == "row":
            return pd.read_json(out, lines=True)
        if strategy == "bulk":
            return pd.read_json(out)
        if strategy == "bigdata":
            parts = sorted(out.glob("part-*.jsonl")) or sorted(out.glob("*.jsonl"))
            if not parts:
                raise AssertionError("No NDJSON part files written.")
            ddf = dd.read_json([str(p) for p in parts], lines=True, blocksize="64MB")
            return ddf.compute()
        raise AssertionError(f"Unknown json strategy: {strategy}")

    raise AssertionError(f"Unknown writer type: {writer_type}")
