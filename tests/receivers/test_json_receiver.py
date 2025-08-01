import json
import pandas as pd
import dask.dataframe as dd
from pathlib import Path
from src.receivers.files.json_receiver import JSONReceiver


def test_jsonreceiver_row(tmp_path: Path):
    receiver = JSONReceiver()
    row = {"id": 1, "name": "Alice"}
    file_path = tmp_path / "row.json"

    # Write row
    receiver.write_row(row, file_path)
    assert file_path.exists()

    # Read row
    read = receiver.read_row(file_path)
    assert isinstance(read, dict)
    assert read["id"] == 1
    assert read["name"] == "Alice"


def test_jsonreceiver_bulk(tmp_path: Path):
    receiver = JSONReceiver()
    df = pd.DataFrame([
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"}
    ])
    file_path = tmp_path / "bulk.json"

    # Write bulk
    receiver.write_bulk(df, file_path)
    assert file_path.exists()

    # Read bulk
    read_df = receiver.read_bulk(file_path)
    assert isinstance(read_df, pd.DataFrame)
    assert len(read_df) == 2
    assert read_df.iloc[0]["name"] == "Alice"


import glob

def test_jsonreceiver_bigdata(tmp_path: Path):
    receiver = JSONReceiver()
    ddf = dd.from_pandas(pd.DataFrame([
        {"id": 10, "name": "Charlie"},
        {"id": 11, "name": "Diana"}
    ]), npartitions=1)

    file_path = tmp_path / "bigdata_output"
    file_path.mkdir()

    # Write bigdata using receiver method (creates part.* files)
    receiver.write_bigdata(ddf, file_path)

    # Read bigdata using file pattern
    json_files = list(file_path.glob("part-*.json"))
    assert json_files, "No JSON partition files found."

    read_ddf = dd.read_json([str(p) for p in json_files], orient="records", blocksize="64MB")
    read_df = read_ddf.compute()

    assert isinstance(read_df, pd.DataFrame)
    assert len(read_df) == 2
    assert "Charlie" in read_df["name"].values