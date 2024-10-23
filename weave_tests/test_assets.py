import os

import pandas as pd
import pytest
from dagster import build_asset_context
from zlib_ng import zlib_ng

from weave.assets.dno_lv_feeder_files import ssen_lv_feeder_files
from weave.assets.dno_lv_feeder_monthly_parquet import ssen_lv_feeder_monthly_parquet
from weave.resources.output_files import OutputFilesResource
from weave.resources.ssen import TestSSENAPIClient

FIXTURE_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    "fixtures",
)


@pytest.fixture
def ssen_api_client():
    return TestSSENAPIClient(
        available_files_url=os.path.join(FIXTURE_DIR, "ssen_files.json"),
        file_to_download=os.path.join(FIXTURE_DIR, "ssen_2024-02-12_head.csv"),
    )


def test_ssen_lv_feeder_files(tmp_path, ssen_api_client):
    output_dir = tmp_path / "ssen"
    output_dir.mkdir()
    context = build_asset_context(
        partition_key="https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-12.csv"
    )
    raw_files_resource = OutputFilesResource(url=tmp_path.as_uri())
    ssen_lv_feeder_files(context, raw_files_resource, ssen_api_client)

    df = pd.read_csv((output_dir / "2024-02-12.csv.gz").as_posix())
    assert len(df) == 10


def create_daily_files(year, month, start, end, dir):
    input_file = os.path.join(FIXTURE_DIR, "ssen_2024-02-12_head.csv")
    for day in range(start, end):
        filename = dir / f"{year}-{month:02d}-{day:02d}.csv.gz"
        with open(filename.as_posix(), "wb") as output_file:
            with open(input_file, "rb") as f:
                output_file.write(
                    zlib_ng.compress(f.read(), level=1, wbits=zlib_ng.MAX_WBITS | 16)
                )


def test_ssen_lv_feeder_monthly_parquet(tmp_path):
    output_dir = tmp_path / "staging" / "ssen"
    output_dir.mkdir(parents=True)
    input_dir = tmp_path / "raw" / "ssen"
    input_dir.mkdir(parents=True)
    create_daily_files(2024, 2, 1, 30, input_dir)  # 2024 was a leap year
    context = build_asset_context(partition_key="2024-02-01")
    staging_files_resource = OutputFilesResource(url=(tmp_path / "staging").as_uri())
    raw_files_resource = OutputFilesResource(url=(tmp_path / "raw").as_uri())
    ssen_lv_feeder_monthly_parquet(context, raw_files_resource, staging_files_resource)

    df = pd.read_parquet((output_dir / "2024-02.parquet").as_posix(), engine="pyarrow")
    assert len(df) == 10 * 29
    # Testing categoricals correctly survive pyarrow -> pandas roundtrip
    assert isinstance(df.dtypes["dno_alias"], pd.CategoricalDtype)
