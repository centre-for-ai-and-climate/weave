import os

import pandas as pd
import pytest
from dagster import build_asset_context

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
