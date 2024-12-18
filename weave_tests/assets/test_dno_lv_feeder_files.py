import os

import pandas as pd
import pyarrow as pa
import pytest
from dagster import build_asset_context

from weave.assets.dno_lv_feeder_files import nged_lv_feeder_files, ssen_lv_feeder_files
from weave.resources.nged import StubNGEDAPICLient
from weave.resources.output_files import OutputFilesResource
from weave.resources.ssen import StubSSENAPICLient

FIXTURE_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    "..",
    "fixtures",
)


class TestSSENLVFeederFiles:
    def test_happy_path(self, tmp_path):
        output_dir = tmp_path / "ssen"
        output_dir.mkdir()
        context = build_asset_context(
            partition_key="https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-12.csv"
        )
        raw_files_resource = OutputFilesResource(url=tmp_path.as_uri())
        api_client = StubSSENAPICLient(
            available_files_url=os.path.join(
                FIXTURE_DIR, "ssen", "available_files.json"
            ),
            file_to_download=os.path.join(
                FIXTURE_DIR, "ssen", "lv_feeder_files", "2024-02-12_head.csv"
            ),
        )
        ssen_lv_feeder_files(context, raw_files_resource, api_client)

        df = pd.read_csv((output_dir / "2024-02-12.csv.gz").as_posix())
        assert len(df) == 10

    def test_deletes_bad_files(self, tmp_path):
        output_dir = tmp_path / "ssen"
        output_dir.mkdir()
        context = build_asset_context(
            partition_key="https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-11-18.csv"
        )
        raw_files_resource = OutputFilesResource(url=tmp_path.as_uri())
        api_client = StubSSENAPICLient(
            available_files_url=os.path.join(
                FIXTURE_DIR, "ssen", "available_files.json"
            ),
            file_to_download=os.path.join(
                FIXTURE_DIR, "ssen", "lv_feeder_files", "2024-11-18_bad.csv"
            ),
        )

        with pytest.raises(pa.lib.ArrowInvalid):
            ssen_lv_feeder_files(context, raw_files_resource, api_client)

        assert not (output_dir / "2024-11-18.csv.gz").exists()


class TestNGEDLVFeederFiles:
    def test_happy_path(self, tmp_path):
        output_dir = tmp_path / "nged"
        output_dir.mkdir()
        context = build_asset_context(
            partition_key="https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/105a7821-7f5c-4591-90e8-5915f253b1ff/download/aggregated-smart-meter-data-lv-feeder-2024-01-part0000.csv"
        )
        raw_files_resource = OutputFilesResource(url=tmp_path.as_uri())
        api_client = StubNGEDAPICLient(
            api_token="TEST",
            file_to_download=os.path.join(
                FIXTURE_DIR,
                "nged",
                "lv_feeder_files",
                "aggregated-smart-meter-data-lv-feeder-2024-01-part0000_head.csv",
            ),
        )
        nged_lv_feeder_files(context, raw_files_resource, api_client)

        df = pd.read_csv(
            (
                output_dir
                / "aggregated-smart-meter-data-lv-feeder-2024-01-part0000.csv.gz"
            ).as_posix()
        )
        assert len(df) == 10
