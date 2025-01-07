import os

import pandas as pd
from dagster import AssetKey, AssetMaterialization, DagsterInstance, build_asset_context
from zlib_ng import zlib_ng

from weave.assets.dno_lv_feeder_monthly_parquet import (
    nged_lv_feeder_monthly_parquet,
    ssen_lv_feeder_monthly_parquet,
)
from weave.core import lv_feeder_parquet_schema
from weave.resources.nged import StubNGEDAPIClient
from weave.resources.output_files import OutputFilesResource
from weave.resources.ssen import StubSSENAPICLient

FIXTURE_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    "..",
    "fixtures",
)


class TestSSENLVFeederMonthlyParquet:
    @staticmethod
    def create_daily_files(year, month, start, end, dir):
        input_file = os.path.join(
            FIXTURE_DIR, "ssen", "lv_feeder_files", "2024-02-12_for_location_lookup.csv"
        )
        for day in range(start, end):
            filename = dir / f"{year}-{month:02d}-{day:02d}.csv.gz"
            with open(filename.as_posix(), "wb") as output_file:
                with open(input_file, "rb") as f:
                    output_file.write(
                        zlib_ng.compress(
                            f.read(), level=1, wbits=zlib_ng.MAX_WBITS | 16
                        )
                    )

    @staticmethod
    def create_substation_location_lookup_postcodes(dir):
        data = {
            "substation_nrn": ["0002002004", "0002002009"],
            "substation_geo_location": ["50.79,-2.43", "49.79,-1.43"],
        }
        df = pd.DataFrame(data=data)
        df.to_parquet(dir / "substation_location_lookup_feeder_postcodes.parquet")

    @staticmethod
    def substation_location_lookup_load_model(dir):
        data = {
            "substation_nrn": ["0002002004"],
            "substation_geo_location": ["51.79,-2.53"],
        }
        df = pd.DataFrame(data=data)
        df.to_parquet(dir / "substation_location_lookup_transformer_load_model.parquet")

    def test_happy_path(self, tmp_path):
        output_dir = tmp_path / "staging" / "ssen"
        output_dir.mkdir(parents=True)
        input_dir = tmp_path / "raw" / "ssen"
        input_dir.mkdir(parents=True)

        self.create_daily_files(2024, 2, 1, 30, input_dir)
        self.create_substation_location_lookup_postcodes(output_dir)
        self.substation_location_lookup_load_model(output_dir)

        context = build_asset_context(partition_key="2024-02-01")
        staging_files_resource = OutputFilesResource(
            url=(tmp_path / "staging").as_uri()
        )
        raw_files_resource = OutputFilesResource(url=(tmp_path / "raw").as_uri())
        ssen_api_client = StubSSENAPICLient()

        ssen_lv_feeder_monthly_parquet(
            context, raw_files_resource, staging_files_resource, ssen_api_client
        )

        df = pd.read_parquet(output_dir / "2024-02.parquet", engine="pyarrow")
        assert len(df) == 6 * 29
        assert (
            df[df["dataset_id"] == "000200200402"].iloc[0].substation_geo_location
            == "51.79,-2.53"
        )
        assert (
            df[df["dataset_id"] == "000200200404"].iloc[0].substation_geo_location
            == "51.79,-2.53"
        )
        assert (
            df[df["dataset_id"] == "000200200901"].iloc[0].substation_geo_location
            == "49.79,-1.43"
        )
        assert (
            df[df["dataset_id"] == "000200200902"].iloc[0].substation_geo_location
            == "49.79,-1.43"
        )
        assert (
            df[df["dataset_id"] == "000200202002"].iloc[0].substation_geo_location
            is None
        )
        assert (
            df[df["dataset_id"] == "000200202003"].iloc[0].substation_geo_location
            is None
        )

    def test_when_missing_input(self, tmp_path):
        output_dir = tmp_path / "staging" / "ssen"
        output_dir.mkdir(parents=True)
        input_dir = tmp_path / "raw" / "ssen"
        input_dir.mkdir(parents=True)

        self.create_substation_location_lookup_postcodes(output_dir)
        self.substation_location_lookup_load_model(output_dir)

        context = build_asset_context(partition_key="2024-02-01")
        staging_files_resource = OutputFilesResource(
            url=(tmp_path / "staging").as_uri()
        )
        raw_files_resource = OutputFilesResource(url=(tmp_path / "raw").as_uri())
        ssen_api_client = StubSSENAPICLient()

        ssen_lv_feeder_monthly_parquet(
            context, raw_files_resource, staging_files_resource, ssen_api_client
        )

        assert not (output_dir / "2024-02.parquet").exists()


class TestNGEDLVFeederMonthlyParquet:
    def test_happy_path(self, tmp_path):
        instance = DagsterInstance.ephemeral()

        output_dir = tmp_path / "staging" / "nged"
        output_dir.mkdir(parents=True)
        input_dir = tmp_path / "raw" / "nged"
        input_dir.mkdir(parents=True)

        fixture_file = os.path.join(
            FIXTURE_DIR,
            "nged",
            "lv_feeder_files",
            "aggregated-smart-meter-data-lv-feeder-2024-01-part0000_head.csv",
        )
        for part in range(10):
            filename = (
                f"aggregated-smart-meter-data-lv-feeder-2024-01-part{part:04d}.csv"
            )
            partition = f"http://example.com/{filename}"
            with open((input_dir / f"{filename}.gz").as_posix(), "wb") as output_file:
                with open(fixture_file, "rb") as f:
                    output_file.write(
                        zlib_ng.compress(
                            f.read(), level=1, wbits=zlib_ng.MAX_WBITS | 16
                        )
                    )
            instance.report_runless_asset_event(
                AssetMaterialization(
                    asset_key=AssetKey("nged_lv_feeder_files"), partition=partition
                )
            )

        context = build_asset_context(partition_key="2024-01-01", instance=instance)
        staging_files_resource = OutputFilesResource(
            url=(tmp_path / "staging").as_uri()
        )
        raw_files_resource = OutputFilesResource(url=(tmp_path / "raw").as_uri())
        nged_api_client = StubNGEDAPIClient()

        result = nged_lv_feeder_monthly_parquet(
            context, raw_files_resource, staging_files_resource, nged_api_client
        )

        df = pd.read_parquet(output_dir / "2024-01.parquet", engine="pyarrow")
        assert len(df) == 100
        assert df.columns.tolist() == lv_feeder_parquet_schema.names
        assert result.metadata["dagster/row_count"] == 100
        assert result.metadata["weave/nunique_feeders"] == 1

    def test_when_missing_input(self, tmp_path):
        output_dir = tmp_path / "staging" / "ssen"
        output_dir.mkdir(parents=True)
        input_dir = tmp_path / "raw" / "ssen"
        input_dir.mkdir(parents=True)

        context = build_asset_context(partition_key="2024-01-01")
        staging_files_resource = OutputFilesResource(
            url=(tmp_path / "staging").as_uri()
        )
        raw_files_resource = OutputFilesResource(url=(tmp_path / "raw").as_uri())
        nged_api_client = StubNGEDAPIClient()

        result = nged_lv_feeder_monthly_parquet(
            context, raw_files_resource, staging_files_resource, nged_api_client
        )

        assert not (output_dir / "2024-01.parquet").exists()
        assert result.metadata["dagster/row_count"] == 0
        assert result.metadata["weave/nunique_feeders"] == 0
