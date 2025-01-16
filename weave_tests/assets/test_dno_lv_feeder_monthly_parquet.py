import calendar
import os

import numpy as np
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
        # TODO - we should refactor this to use the factory function
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

    def test_sorts_output(self, tmp_path):
        output_dir = tmp_path / "staging" / "ssen"
        output_dir.mkdir(parents=True)
        input_dir = tmp_path / "raw" / "ssen"
        input_dir.mkdir(parents=True)

        data = ssen_lv_feeder_raw_csv_factory(2)
        df = pd.DataFrame.from_records(data=data)
        output_columns = df.columns
        df["day"] = pd.to_datetime(df["data_collection_log_timestamp"]).dt.day
        for day in range(1, 30):
            filename = input_dir / f"2024-02-{day:02d}.csv.gz"
            day_df = df[df["day"] == day]
            day_df.to_csv(
                filename, columns=output_columns, index=False, compression="gzip"
            )

        # These won't match anything, but we need them to exist for the asset to run
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

        expected = {
            0: {
                "secondary_substation_id": "001",
                "lv_feeder_id": "01",
                "data_collection_log_timestamp": pd.Timestamp(
                    "2024-02-01T00:30:00+00:00", tz="UTC"
                ),
            },
            1: {
                "secondary_substation_id": "001",
                "lv_feeder_id": "02",
                "data_collection_log_timestamp": pd.Timestamp(
                    "2024-02-01T00:30:00+00:00", tz="UTC"
                ),
            },
            2: {
                "secondary_substation_id": "002",
                "lv_feeder_id": "01",
                "data_collection_log_timestamp": pd.Timestamp(
                    "2024-02-01T00:30:00+00:00", tz="UTC"
                ),
            },
            6: {
                "secondary_substation_id": "001",
                "lv_feeder_id": "01",
                "data_collection_log_timestamp": pd.Timestamp(
                    "2024-02-02T00:30:00+00:00", tz="UTC"
                ),
            },
        }

        for idx, expected_values in expected.items():
            assert df.iloc[idx][expected_values.keys()].to_dict() == expected_values, (
                f"Expected data at {idx} to be sorted by the appropriate columns"
            )


class TestNGEDLVFeederMonthlyParquet:
    def create_part_files(self, tmp_path, month, instance):
        input_dir = tmp_path / "raw" / "nged"
        input_dir.mkdir(parents=True)

        data = nged_lv_feeder_raw_csv_factory(month)
        df = pd.DataFrame.from_records(data=data)
        df_parts = np.array_split(df, 10)
        for part in range(10):
            filename = f"aggregated-smart-meter-data-lv-feeder-2024-{month:02d}-part{part:04d}.csv"
            partition = f"http://example.com/{filename}"
            df_parts[part].to_csv(
                (input_dir / f"{filename}.gz").as_posix(),
                index=False,
                compression="gzip",
            )
            instance.report_runless_asset_event(
                AssetMaterialization(
                    asset_key=AssetKey("nged_lv_feeder_files"), partition=partition
                )
            )

    def test_happy_path(self, tmp_path):
        instance = DagsterInstance.ephemeral()

        self.create_part_files(tmp_path, 1, instance)

        output_dir = tmp_path / "staging" / "nged"
        output_dir.mkdir(parents=True)

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
        assert len(df) == 3 * 2 * 31  # 3 substations * 2 feeders * 31 days
        assert df.columns.tolist() == lv_feeder_parquet_schema.names
        assert result.metadata["dagster/row_count"] == 3 * 2 * 31
        assert result.metadata["weave/nunique_feeders"] == 3 * 2

        # Check conversion of empty string to null and backfill of NGED dno_alias
        missing_data_row = df[df["secondary_substation_id"] == "345678"].iloc[0]
        assert missing_data_row.to_dict() == {
            "dataset_id": None,
            "dno_alias": "NGED",
            "secondary_substation_id": "345678",
            "secondary_substation_name": None,
            "lv_feeder_id": "1",
            "lv_feeder_name": "1",
            "substation_geo_location": None,
            "aggregated_device_count_active": 15.3,
            "total_consumption_active_import": 2331.5,
            "data_collection_log_timestamp": pd.Timestamp(
                "2024-01-01T00:30:00+00:00", tz="UTC"
            ),
            "insert_time": pd.Timestamp("2024-02-14T00:07:06+00:00", tz="UTC"),
            "last_modified_time": pd.Timestamp("2024-02-25T12:46:46+00:00", tz="UTC"),
        }

    def test_when_missing_input(self, tmp_path):
        output_dir = tmp_path / "staging" / "ssen"
        output_dir.mkdir(parents=True)

        # Create the folder but no files in it
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

    def test_sorting_output(self, tmp_path):
        instance = DagsterInstance.ephemeral()

        self.create_part_files(tmp_path, 1, instance)

        output_dir = tmp_path / "staging" / "nged"
        output_dir.mkdir(parents=True)

        context = build_asset_context(partition_key="2024-01-01", instance=instance)
        staging_files_resource = OutputFilesResource(
            url=(tmp_path / "staging").as_uri()
        )
        raw_files_resource = OutputFilesResource(url=(tmp_path / "raw").as_uri())
        nged_api_client = StubNGEDAPIClient()

        nged_lv_feeder_monthly_parquet(
            context, raw_files_resource, staging_files_resource, nged_api_client
        )

        df = pd.read_parquet(output_dir / "2024-01.parquet", engine="pyarrow")
        assert len(df) == 6 * 31  # 3 substations * 2 feeders * 31 days

        expected = {
            0: {
                "secondary_substation_id": "123456",
                "lv_feeder_id": "1",
                "data_collection_log_timestamp": pd.Timestamp(
                    "2024-01-01T00:30:00+00:00", tz="UTC"
                ),
            },
            1: {
                "secondary_substation_id": "123456",
                "lv_feeder_id": "2",
                "data_collection_log_timestamp": pd.Timestamp(
                    "2024-01-01T00:30:00+00:00", tz="UTC"
                ),
            },
            2: {
                "secondary_substation_id": "234567",
                "lv_feeder_id": "1",
                "data_collection_log_timestamp": pd.Timestamp(
                    "2024-01-01T00:30:00+00:00", tz="UTC"
                ),
            },
            6: {
                "secondary_substation_id": "123456",
                "lv_feeder_id": "1",
                "data_collection_log_timestamp": pd.Timestamp(
                    "2024-01-02T00:30:00+00:00", tz="UTC"
                ),
            },
        }

        for idx, expected_values in expected.items():
            assert df.iloc[idx][expected_values.keys()].to_dict() == expected_values, (
                f"Expected data at {idx} to be sorted by the appropriate columns"
            )


def nged_lv_feeder_raw_csv_factory(month: int):
    """Very basic test data factory for NGED LV Feeder raw CSV part files.

    Makes one row of data per day of the month, for 3 substations, with two
    feeders per substation.

    The data is ordered as it is in the raw files, i.e. grouped (but not ordered) by
    substation id, then ordered by feeder id, and then by timestamp.

    Each substation is located at a different lat/lng, with one missing data typical of
    NGED: dataset_id, dno_name, dno_alias, substation name & location."""

    substation_ids = ["234567", "123456", "345678"]
    substation_names = [
        "Donnington Wood Church - Fs Tran",
        "LOWER END WAVENDON - TRAN",
        "",
    ]
    feeder_ids = ["1", "2"]
    locations = ["52.7149, -2.4298", "52.0332, -.6461", ", "]

    data = []

    days_in_month = calendar.monthrange(2024, month)[1]
    month_abbr = calendar.month_abbr[month]

    for substation_idx, substation_id in enumerate(substation_ids):
        for feeder_id in feeder_ids:
            for day in range(1, days_in_month + 1):
                # Replicate missing data pattern typical of NGED
                substation_name = substation_names[substation_idx]
                dataset_id = (
                    f"NGED_{substation_id}_{feeder_id}_{month_abbr}_2024"
                    if substation_name
                    else ""
                )
                dno_alias = "NGED" if substation_name else ""
                dno_name = (
                    "National Grid Electricity Distribution" if substation_name else ""
                )
                data.append(
                    {
                        "dataset_id": dataset_id,
                        "dno_name": dno_name,
                        "dno_alias": dno_alias,
                        "secondary_substation_id": substation_id,
                        "secondary_substation_name": substation_names[substation_idx],
                        "LV_feeder_ID": feeder_id,
                        "LV_feeder_name": feeder_id,
                        "substation_geo_location": locations[substation_idx],
                        "aggregated_device_count_Active": "15.3",
                        "Total_consumption_active_import": "2331.5",
                        "data_collection_log_timestamp": f"2024-{month:02d}-{day:02d}T00:30:00+00:00",
                        "Insert_time": f"2024-{month + 1:02d}-14T00:07:06+00:00",
                        "last_modified_time": f"2024-{month + 1:02d}-25T12:46:46+00:00",
                    }
                )
    return data


def ssen_lv_feeder_raw_csv_factory(month: int):
    """Very basic test data factory for SSEN LV Feeder raw CSV files.

    Makes one row of data per day of the given month, for 3 substations, with two
    feeders per substation.

    The data is ordered as it is in the raw CSV files, i.e. by susbstation id, feeder
    and then timestamp.

    Each substation is located at a different lat/lng, with one missing a location."""

    substation_ids = ["001", "002", "003"]
    substation_names = ["DUNCRAIG ROAD", "MANOR FARM", "PLAYING PLACE"]
    feeder_ids = ["01", "02"]
    locations = ["50.79,-2.43", "51.79,-2.43", None]

    data = []

    days_in_month = calendar.monthrange(2024, month)[1]

    for substation_idx, substation_id in enumerate(substation_ids):
        for feeder_id in feeder_ids:
            for day in range(1, days_in_month + 1):
                data.append(
                    {
                        "dataset_id": f"0002002{substation_id}{feeder_id}",
                        "dno_name": "Scottish and Southern Electricity Networks",
                        "dno_alias": "SSEN",
                        "secondary_substation_id": substation_id,
                        "secondary_substation_name": substation_names[substation_idx],
                        "lv_feeder_id": feeder_id,
                        "lv_feeder_name": "",
                        "substation_geo_location": locations[substation_idx],
                        "aggregated_device_count_active": "15.3",
                        "total_consumption_active_import": "2331.5",
                        "data_collection_log_timestamp": f"2024-{month:02d}-{day:02d}T00:30:00.000Z",
                        "insert_time": f"2024-{month + 1:02d}-14T00:07:06.000Z",
                        "last_modified_time": f"2024-{month + 1:02d}-25T12:46:46.000Z",
                    }
                )
    return data
