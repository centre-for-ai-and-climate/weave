import calendar
import os
from calendar import monthrange
from datetime import datetime, timezone

import geopandas as gpd
import pandas as pd
import pyproj
from dagster import build_asset_context
from shapely import Point

from weave.assets.dno_lv_feeder_combined_geoparquet import lv_feeder_combined_geoparquet
from weave.resources.output_files import OutputFilesResource

FIXTURE_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    "..",
    "fixtures",
)


def nged_lv_feeder_monthly_parquet_factory(month: int):
    """Very basic test data factory for NGED LV Feeder mothly parquet files.

    Makes one row of data per day of the given month, for 3 substations, with two
    feeders per substation.

    Each substation is located at a different lat/lng, with missing data typical of
    NGED: substation name & location."""

    substation_ids = ["123456", "234567", "345678"]
    substation_names = [
        "LOWER END WAVENDON - TRAN",
        "Donnington Wood Church - Fs Tran",
        "",
    ]
    feeder_ids = ["1", "2"]
    locations = ["52.0332, -.6461", "52.7149, -2.4298", None]

    data = []

    days_in_month = monthrange(2024, month)[1]
    month_abbr = calendar.month_abbr[month]

    for day in range(1, days_in_month + 1):
        for substation_idx, substation_id in enumerate(substation_ids):
            for feeder_id in feeder_ids:
                data.append(
                    {
                        "dataset_id": f"NGED_{substation_id}_{feeder_id}_{month_abbr}_2024",
                        "dno_name": "National Grid Electricity Distribution",
                        "dno_alias": "NGED",
                        "secondary_substation_id": substation_id,
                        "secondary_substation_name": substation_names[substation_idx],
                        "lv_feeder_id": feeder_id,
                        "lv_feeder_name": feeder_id,
                        "substation_geo_location": locations[substation_idx],
                        "aggregated_device_count_active": 15.3,
                        "total_consumption_active_import": 2331.5,
                        "data_collection_log_timestamp": datetime(
                            2024, month, day, 0, 30, 0, 0, tzinfo=timezone.utc
                        ),
                        "insert_time": datetime(
                            2024, month + 1, 14, 0, 7, 6, 0, tzinfo=timezone.utc
                        ),
                        "last_modified_time": datetime(
                            2024, month + 1, 25, 12, 46, 46, 0, tzinfo=timezone.utc
                        ),
                    }
                )
    return data


def ssen_lv_feeder_monthly_parquet_factory(month: int):
    """Very basic test data factory for SSEN LV Feeder mothly parquet files.

    Makes one row of data per day of the given month, for 3 substations, with two
    feeders per substation.

    Each substation is located at a different lat/lng, with one missing a location."""

    substation_ids = ["001", "002", "003"]
    substation_names = ["DUNCRAIG ROAD", "MANOR FARM", "PLAYING PLACE"]
    feeder_ids = ["01", "02"]
    locations = ["50.79,-2.43", "51.79,-2.43", None]

    data = []

    days_in_month = monthrange(2024, month)[1]

    for day in range(1, days_in_month + 1):
        for substation_idx, substation_id in enumerate(substation_ids):
            for feeder_id in feeder_ids:
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
                        "aggregated_device_count_active": 15.3,
                        "total_consumption_active_import": 2331.5,
                        "data_collection_log_timestamp": datetime(
                            2024, month, day, 0, 30, 0, 0, tzinfo=timezone.utc
                        ),
                        "insert_time": datetime(
                            2024, month + 1, 14, 0, 7, 6, 0, tzinfo=timezone.utc
                        ),
                        "last_modified_time": datetime(
                            2024, month + 1, 25, 12, 46, 46, 0, tzinfo=timezone.utc
                        ),
                    }
                )
    return data


def create_monthly_parquet_files(tmp_path):
    month = 2  # February

    input_dir = tmp_path / "staging" / "nged"
    input_dir.mkdir(parents=True)
    monthly_file = input_dir / f"2024-{month:02d}.parquet"
    data = nged_lv_feeder_monthly_parquet_factory(month)
    df = pd.DataFrame.from_records(data=data)
    df.to_parquet(monthly_file)

    input_dir = tmp_path / "staging" / "ssen"
    input_dir.mkdir(parents=True)
    monthly_file = input_dir / f"2024-{month:02d}.parquet"
    data = ssen_lv_feeder_monthly_parquet_factory(month)
    df = pd.DataFrame.from_records(data=data)
    df.to_parquet(monthly_file)


def test_lv_feeder_combined_geoparquet(tmp_path):
    output_dir = tmp_path / "output" / "smart-meter"
    output_dir.mkdir(parents=True)

    create_monthly_parquet_files(tmp_path)

    context = build_asset_context(partition_key="2024-02-01")
    staging_files_resource = OutputFilesResource(url=(tmp_path / "staging").as_uri())
    output_files_resource = OutputFilesResource(url=(tmp_path / "output").as_uri())

    result = lv_feeder_combined_geoparquet(
        context, staging_files_resource, output_files_resource
    )

    assert result.metadata["dagster/row_count"] == 6 * 29 * 2
    assert result.metadata["weave/nunique_feeders"] == 6 * 2
    assert result.metadata["weave/nunique_substations"] == 3 * 2

    gdf = gpd.read_parquet(tmp_path / "output" / "smart-meter" / "2024-02.parquet")
    assert len(gdf) == 6 * 29 * 2  # 6 feeders * 29 days (2024 was a leap year)

    assert gdf.columns.tolist() == [
        "dataset_id",
        "dno_alias",
        "aggregated_device_count_active",
        "total_consumption_active_import",
        "data_collection_log_timestamp",
        "geometry",
        "secondary_substation_unique_id",
        "lv_feeder_unique_id",
    ]

    # Dataframe has correct geo-metadata
    assert gdf.crs == pyproj.CRS.from_string("EPSG:4326")

    # Dataframe is ordered by:
    # timestamp, dno, substation, feeder
    expected = {
        0: {
            "data_collection_log_timestamp": pd.Timestamp(
                "2024-02-01 00:30:00+0000", tz="UTC"
            ),
            "secondary_substation_unique_id": "NGED-123456",
            "lv_feeder_unique_id": "NGED-123456-1",
        },
        1: {
            "data_collection_log_timestamp": pd.Timestamp(
                "2024-02-01 00:30:00+0000", tz="UTC"
            ),
            "secondary_substation_unique_id": "NGED-123456",
            "lv_feeder_unique_id": "NGED-123456-2",
        },
        2: {
            "data_collection_log_timestamp": pd.Timestamp(
                "2024-02-01 00:30:00+0000", tz="UTC"
            ),
            "secondary_substation_unique_id": "NGED-234567",
            "lv_feeder_unique_id": "NGED-234567-1",
        },
        6: {
            "data_collection_log_timestamp": pd.Timestamp(
                "2024-02-01 00:30:00+0000", tz="UTC"
            ),
            "secondary_substation_unique_id": "SSEN-0002002001",
            "lv_feeder_unique_id": "SSEN-000200200101",
        },
        7: {
            "data_collection_log_timestamp": pd.Timestamp(
                "2024-02-01 00:30:00+0000", tz="UTC"
            ),
            "secondary_substation_unique_id": "SSEN-0002002001",
            "lv_feeder_unique_id": "SSEN-000200200102",
        },
    }

    for idx, expected_values in expected.items():
        assert gdf.iloc[idx][expected_values.keys()].to_dict() == expected_values, (
            f"Expected data at {idx} to be sorted by the appropriate columns"
        )

    # Locations are parsed to geometries correctly (including empty ones)
    assert gdf.iloc[0].geometry == Point(-0.6461, 52.0332)
    assert gdf.iloc[5].geometry == Point()
    assert gdf.iloc[174].geometry == Point(-2.43, 50.79)

    # Numbers are rounded to ints
    assert gdf.iloc[0].aggregated_device_count_active == 15
    assert gdf.iloc[0].total_consumption_active_import == 2332

    # NGED substations have their ids taken from combining the existing ids
    assert gdf.iloc[0].secondary_substation_unique_id == "NGED-123456"
    assert gdf.iloc[0].lv_feeder_unique_id == "NGED-123456-1"

    # SSEN substations have their ids taken from the dataset_id
    assert gdf.iloc[174].secondary_substation_unique_id == "SSEN-0002002001"
    assert gdf.iloc[174].lv_feeder_unique_id == "SSEN-000200200101"


def test_lv_feeder_combined_geoparquet_handles_completely_missing_input(tmp_path):
    input_dir = tmp_path / "staging" / "ssen"
    input_dir.mkdir(parents=True)
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    # We haven't made any monthly files, so there's nothing to combine

    context = build_asset_context(partition_key="2024-02-01")
    staging_files_resource = OutputFilesResource(url=(tmp_path / "staging").as_uri())
    output_files_resource = OutputFilesResource(url=(tmp_path / "output").as_uri())

    # Shouldn't raise, this is an expected situation at the beginning of the month
    lv_feeder_combined_geoparquet(
        context, staging_files_resource, output_files_resource
    )

    assert not (output_dir / "smart-meter" / "2024-02.parquet").exists()


def test_lv_feeder_combined_geoparquet_handles_partially_missing_input(tmp_path):
    ssen_input_dir = tmp_path / "staging" / "ssen"
    ssen_input_dir.mkdir(parents=True)
    nged_input_dir = tmp_path / "staging" / "nged"
    nged_input_dir.mkdir(parents=True)
    output_dir = tmp_path / "output" / "smart-meter"
    output_dir.mkdir(parents=True)

    # Make some data, but not all of it:
    # - No SSEN data, to test a missing DNO
    # - Missing even-numbered days in NGED data, to test missing days
    monthly_file = nged_input_dir / "2024-01.parquet"
    data = nged_lv_feeder_monthly_parquet_factory(1)
    df = pd.DataFrame.from_records(data=data)
    df = df[df["data_collection_log_timestamp"].dt.day % 2 == 1]
    df.to_parquet(monthly_file)

    context = build_asset_context(partition_key="2024-01-01")
    staging_files_resource = OutputFilesResource(url=(tmp_path / "staging").as_uri())
    output_files_resource = OutputFilesResource(url=(tmp_path / "output").as_uri())

    lv_feeder_combined_geoparquet(
        context, staging_files_resource, output_files_resource
    )

    gdf = gpd.read_parquet(tmp_path / "output" / "smart-meter" / "2024-01.parquet")
    assert len(gdf) == 6 * 16  # 6 feeders * 16 odd-numbered days in January
