import os
from calendar import monthrange

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
                        "data_collection_log_timestamp": f"2024-{month:02d}-{day:02d}T00:30:00.000Z",
                        "insert_time": f"2024-{month + 1:02d}-14T00:07:06.000Z",
                        "last_modified_time": f"2024-{month + 1:02d}-25T12:46:46.298Z",
                    }
                )
    return data


def create_monthly_parquet_files(dir, month):
    data = ssen_lv_feeder_monthly_parquet_factory(month)
    monthly_file = dir / f"2024-{month:02d}.parquet"
    df = pd.DataFrame.from_records(data=data)
    df.to_parquet(monthly_file)


def test_lv_feeder_combined_geoparquet(tmp_path):
    input_dir = tmp_path / "staging" / "ssen"
    input_dir.mkdir(parents=True)
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    create_monthly_parquet_files(input_dir, 2)

    context = build_asset_context(partition_key="2024-02-01")
    staging_files_resource = OutputFilesResource(url=(tmp_path / "staging").as_uri())
    output_files_resource = OutputFilesResource(url=(tmp_path / "output").as_uri())

    result = lv_feeder_combined_geoparquet(
        context, staging_files_resource, output_files_resource
    )

    assert result.metadata["dagster/row_count"] == 6 * 29
    assert result.metadata["weave/nunique_feeders"] == 6
    assert result.metadata["weave/nunique_substations"] == 3

    gdf = gpd.read_parquet(output_dir / "smart-meter" / "2024-02.parquet")
    assert len(gdf) == 6 * 29  # 6 feeders * 29 days (2024 was a leap year)

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
    # timestamp, dno (moot atm), substation, feeder
    assert gdf.iloc[0].data_collection_log_timestamp == pd.Timestamp(
        "2024-02-01 00:30:00+0000", tz="UTC"
    )
    assert gdf.iloc[0].secondary_substation_unique_id == "0002002001"
    assert gdf.iloc[0].lv_feeder_unique_id == "000200200101"

    assert gdf.iloc[1].data_collection_log_timestamp == pd.Timestamp(
        "2024-02-01 00:30:00+0000", tz="UTC"
    )
    assert gdf.iloc[1].secondary_substation_unique_id == "0002002001"
    assert gdf.iloc[1].lv_feeder_unique_id == "000200200102"

    assert gdf.iloc[2].data_collection_log_timestamp == pd.Timestamp(
        "2024-02-01 00:30:00+0000", tz="UTC"
    )
    assert gdf.iloc[2].secondary_substation_unique_id == "0002002002"
    assert gdf.iloc[2].lv_feeder_unique_id == "000200200201"

    assert gdf.iloc[-1].data_collection_log_timestamp == pd.Timestamp(
        "2024-02-29 00:30:00+0000", tz="UTC"
    )
    assert gdf.iloc[-1].secondary_substation_unique_id == "0002002003"
    assert gdf.iloc[-1].lv_feeder_unique_id == "000200200302"

    # Locations are parsed to geometries correctly (including empty ones)
    assert gdf.iloc[0].geometry == Point(-2.43, 50.79)
    assert gdf.iloc[2].geometry == Point(-2.43, 51.79)
    assert gdf.iloc[4].geometry == Point()

    # Numbers are rounded to ints
    assert gdf.iloc[0].aggregated_device_count_active == 15
    assert gdf.iloc[0].total_consumption_active_import == 2332

    # SSEN substations have their ids taken from the dataset_id
    assert gdf.iloc[0].dataset_id == "000200200101"
    assert gdf.iloc[0].secondary_substation_unique_id == "0002002001"
    assert gdf.iloc[0].lv_feeder_unique_id == "000200200101"


def test_lv_feeder_combined_geoparquet_handles_missing_input(tmp_path):
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
