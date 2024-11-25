import os
from calendar import monthrange

import geopandas as gpd
import pandas as pd
from dagster import build_asset_context

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
                        "aggregated_device_count_active": 15.0,
                        "total_consumption_active_import": 2331.0,
                        "data_collection_log_timestamp": f"2024-{month:02d}-{day:02d}T00:30:00.000Z",
                        "insert_time": f"2024-{month + 1:02d}-14T00:07:06.000Z",
                        "last_modified_time": f"2024-{month + 1:02d}-25T12:46:46.298Z",
                    }
                )
    return data


def create_monthly_parquet_files(dir):
    for month in range(1, 13):
        data = ssen_lv_feeder_monthly_parquet_factory(month)
        monthly_file = dir / f"2024-{month:02d}.parquet"
        df = pd.DataFrame(data=data)
        df.to_parquet(monthly_file)


def test_lv_feeder_combined_geoparquet(tmp_path):
    input_dir = tmp_path / "staging" / "ssen"
    input_dir.mkdir(parents=True)
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    create_monthly_parquet_files(input_dir)

    context = build_asset_context()
    staging_files_resource = OutputFilesResource(url=(tmp_path / "staging").as_uri())
    output_files_resource = OutputFilesResource(url=(tmp_path / "output").as_uri())

    lv_feeder_combined_geoparquet(
        context, staging_files_resource, output_files_resource
    )

    gdf = gpd.read_parquet(output_dir / "smart-meter.parquet")
    assert len(gdf) == 6 * 366  # 6 feeders  * 366 days (2024 was a leap year)

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

    # TODO:
    # Test expected dtypes
    # Test all the data is there
    # Test geometry is correct (within UK)
    # Test floats coerced to ints properly
    # SSEN-specific tests:
    # - Test substation and feeder ids come from dataset_id
