import os
import shutil

import pandas as pd
import pytest
from dagster import build_asset_context
from pandas.testing import assert_frame_equal

from weave.assets.ssen_substation_locations import (
    ssen_lv_feeder_postcode_mapping,
    ssen_substation_location_lookup_feeder_postcodes,
    ssen_substation_location_lookup_transformer_load_model,
    ssen_transformer_load_model,
)
from weave.resources.ons import StubONSAPIClient
from weave.resources.output_files import OutputFilesResource
from weave.resources.ssen import StubSSENAPICLient

FIXTURE_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    "..",
    "fixtures",
)


def test_ssen_lv_feeder_postcode_mapping(tmp_path):
    output_dir = tmp_path / "ssen"
    output_dir.mkdir()

    context = build_asset_context()
    raw_files_resource = OutputFilesResource(url=tmp_path.as_uri())
    ssen_api_client = StubSSENAPICLient(
        file_to_download=os.path.join(
            FIXTURE_DIR, "ssen", "lv_feeder_postcode_mapping.csv"
        ),
    )
    ssen_lv_feeder_postcode_mapping(context, raw_files_resource, ssen_api_client)

    df = pd.read_csv(
        (output_dir / "lv_feeder_postcode_mapping.csv.gz").as_posix(), dtype="string"
    )
    assert len(df) == 9
    assert df.iloc[0].dataset_id == "000200200402"


def test_ssen_transformer_load_model(tmp_path):
    output_dir = tmp_path / "ssen"
    output_dir.mkdir()

    context = build_asset_context()
    raw_files_resource = OutputFilesResource(url=tmp_path.as_uri())
    ssen_api_client = StubSSENAPICLient(
        file_to_download=os.path.join(
            FIXTURE_DIR, "ssen", "transformer_load_model.zip"
        ),
    )
    ssen_transformer_load_model(context, raw_files_resource, ssen_api_client)

    with open(output_dir / "transformer_load_model.zip", "rb") as f:
        df = ssen_api_client.transformer_load_model_dataframe(f)
        assert len(df) == 10


@pytest.mark.parametrize(
    "ssen_lookup_data, onspd_data, expected",
    [
        [
            # Single point location for substation with one feeder postcode
            {
                "primary_substation_id": ["0001"],
                "hv_feeder_id": ["001"],
                "secondary_substation_id": ["001"],
                "postcode": ["AB1 0AA"],
            },
            {"pcd": ["AB1 0AA"], "lat": [57.101474], "long": [-2.242851]},
            {
                "substation_nrn": ["0001001001"],
                "substation_geo_location": ["57.101474,-2.242851"],
            },
        ],
        [
            # Line location for substation with two feeder postcodes
            {
                "primary_substation_id": ["0001", "0001"],
                "hv_feeder_id": ["001", "001"],
                "secondary_substation_id": ["001", "001"],
                "postcode": ["AB1 0AA", "AB1 0AB"],
            },
            {
                "pcd": ["AB1 0AA", "AB1 0AB"],
                "lat": [57.101474, 57.101474],
                "long": [-2.242851, -2.242853],
            },
            {
                "substation_nrn": ["0001001001"],
                # Centre of the line
                # 2--+--1
                "substation_geo_location": ["57.101474,-2.242852"],
            },
        ],
        [
            # Polygon location for substation with four feeders
            {
                "primary_substation_id": ["0001", "0001", "0001", "0001"],
                "hv_feeder_id": ["001", "001", "001", "001"],
                "secondary_substation_id": ["001", "001", "001", "001"],
                "postcode": ["AB1 0AA", "AB1 0AB", "AB1 0AC", "AB1 0AD"],
            },
            {
                "pcd": ["AB1 0AA", "AB1 0AB", "AB1 0AC", "AB1 0AD"],
                "lat": [57.101474, 57.101474, 57.101472, 57.101472],
                "long": [-2.242851, -2.242853, -2.242851, -2.242853],
            },
            {
                "substation_nrn": ["0001001001"],
                # Centre of the box
                # 1---------2
                # |    +    |
                # 3---------4
                "substation_geo_location": ["57.101473,-2.242852"],
            },
        ],
        [
            # Substations with bad/missing postcodes in feeder lookup
            {
                "primary_substation_id": ["0001", "0001", "0001"],
                "hv_feeder_id": ["001", "001", "001"],
                "secondary_substation_id": ["001", "002", "003"],
                "postcode": ["AB1 0AA", "AB1", None],
            },
            {"pcd": ["AB1 0AA"], "lat": [57.101474], "long": [-2.242851]},
            {
                # No row for 0001001002 or 0001001003
                "substation_nrn": ["0001001001"],
                "substation_geo_location": ["57.101474,-2.242851"],
            },
        ],
        [
            # Postcode missing in ONSPD
            {
                "primary_substation_id": ["0001", "0001"],
                "hv_feeder_id": ["001", "001"],
                "secondary_substation_id": ["001", "002"],
                "postcode": ["AB1 0AA", "AB1 0AB"],
            },
            {"pcd": ["AB1 0AA"], "lat": [57.101474], "long": [-2.242851]},
            {
                # No row for 0001001002
                "substation_nrn": ["0001001001"],
                "substation_geo_location": ["57.101474,-2.242851"],
            },
        ],
        [
            # Mismatched postcode formatting
            {
                "primary_substation_id": ["0001"],
                "hv_feeder_id": ["001"],
                "secondary_substation_id": ["001"],
                "postcode": ["ab1  0aa"],
            },
            {"pcd": ["AB1 0AA"], "lat": [57.101474], "long": [-2.242851]},
            {
                "substation_nrn": ["0001001001"],
                "substation_geo_location": ["57.101474,-2.242851"],
            },
        ],
        [
            # Two postcodes more than 100m apart
            {
                "primary_substation_id": ["0001", "0001", "0001"],
                "hv_feeder_id": ["001", "001", "001"],
                "secondary_substation_id": ["001", "002", "002"],
                "postcode": ["AB1 0AA", "AB1 0AB", "AB1 0AC"],
            },
            {
                "pcd": ["AB1 0AA", "AB1 0AB", "AB1 0AC"],
                "lat": [57.101474, 57.101474, 57.101474],
                # 100m is approx 0.001654 degrees at 57.101474
                "long": [-2.242851, -2.242853, (-2.242853 + 0.0017)],
            },
            {
                "substation_nrn": ["0001001001"],
                "substation_geo_location": ["57.101474,-2.242851"],
            },
        ],
        [
            # Polygon more than 10000sqm in area (but not > 100m apart at any point)
            {
                "primary_substation_id": [
                    "0001",
                    "0001",
                    "0001",
                    "0001",
                    "0001",
                    "0001",
                ],
                "hv_feeder_id": ["001", "001", "001", "001", "001", "001"],
                "secondary_substation_id": ["001", "002", "002", "002", "002", "002"],
                "postcode": [
                    "AB1 0AA",
                    "AB1 0AB",
                    "AB1 0AC",
                    "AB1 0AD",
                    "AB1 0AE",
                    "AB1 0AF",
                ],
            },
            {
                # Box is a bit more than 10,000 sqm
                #      1
                #    /   \
                #   /     \
                # 2    +    3
                # |         |
                # 4---------5
                "pcd": [
                    "AB1 0AA",
                    "AB1 0AB",
                    "AB1 0AC",
                    "AB1 0AD",
                    "AB1 0AE",
                    "AB1 0AF",
                ],
                "lat": [
                    57.101474,
                    # Polygon starts here
                    # 100m of latitude is approx 0.000898 degrees
                    57.101474 + (0.0008 / 2),
                    57.101474,
                    57.101474,
                    57.101474 - 0.0008,
                    57.101474 - 0.0008,
                ],
                "long": [
                    -2.242851,
                    # Polygon starts here
                    # 100m is approx 0.001654 degrees at 57.101474
                    -2.242851 + (0.0016 / 2),
                    -2.242851,
                    -2.242851 + 0.0016,
                    -2.242851,
                    -2.242851 + 0.0016,
                ],
            },
            {
                "substation_nrn": ["0001001001"],
                # 0001001002 is excluded
                "substation_geo_location": ["57.101474,-2.242851"],
            },
        ],
        [
            # Polygon location where the polygon is less than 10,000sqm but
            # the points are more than 100m apart
            # 1---------------2
            # |               |
            # 3---------------4
            {
                "primary_substation_id": ["0001", "0001", "0001", "0001", "0001"],
                "hv_feeder_id": ["0001", "001", "001", "001", "001"],
                "secondary_substation_id": ["001", "002", "002", "002", "002"],
                "postcode": ["AB1 0AA", "AB1 0AB", "AB1 0AC", "AB1 0AD", "AB1 0AE"],
            },
            {
                "pcd": ["AB1 0AA", "AB1 0AB", "AB1 0AC", "AB1 0AD", "AB1 0AE"],
                "lat": [57.101474, 57.101474, 57.101474, 57.101473, 57.101473],
                # 100m is approx 0.001654 degrees at 57.101474
                "long": [
                    -2.242851,
                    -2.242851,
                    -2.242851 + (0.0017 * 4),
                    -2.242851,
                    -2.242851 + (0.0017 * 4),
                ],
            },
            {
                "substation_nrn": ["0001001001"],
                # 0001001002 is excluded
                "substation_geo_location": ["57.101474,-2.242851"],
            },
        ],
    ],
    ids=[
        "Single point location for substation with one feeder postcode",
        "Line location for substation with two feeder postcodes",
        "Polygon location for substation with four feeders",
        "Substations with bad/missing postcodes in feeder lookup",
        "Postcode missing in ONSPD",
        "Mismatched postcode formatting",
        "Two postcodes more than 100m apart",
        "Polygon more than 10000sqm in area (but not > 100m apart at any point)",
        "Polygon location where the polygon is less than 10,000sqm but the points are more than 100m apart",
    ],
)
def test_ssen_substation_location_lookup_feeder_postcodes(
    tmp_path, ssen_lookup_data, onspd_data, expected
):
    output_dir = tmp_path / "staging" / "ssen"
    output_dir.mkdir(parents=True)
    ssen_input_dir = tmp_path / "raw" / "ssen"
    ssen_input_dir.mkdir(parents=True)
    ons_input_dir = tmp_path / "raw" / "ons"
    ons_input_dir.mkdir(parents=True)

    staging_files_resource = OutputFilesResource(url=(tmp_path / "staging").as_uri())
    raw_files_resource = OutputFilesResource(url=(tmp_path / "raw").as_uri())

    create_lv_feeder_postcode_mapping(ssen_lookup_data, ssen_input_dir)
    ssen_api_client = StubSSENAPICLient()
    create_onspd(onspd_data, ons_input_dir)
    ons_api_client = StubONSAPIClient()

    context = build_asset_context()

    ssen_substation_location_lookup_feeder_postcodes(
        context,
        raw_files_resource,
        staging_files_resource,
        ssen_api_client,
        ons_api_client,
    )

    df = pd.read_parquet(
        output_dir / "substation_location_lookup_feeder_postcodes.parquet"
    )
    assert_frame_equal(df, pd.DataFrame(expected))


def create_lv_feeder_postcode_mapping(data, input_dir):
    pd.DataFrame(data).to_csv(
        input_dir / "lv_feeder_postcode_mapping.csv.gz",
        index=False,
        compression="gzip",
    )


def create_onspd(data, input_dir):
    data_dir = input_dir / "Data"
    data_dir.mkdir()

    pd.DataFrame(data).to_csv(data_dir / "ONSPD_AUG_2024_UK.csv", index=False)

    shutil.make_archive(
        input_dir / "onspd", "zip", root_dir=input_dir, base_dir=data_dir
    )


@pytest.mark.parametrize(
    "load_model_data, expected",
    [
        [
            {
                "full_nrn": ["0001_001_001", "0001_001_001"],
                "latitude": [57.101671, 57.101671],
                "longitude": [-2.241243, -2.241243],
            },
            {
                "substation_nrn": ["0001001001"],
                # Grid shifted lat/lng
                "substation_geo_location": ["57.101474,-2.242851"],
            },
        ],
        [
            {
                "full_nrn": ["0001_001_001", "0001_001_002"],
                "latitude": [57.101671, None],
                "longitude": [-2.241243, None],
            },
            {
                "substation_nrn": ["0001001001"],
                # Grid shifted lat/lng
                "substation_geo_location": ["57.101474,-2.242851"],
            },
        ],
        [
            {
                "full_nrn": ["1_1_1"],
                "latitude": [57.101671],
                "longitude": [-2.2412431],
            },
            {
                "substation_nrn": ["0001001001"],
                # Grid shifted lat/lng
                "substation_geo_location": ["57.101474,-2.242851"],
            },
        ],
    ],
    ids=[
        "Outputs single value for duplicates in the data",
        "Doesn't output substations with missing location",
        "Reformats the NRN in the output",
    ],
)
def test_ssen_substation_location_lookup_transformer_load_model(
    tmp_path, load_model_data, expected
):
    output_dir = tmp_path / "staging" / "ssen"
    output_dir.mkdir(parents=True)
    input_dir = tmp_path / "raw" / "ssen"
    input_dir.mkdir(parents=True)

    staging_files_resource = OutputFilesResource(url=(tmp_path / "staging").as_uri())
    raw_files_resource = OutputFilesResource(url=(tmp_path / "raw").as_uri())

    create_transformer_load_model(load_model_data, input_dir)
    ssen_api_client = StubSSENAPICLient()

    context = build_asset_context()

    ssen_substation_location_lookup_transformer_load_model(
        context,
        raw_files_resource,
        staging_files_resource,
        ssen_api_client,
    )

    df = pd.read_parquet(
        output_dir / "substation_location_lookup_transformer_load_model.parquet"
    )
    assert_frame_equal(df, pd.DataFrame(expected))


def create_transformer_load_model(data, input_dir):
    pd.DataFrame(data).to_csv(
        input_dir / "SEPD_transformers_open_data_with_nrn.zip",
        index=False,
        compression={
            "method": "zip",
            "archive_name": "SEPD_transformers_open_data_with_nrn.csv",
        },
    )
    pd.DataFrame(data).to_csv(
        input_dir / "SEPD_transformers_open_data.zip",
        index=False,
        compression={
            "method": "zip",
            "archive_name": "SEPD_transformers_open_data.csv",
        },
    )

    shutil.make_archive(input_dir / "transformer_load_model", "zip", root_dir=input_dir)
