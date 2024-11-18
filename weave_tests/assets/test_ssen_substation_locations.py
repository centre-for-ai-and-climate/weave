import os

import pandas as pd
from dagster import build_asset_context

from weave.assets.ssen_substation_locations import (
    ssen_lv_feeder_postcode_mapping,
    ssen_transformer_load_model,
)
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
