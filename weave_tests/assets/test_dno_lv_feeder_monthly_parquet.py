import os

import pandas as pd
from dagster import build_asset_context
from zlib_ng import zlib_ng

from weave.assets.dno_lv_feeder_monthly_parquet import ssen_lv_feeder_monthly_parquet
from weave.resources.output_files import OutputFilesResource

FIXTURE_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    "..",
    "fixtures",
)


def create_daily_files(year, month, start, end, dir):
    # 4 different secondary substations with 2 feeders each, one will have locations
    # from the postcodes lookup and the other from the transformer load model, the final
    # one will remain blank
    input_file = os.path.join(
        FIXTURE_DIR, "ssen", "lv_feeder_files", "2024-02-12_for_location_lookup.csv"
    )
    for day in range(start, end):
        filename = dir / f"{year}-{month:02d}-{day:02d}.csv.gz"
        with open(filename.as_posix(), "wb") as output_file:
            with open(input_file, "rb") as f:
                output_file.write(
                    zlib_ng.compress(f.read(), level=1, wbits=zlib_ng.MAX_WBITS | 16)
                )


def create_substation_location_lookup_postcodes(dir):
    data = {
        "substation_nrn": ["0002002004", "0002002009"],
        "substation_geo_location": ["50.79,-2.43", "49.79,-1.43"],
    }
    df = pd.DataFrame(data=data)
    df.to_parquet(dir / "substation_location_lookup_feeder_postcodes.parquet")


def substation_location_lookup_load_model(dir):
    data = {
        # Only one substation in the lookup so we can we fall back to the postcode lookup
        "substation_nrn": ["0002002004"],
        # Different location for the first to the postcode lookup so we can tell its been used
        "substation_geo_location": ["51.79,-2.53"],
    }
    df = pd.DataFrame(data=data)
    df.to_parquet(dir / "substation_location_lookup_transformer_load_model.parquet")


def test_ssen_lv_feeder_monthly_parquet(tmp_path):
    output_dir = tmp_path / "staging" / "ssen"
    output_dir.mkdir(parents=True)
    input_dir = tmp_path / "raw" / "ssen"
    input_dir.mkdir(parents=True)

    create_daily_files(2024, 2, 1, 30, input_dir)  # 2024 was a leap year hence 29 days
    create_substation_location_lookup_postcodes(output_dir)
    substation_location_lookup_load_model(output_dir)

    context = build_asset_context(partition_key="2024-02-01")
    staging_files_resource = OutputFilesResource(url=(tmp_path / "staging").as_uri())
    raw_files_resource = OutputFilesResource(url=(tmp_path / "raw").as_uri())

    ssen_lv_feeder_monthly_parquet(context, raw_files_resource, staging_files_resource)

    df = pd.read_parquet(output_dir / "2024-02.parquet", engine="pyarrow")
    assert len(df) == 6 * 29  # 6 rows in the fixture * 29 days
    # Locations in the load model should override the postcode lookup
    assert (
        df[df["dataset_id"] == "000200200402"].iloc[0].substation_geo_location
        == "51.79,-2.53"
    )
    # All feeders should get the same substation location
    assert (
        df[df["dataset_id"] == "000200200404"].iloc[0].substation_geo_location
        == "51.79,-2.53"
    )
    # If there's no location in the load model we should use the postcode lookup
    assert (
        df[df["dataset_id"] == "000200200901"].iloc[0].substation_geo_location
        == "49.79,-1.43"
    )
    # All feeders should get the same substation location, even when using the postcode
    # lookup
    assert (
        df[df["dataset_id"] == "000200200902"].iloc[0].substation_geo_location
        == "49.79,-1.43"
    )
    # If there's no location in the load model or postcode lookup it should stay blank
    assert (
        df[df["dataset_id"] == "000200202002"].iloc[0].substation_geo_location is None
    )
    # All feeders should get a blank substation location if there is none in either
    # lookup
    assert (
        df[df["dataset_id"] == "000200202003"].iloc[0].substation_geo_location is None
    )
