import os

import fsspec
import pandas as pd
import pytest
import responses
from dagster import build_asset_context

from weave.core import AvailableFile
from weave.resources.ons import LiveONSAPIClient
from weave.resources.ssen import LiveSSENAPIClient

FIXTURE_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    "fixtures",
)


@pytest.fixture
def ssen_files_response():
    with open(os.path.join(FIXTURE_DIR, "ssen_files.json")) as f:
        yield f.read()


@pytest.fixture
def ssen_csv_response():
    with open(os.path.join(FIXTURE_DIR, "ssen_2024-02-12_head.csv")) as f:
        yield f.read()


@pytest.fixture
def ssen_api_client():
    return LiveSSENAPIClient()


@pytest.fixture
def ons_api_client():
    return LiveONSAPIClient()


@pytest.fixture
def onspd_response():
    with open(os.path.join(FIXTURE_DIR, "onspd.zip"), "rb") as f:
        yield f.read()


class TestLiveSSENAPIClient:
    def test_get_available_files(self, ssen_files_response, ssen_api_client):
        with responses.RequestsMock() as mocked_responses:
            mocked_responses.get(
                "https://ssen-smart-meter-prod.datopian.workers.dev/LV_FEEDER_USAGE/",
                body=ssen_files_response,
                status=200,
                content_type="application/json",
            )
            results = ssen_api_client.get_available_files()
            assert len(results) == 228
            assert results[0] == AvailableFile(
                filename="2024-02-12.csv",
                url="https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-12.csv",
            )
            assert results[-1] == AvailableFile(
                filename="2024-09-27.csv",
                url="https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-09-27.csv",
            )

    def test_download_file_gzip(self, tmp_path, ssen_api_client, ssen_csv_response):
        downloaded_file = (tmp_path / "downloaded.csv.gz").as_uri()
        context = build_asset_context()
        with responses.RequestsMock() as mocked_responses:
            mocked_responses.get(
                "https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-12.csv",
                body=ssen_csv_response,
                status=200,
            )
            with fsspec.open(downloaded_file, "wb") as f:
                ssen_api_client.download_file(
                    context=context,
                    url="https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-12.csv",
                    output_file=f,
                )
            # Using Pandas to read the gzipped CSV file to ensure it's valid
            df = pd.read_csv(downloaded_file)
            assert len(df) == 10

    def test_download_file_no_gzip(self, tmp_path, ssen_api_client, ssen_csv_response):
        downloaded_file = (tmp_path / "downloaded.csv").as_uri()
        context = build_asset_context()
        with responses.RequestsMock() as mocked_responses:
            mocked_responses.get(
                "https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-12.csv",
                body=ssen_csv_response,
                status=200,
            )
            with fsspec.open(downloaded_file, "wb") as f:
                ssen_api_client.download_file(
                    context=context,
                    url="https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-12.csv",
                    output_file=f,
                    gzip=False,
                )
            # Using Pandas to read the gzipped CSV file to ensure it's valid
            df = pd.read_csv(downloaded_file)
            assert len(df) == 10

    def test_lv_feeder_postcode_lookup_dataframe(self, ssen_api_client):
        input_file = os.path.join(FIXTURE_DIR, "ssen_lv_feeder_postcode_mapping.csv.gz")
        df = ssen_api_client.lv_feeder_postcode_lookup_dataframe(input_file)
        assert len(df) == 9
        assert df["dataset_id"].dtype == "string", "dataset_id should be string"

    def test_transformer_load_model_dataframe(self, ssen_api_client):
        input_file = os.path.join(FIXTURE_DIR, "ssen_transformer_load_model.zip")
        with open(input_file, "rb") as f:
            df = ssen_api_client.transformer_load_model_dataframe(
                f, cols=["full_nrn", "latitude", "longitude"]
            )
            assert len(df) == 10
            assert list(df.columns) == [
                "full_nrn",
                "latitude",
                "longitude",
            ], "should only parse specified columns"
            assert df["full_nrn"].dtype == "string", "full_nrn should be string"
            assert df["latitude"].dtype == "float", "latitude should be float"
            assert df["longitude"].dtype == "float", "longitude should be float"


class TestLiveONSClient:
    def test_download_onspd(self, tmp_path, ons_api_client, onspd_response):
        downloaded_file = (tmp_path / "onspd.zip").as_uri()
        context = build_asset_context()
        with responses.RequestsMock() as mocked_responses:
            mocked_responses.get(
                "https://www.arcgis.com/sharing/rest/content/items/265778cd85754b7e97f404a1c63aea04/data",
                body=onspd_response,
                status=200,
            )
            with fsspec.open(downloaded_file, "wb") as f:
                ons_api_client.download_onspd(context=context, output_file=f)

            with fsspec.open(downloaded_file, "rb") as f:
                df = ons_api_client.onspd_dataframe(f)
                assert len(df) == 9

    def test_onspd_dataframe(self, ons_api_client):
        with open(os.path.join(FIXTURE_DIR, "onspd.zip"), "rb") as f:
            df = ons_api_client.onspd_dataframe(f, cols=["pcd", "lat", "long"])
            assert len(df) == 9
            assert list(df.columns) == [
                "pcd",
                "lat",
                "long",
            ], "should only parse specified columns"
            assert df["pcd"].dtype == "string", "pcd should be string"
            assert df["lat"].dtype == "float", "lat should be float"
            assert df["long"].dtype == "float", "long should be float"
            df.set_index("pcd", inplace=True)
            assert pd.isna(
                df.loc["AB1 0AN"].lat
            ), "99.999999 lats should be parsed as NaN"
            assert pd.isna(
                df.loc["AB1 0AN"].long
            ), "0.000000 longs should parsed as NaN"
