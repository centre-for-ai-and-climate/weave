import os
from datetime import datetime, timezone

import fsspec
import pandas as pd
import pytest
import responses
from dagster import build_asset_context
from responses import matchers
from zlib_ng import zlib_ng

from weave.core import AvailableFile
from weave.resources.nged import LiveNGEDAPIClient
from weave.resources.ons import LiveONSAPIClient
from weave.resources.ssen import LiveSSENAPIClient

FIXTURE_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    "fixtures",
)


class TestLiveSSENAPIClient:
    @pytest.fixture
    def files_response(self):
        with open(os.path.join(FIXTURE_DIR, "ssen", "available_files.json")) as f:
            yield f.read()

    @pytest.fixture
    def csv_response(self):
        with open(
            os.path.join(FIXTURE_DIR, "ssen", "lv_feeder_files", "2024-02-12_head.csv")
        ) as f:
            yield f.read()

    @pytest.fixture
    def api_client(self):
        return LiveSSENAPIClient()

    def test_get_available_files(self, files_response, api_client):
        with responses.RequestsMock() as mocked_responses:
            mocked_responses.get(
                "https://ssen-smart-meter-prod.datopian.workers.dev/LV_FEEDER_USAGE/",
                body=files_response,
                status=200,
                content_type="application/json",
            )
            results = api_client.get_available_files()
            assert len(results) == 228
            assert results[0] == AvailableFile(
                filename="2024-02-12.csv",
                url="https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-12.csv",
            )
            assert results[-1] == AvailableFile(
                filename="2024-09-27.csv",
                url="https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-09-27.csv",
            )

    def test_download_file_gzip(self, tmp_path, api_client, csv_response):
        downloaded_file = (tmp_path / "downloaded.csv.gz").as_uri()
        context = build_asset_context()
        with responses.RequestsMock() as mocked_responses:
            mocked_responses.get(
                "https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-12.csv",
                body=csv_response,
                status=200,
            )
            with fsspec.open(downloaded_file, "wb") as f:
                api_client.download_file(
                    context=context,
                    url="https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-12.csv",
                    output_file=f,
                )
            # Using Pandas to read the gzipped CSV file to ensure it's valid
            df = pd.read_csv(downloaded_file)
            assert len(df) == 10

    def test_download_file_no_gzip(self, tmp_path, api_client, csv_response):
        downloaded_file = (tmp_path / "downloaded.csv").as_uri()
        context = build_asset_context()
        with responses.RequestsMock() as mocked_responses:
            mocked_responses.get(
                "https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-12.csv",
                body=csv_response,
                status=200,
            )
            with fsspec.open(downloaded_file, "wb") as f:
                api_client.download_file(
                    context=context,
                    url="https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-12.csv",
                    output_file=f,
                    gzip=False,
                )
            # Using Pandas to read the CSV file to ensure it's valid
            df = pd.read_csv(downloaded_file)
            assert len(df) == 10

    def test_lv_feeder_postcode_lookup_dataframe(self, api_client):
        input_file = os.path.join(
            FIXTURE_DIR, "ssen", "lv_feeder_postcode_mapping.csv.gz"
        )
        df = api_client.lv_feeder_postcode_lookup_dataframe(input_file)
        assert len(df) == 9
        assert df["dataset_id"].dtype == "string", "dataset_id should be string"

    def test_transformer_load_model_dataframe(self, api_client):
        input_file = os.path.join(FIXTURE_DIR, "ssen", "transformer_load_model.zip")
        with open(input_file, "rb") as f:
            df = api_client.transformer_load_model_dataframe(
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

    def test_lv_feeder_file_pyarrow_table(self, tmp_path, api_client):
        input_file = os.path.join(
            FIXTURE_DIR, "ssen", "lv_feeder_files", "2024-02-12_head.csv"
        )
        with open(input_file, "rb") as f:
            gzipped_file = tmp_path / "2024-02-12.csv.gz"
            with open(gzipped_file, "wb") as output_file:
                output_file.write(
                    zlib_ng.compress(f.read(), level=1, wbits=zlib_ng.MAX_WBITS | 16)
                )

        with open(gzipped_file, "rb") as f:
            table = api_client.lv_feeder_file_pyarrow_table(f)
            assert table.num_rows == 10


class TestLiveONSClient:
    @pytest.fixture
    def api_client(self):
        return LiveONSAPIClient()

    @pytest.fixture
    def onspd_response(self):
        with open(os.path.join(FIXTURE_DIR, "onspd.zip"), "rb") as f:
            yield f.read()

    def test_download_onspd(self, tmp_path, api_client, onspd_response):
        downloaded_file = (tmp_path / "onspd.zip").as_uri()
        context = build_asset_context()
        with responses.RequestsMock() as mocked_responses:
            mocked_responses.get(
                "https://www.arcgis.com/sharing/rest/content/items/265778cd85754b7e97f404a1c63aea04/data",
                body=onspd_response,
                status=200,
            )
            with fsspec.open(downloaded_file, "wb") as f:
                api_client.download_onspd(context=context, output_file=f)

            with fsspec.open(downloaded_file, "rb") as f:
                df = api_client.onspd_dataframe(f)
                assert len(df) == 9

    def test_onspd_dataframe(self, api_client):
        with open(os.path.join(FIXTURE_DIR, "onspd.zip"), "rb") as f:
            df = api_client.onspd_dataframe(f, cols=["pcd", "lat", "long"])
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
            assert pd.isna(df.loc["AB1 0AN"].lat), (
                "99.999999 lats should be parsed as NaN"
            )
            assert pd.isna(df.loc["AB1 0AN"].long), (
                "0.000000 longs should parsed as NaN"
            )


class TestLiveNGEDAPIClient:
    @pytest.fixture
    def datapackage_response(self):
        with open(os.path.join(FIXTURE_DIR, "nged", "datapackage.json")) as f:
            yield f.read()

    @pytest.fixture
    def api_client(self):
        return LiveNGEDAPIClient(api_token="TEST")

    @pytest.fixture
    def csv_response(self):
        with open(
            os.path.join(
                FIXTURE_DIR,
                "nged",
                "lv_feeder_files",
                "aggregated-smart-meter-data-lv-feeder-2024-01-part0000_head.csv.gz",
            ),
            "rb",
        ) as f:
            yield f.read()

    def test_get_available_files(self, datapackage_response, api_client):
        with responses.RequestsMock() as mocked_responses:
            mocked_responses.get(
                "https://connecteddata.nationalgrid.co.uk/dataset/aggregated-smart-meter-data-lv-feeder/datapackage.json",
                body=datapackage_response,
                status=200,
                content_type="application/json",
                match=[matchers.header_matcher({"Authorization": "TEST"})],
            )
            results = api_client.get_available_files()
            assert len(results) == 1261
            assert results[0] == AvailableFile(
                filename="aggregated-smart-meter-data-lv-feeder-2024-01-part0000.csv.gz",
                url="https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/83fc80ad-954a-4a12-9760-2c6a0854f029/download/aggregated-smart-meter-data-lv-feeder-2024-01-part0000.csv.gz",
                created=datetime(2025, 3, 24, 20, 29, 3, 194540, tzinfo=timezone.utc),
            )
            assert results[-1] == AvailableFile(
                filename="aggregated-smart-meter-data-lv-feeder-2025-04-part0162.csv.gz",
                url="https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/b82a3a6f-18c2-47aa-83f1-9c1c3e0b43a6/download/aggregated-smart-meter-data-lv-feeder-2025-04-part0162.csv.gz",
                created=datetime(2025, 5, 31, 18, 49, 34, 531661, tzinfo=timezone.utc),
            )

    def test_download_file_gzip(self, tmp_path, api_client, csv_response):
        downloaded_file = (tmp_path / "downloaded.csv.gz").as_uri()
        context = build_asset_context()
        with responses.RequestsMock() as mocked_responses:
            mocked_responses.get(
                "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/105a7821-7f5c-4591-90e8-5915f253b1ff/download/aggregated-smart-meter-data-lv-feeder-2024-01-part0000.csv",
                body=csv_response,
                status=200,
                match=[matchers.header_matcher({"Authorization": "TEST"})],
            )
            with fsspec.open(downloaded_file, "wb") as f:
                api_client.download_file(
                    context=context,
                    url="https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/105a7821-7f5c-4591-90e8-5915f253b1ff/download/aggregated-smart-meter-data-lv-feeder-2024-01-part0000.csv",
                    output_file=f,
                )
            # Using Pandas to read the gzipped CSV file to ensure it's valid
            df = pd.read_csv(downloaded_file)
            assert len(df) == 10

    def test_lv_feeder_file_pyarrow_table(self, tmp_path, api_client):
        gzipped_file = os.path.join(
            FIXTURE_DIR,
            "nged",
            "lv_feeder_files",
            "aggregated-smart-meter-data-lv-feeder-2024-01-part0000_head.csv.gz",
        )

        with open(gzipped_file, "rb") as f:
            table = api_client.lv_feeder_file_pyarrow_table(f)
            assert table.num_rows == 10
