import os

import fsspec
import pandas as pd
import pytest
import responses
from dagster import build_asset_context

from weave.core import AvailableFile
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

    def test_download_file(self, tmp_path, ssen_api_client, ssen_csv_response):
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
