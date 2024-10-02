import os
from pathlib import Path

import pytest
import responses
import responses.matchers
from dagster import materialize

from weave.assets.core import DNO
from weave.assets.dno_smart_meter_files import (
    AvailableFile,
    downloaded_files,
    new_nged_files,
    new_npg_files,
    new_ssen_files,
    new_ukpn_files,
    nged_files,
    npg_files,
    ssen_files,
    ukpn_files,
)

FIXTURE_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    "fixtures",
)


@pytest.fixture
def config(tmp_path_factory):
    return {
        "ssen_files": {
            "config": {
                "url": "https://ssen-smart-meter-prod.datopian.workers.dev/LV_FEEDER_USAGE/",
            }
        },
        "npg_files": {
            "config": {"url": "https://example.com/npg", "api_key": "npg_api_key"}
        },
        "nged_files": {
            "config": {"url": "https://example.com/nged", "api_token": "nged_api_token"}
        },
        "ukpn_files": {"config": {"url": "https://example.com/ukpn.csv"}},
        "downloaded_files": {
            "config": {
                "filesystem": "local",
                "root_directory": tmp_path_factory.mktemp("raw-data").as_posix(),
            }
        },
    }


@pytest.fixture
def mocked_responses():
    with responses.RequestsMock() as rsps:
        yield rsps


@pytest.fixture
def ssen_files_response():
    with open(os.path.join(FIXTURE_DIR, "ssen_files.json")) as f:
        yield f.read()


@pytest.fixture
def npg_files_response():
    with open(os.path.join(FIXTURE_DIR, "npg_files.json")) as f:
        yield f.read()


@pytest.fixture
def nged_files_response():
    with open(os.path.join(FIXTURE_DIR, "nged_files.json")) as f:
        yield f.read()


@pytest.fixture
def already_downloaded(config):
    # Put some files in the tmp raw data directory
    root = Path(config["downloaded_files"]["config"]["root_directory"])
    root.joinpath(DNO.SSEN.value).mkdir()
    root.joinpath(DNO.UKPN.value).mkdir()
    root.joinpath(DNO.NPG.value).mkdir()
    root.joinpath(DNO.NGED.value).mkdir()
    root.joinpath(DNO.SSEN.value, "2024-02-12.csv").touch()
    root.joinpath(DNO.SSEN.value, "2024-02-13.csv").touch()
    root.joinpath(DNO.SSEN.value, "2024-02-14.csv").touch()
    root.joinpath(DNO.NPG.value, "npg_feeder_november2023.zip").touch()
    root.joinpath(
        DNO.NGED.value, "aggregated-smart-meter-data-lv-feeder-2024-01-part0000.csv"
    ).touch()


class TestDNOSmartMeterFiles:
    def test_ssen_files(self, config, mocked_responses, ssen_files_response):
        self._mock_ssen_files_response(mocked_responses, ssen_files_response)
        results = sorted(ssen_files(config["ssen_files"]["config"]))
        assert len(results) == 228
        assert results[0] == AvailableFile(
            filename="2024-02-12.csv",
            url="https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-12.csv",
        )
        assert results[-1] == AvailableFile(
            filename="2024-09-27.csv",
            url="https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-09-27.csv",
        )

    def _mock_ssen_files_response(self, mocked_responses, ssen_files_response):
        mocked_responses.get(
            "https://ssen-smart-meter-prod.datopian.workers.dev/LV_FEEDER_USAGE/",
            body=ssen_files_response,
            status=200,
            content_type="application/json",
        )

    def test_ukpn_files(self, config):
        results = ukpn_files(config["ukpn_files"]["config"])
        assert len(results) == 1
        assert results == {
            AvailableFile(filename="ukpn.csv", url="https://example.com/ukpn.csv")
        }

    def test_npg_files(self, config, mocked_responses, npg_files_response):
        self._mock_npg_files_response(npg_files_response, mocked_responses)
        results = npg_files(config["npg_files"]["config"])
        assert results == {
            AvailableFile(
                filename="npg_feeder_november2023.zip",
                url="https://northernpowergrid.opendatasoft.com/api/explore/v2.1/catalog/datasets/aggregated-smart-metering-data/attachments/npg_feeder_november2023_zip",
            ),
            AvailableFile(
                filename="npg_feeder_december2023.zip",
                url="https://northernpowergrid.opendatasoft.com/api/explore/v2.1/catalog/datasets/aggregated-smart-metering-data/attachments/npg_feeder_december2023_zip",
            ),
            AvailableFile(
                filename="npg_feeder_january2024.zip",
                url="https://northernpowergrid.opendatasoft.com/api/explore/v2.1/catalog/datasets/aggregated-smart-metering-data/attachments/npg_feeder_january2024_zip",
            ),
            AvailableFile(
                filename="npg_feeder_february2024.zip",
                url="https://northernpowergrid.opendatasoft.com/api/explore/v2.1/catalog/datasets/aggregated-smart-metering-data/attachments/npg_feeder_february2024_zip",
            ),
            AvailableFile(
                filename="npg_feeder_march2024.zip",
                url="https://northernpowergrid.opendatasoft.com/api/explore/v2.1/catalog/datasets/aggregated-smart-metering-data/attachments/npg_feeder_march2024_zip",
            ),
            AvailableFile(
                filename="npg_feeder_april2024.zip",
                url="https://northernpowergrid.opendatasoft.com/api/explore/v2.1/catalog/datasets/aggregated-smart-metering-data/attachments/npg_feeder_april2024_zip",
            ),
            AvailableFile(
                filename="npg_feeder_may2024.zip",
                url="https://northernpowergrid.opendatasoft.com/api/explore/v2.1/catalog/datasets/aggregated-smart-metering-data/attachments/npg_feeder_may2024_zip",
            ),
            AvailableFile(
                filename="npg_feeder_june2024.zip",
                url="https://northernpowergrid.opendatasoft.com/api/explore/v2.1/catalog/datasets/aggregated-smart-metering-data/attachments/npg_feeder_june2024_zip",
            ),
        }

    def _mock_npg_files_response(self, npg_files_response, mocked_responses):
        expected_auth = {"Authorization": "Apikey npg_api_key"}
        mocked_responses.get(
            "https://example.com/npg",
            body=npg_files_response,
            status=200,
            content_type="application/json",
            match=[responses.matchers.header_matcher(expected_auth)],
        )

    def test_nged_files(self, config, mocked_responses, nged_files_response):
        self._mock_nged_files_response(mocked_responses, nged_files_response)
        results = nged_files(config["nged_files"]["config"])
        assert len(results) == 999
        first = AvailableFile(
            filename="aggregated-smart-meter-data-lv-feeder-2024-01-part0000.csv",
            url="https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/105a7821-7f5c-4591-90e8-5915f253b1ff/download/aggregated-smart-meter-data-lv-feeder-2024-01-part0000.csv",
        )
        last = AvailableFile(
            filename="aggregated-smart-meter-data-lv-feeder-2024-08-part0071.csv",
            url="https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/27eb78c3-fa23-479a-aeb2-b24ab9305979/download/aggregated-smart-meter-data-lv-feeder-2024-08-part0071.csv",
        )
        assert first in results
        assert last in results

    def _mock_nged_files_response(self, mocked_responses, nged_files_response):
        expected_auth = {"Authorization": "nged_api_token"}
        mocked_responses.get(
            "https://example.com/nged",
            body=nged_files_response,
            status=200,
            content_type="application/json",
            match=[responses.matchers.header_matcher(expected_auth)],
        )

    def test_downloaded_files(self, config, already_downloaded):
        results = downloaded_files(config["downloaded_files"]["config"])
        assert results == {
            DNO.SSEN: {"2024-02-12.csv", "2024-02-13.csv", "2024-02-14.csv"},
            DNO.NPG: {"npg_feeder_november2023.zip"},
            DNO.NGED: {"aggregated-smart-meter-data-lv-feeder-2024-01-part0000.csv"},
        }

    def test_new_ssen_files(self):
        available_files = {
            AvailableFile(
                filename="2024-02-12.csv",
                url="https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-12.csv",
            ),
            AvailableFile(
                filename="2024-02-13.csv",
                url="https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-13.csv",
            ),
            AvailableFile(
                filename="2024-02-14.csv",
                url="https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-14.csv",
            ),
            AvailableFile(
                filename="2024-02-15.csv",
                url="https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-15.csv",
            ),
            AvailableFile(
                filename="2024-02-16.csv",
                url="https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-16.csv",
            ),
        }

        downloaded_files = {
            DNO.SSEN: {"2024-02-12.csv", "2024-02-13.csv", "2024-02-14.csv"},
        }

        result = new_ssen_files(downloaded_files, available_files)

        assert result == {
            AvailableFile(
                filename="2024-02-15.csv",
                url="https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-15.csv",
            ),
            AvailableFile(
                filename="2024-02-16.csv",
                url="https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-16.csv",
            ),
        }

    def test_new_ukpn_files(self):
        available_files = {
            AvailableFile(filename="ukpn.csv", url="https://example.com/ukpn.csv")
        }
        downloaded_files = {}
        result = new_ukpn_files(downloaded_files, available_files)
        assert result == available_files

    def test_new_npg_files(self):
        available_files = {
            AvailableFile(
                filename="npg_feeder_november2023.zip",
                url="https://northernpowergrid.opendatasoft.com/api/explore/v2.1/catalog/datasets/aggregated-smart-metering-data/attachments/npg_feeder_november2023_zip",
            ),
            AvailableFile(
                filename="npg_feeder_december2023.zip",
                url="https://northernpowergrid.opendatasoft.com/api/explore/v2.1/catalog/datasets/aggregated-smart-metering-data/attachments/npg_feeder_december2023_zip",
            ),
        }
        downloaded_files = {DNO.NPG: {"npg_feeder_december2023.zip"}}
        result = new_npg_files(downloaded_files, available_files)
        assert result == {
            AvailableFile(
                filename="npg_feeder_november2023.zip",
                url="https://northernpowergrid.opendatasoft.com/api/explore/v2.1/catalog/datasets/aggregated-smart-metering-data/attachments/npg_feeder_november2023_zip",
            )
        }

    def test_new_nged_files(self):
        available_files = {
            AvailableFile(
                filename="aggregated-smart-meter-data-lv-feeder-2024-01-part0000.csv",
                url="https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/105a7821-7f5c-4591-90e8-5915f253b1ff/download/aggregated-smart-meter-data-lv-feeder-2024-01-part0000.csv",
            ),
            AvailableFile(
                filename="aggregated-smart-meter-data-lv-feeder-2024-01-part0001.csv",
                url="https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/a9cea137-9149-44df-a90e-f8a89ab8dcfa/download/aggregated-smart-meter-data-lv-feeder-2024-01-part0001.csv",
            ),
            AvailableFile(
                filename="aggregated-smart-meter-data-lv-feeder-2024-01-part0002.csv",
                url="https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/23541896-d38e-45ad-9271-a1e8dbad8a35/download/aggregated-smart-meter-data-lv-feeder-2024-01-part0002.csv",
            ),
            AvailableFile(
                filename="aggregated-smart-meter-data-lv-feeder-2024-01-part0003.csv",
                url="https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/a0598187-c6a7-4314-9072-ca87d3c529e1/download/aggregated-smart-meter-data-lv-feeder-2024-01-part0003.csv",
            ),
        }

        downloaded_files = {
            DNO.NGED: {"aggregated-smart-meter-data-lv-feeder-2024-01-part0000.csv"}
        }

        result = new_nged_files(downloaded_files, available_files)

        assert result == {
            AvailableFile(
                filename="aggregated-smart-meter-data-lv-feeder-2024-01-part0001.csv",
                url="https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/a9cea137-9149-44df-a90e-f8a89ab8dcfa/download/aggregated-smart-meter-data-lv-feeder-2024-01-part0001.csv",
            ),
            AvailableFile(
                filename="aggregated-smart-meter-data-lv-feeder-2024-01-part0002.csv",
                url="https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/23541896-d38e-45ad-9271-a1e8dbad8a35/download/aggregated-smart-meter-data-lv-feeder-2024-01-part0002.csv",
            ),
            AvailableFile(
                filename="aggregated-smart-meter-data-lv-feeder-2024-01-part0003.csv",
                url="https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/a0598187-c6a7-4314-9072-ca87d3c529e1/download/aggregated-smart-meter-data-lv-feeder-2024-01-part0003.csv",
            ),
        }

    def test_ssen_graph_clean_slate(
        self,
        config,
        mocked_responses,
        ssen_files_response,
    ):
        self._mock_ssen_files_response(mocked_responses, ssen_files_response)
        results = materialize(
            assets=[
                ssen_files,
                downloaded_files,
                new_ssen_files,
            ],
            run_config={
                "ops": {
                    "ssen_files": config["ssen_files"],
                    "downloaded_files": config["downloaded_files"],
                }
            },
        )

        result = results.output_for_node("new_ssen_files")

        assert len(result) == 228

    def test_ssen_graph_already_downloaded(
        self,
        config,
        mocked_responses,
        already_downloaded,
        ssen_files_response,
    ):
        self._mock_ssen_files_response(mocked_responses, ssen_files_response)
        results = materialize(
            assets=[
                ssen_files,
                downloaded_files,
                new_ssen_files,
            ],
            run_config={
                "ops": {
                    "ssen_files": config["ssen_files"],
                    "downloaded_files": config["downloaded_files"],
                }
            },
        )

        result = results.output_for_node("new_ssen_files")

        assert len(result) == 225
