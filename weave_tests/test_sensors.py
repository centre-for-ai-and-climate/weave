import os

import pytest
from dagster import DagsterInstance, SkipReason, build_sensor_context

from weave.resources.ssen import TestSSENAPIClient
from weave.sensors import ssen_lv_feeder_files_sensor

FIXTURE_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    "fixtures",
)


@pytest.fixture
def instance():
    return DagsterInstance.ephemeral()


@pytest.fixture
def context(instance):
    return build_sensor_context(instance=instance)


@pytest.fixture
def api_client():
    return TestSSENAPIClient(
        available_files_url=os.path.join(FIXTURE_DIR, "ssen_files.json")
    )


class TestSSENLVFeederFilesSensor:
    def test_clean_slate(self, context, api_client):
        result = ssen_lv_feeder_files_sensor(context, ssen_api_client=api_client)
        assert len(result.run_requests) == 228
        assert len(result.dynamic_partitions_requests[0].partition_keys) == 228
        assert (
            result.run_requests[0].partition_key
            == "https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-12.csv"
        )
        assert (
            result.run_requests[-1].partition_key
            == "https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-09-27.csv"
        )

    def test_with_cursor(self, context, api_client):
        instance = DagsterInstance.ephemeral()
        context = build_sensor_context(instance=instance, cursor="2024-08-31.csv")
        api_client = TestSSENAPIClient(
            available_files_url=os.path.join(FIXTURE_DIR, "ssen_files.json")
        )
        result = ssen_lv_feeder_files_sensor(context, ssen_api_client=api_client)
        assert len(result.run_requests) == 27
        assert len(result.dynamic_partitions_requests[0].partition_keys) == 27
        assert (
            result.run_requests[0].partition_key
            == "https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-09-01.csv"
        )
        assert (
            result.run_requests[-1].partition_key
            == "https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-09-27.csv"
        )

    def test_no_results(self, context, api_client):
        instance = DagsterInstance.ephemeral()
        context = build_sensor_context(instance=instance, cursor="2024-09-27.csv")
        api_client = TestSSENAPIClient(
            available_files_url=os.path.join(FIXTURE_DIR, "ssen_files.json")
        )
        result = ssen_lv_feeder_files_sensor(context, ssen_api_client=api_client)
        assert isinstance(result, SkipReason)
