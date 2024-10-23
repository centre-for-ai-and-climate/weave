import os

import pytest
from dagster import (
    DagsterInstance,
    SkipReason,
    build_sensor_context,
    materialize_to_memory,
)

from weave.assets.dno_lv_feeder_files import (
    ssen_lv_feeder_files,
    ssen_lv_feeder_files_partitions_def,
)
from weave.resources.output_files import OutputFilesResource
from weave.resources.ssen import TestSSENAPIClient
from weave.sensors import (
    ssen_lv_feeder_files_sensor,
    ssen_lv_feeder_monthly_parquet_sensor,
)

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
        available_files_url=os.path.join(FIXTURE_DIR, "ssen_files.json"),
        file_to_download=os.path.join(FIXTURE_DIR, "ssen_2024-02-12_head.csv"),
    )


@pytest.fixture
def raw_files_resource(tmp_path):
    output_dir = tmp_path / "raw" / "ssen"
    output_dir.mkdir(parents=True)
    return OutputFilesResource(url=tmp_path.as_uri())


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


class TestSSENLVFeederMonthlyParquetSensor:
    def test_clean_slate(self, context, api_client):
        result = ssen_lv_feeder_monthly_parquet_sensor(context)
        assert isinstance(result, SkipReason)

    def _materialize_raw_file(self, raw_file, instance, api_client, raw_files_resource):
        materialize_to_memory(
            [ssen_lv_feeder_files],
            instance=instance,
            partition_key=raw_file,
            resources={
                "ssen_api_client": api_client,
                "raw_files_resource": raw_files_resource,
            },
        )

    def test_with_cursor(self, instance, context, api_client, raw_files_resource):
        instance.add_dynamic_partitions(
            ssen_lv_feeder_files_partitions_def.name,
            [
                "https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-12.csv",
                "https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-13.csv",
                "https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-03-01.csv",
            ],
        )
        self._materialize_raw_file(
            "https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-12.csv",
            instance,
            api_client,
            raw_files_resource,
        )
        self._materialize_raw_file(
            "https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-13.csv",
            instance,
            api_client,
            raw_files_resource,
        )
        self._materialize_raw_file(
            "https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-03-01.csv",
            instance,
            api_client,
            raw_files_resource,
        )
        result = ssen_lv_feeder_monthly_parquet_sensor(context)
        assert len(result.run_requests) == 2
        # Multiple new partitions in a month should be deduped
        assert result.run_requests[0].partition_key == "2024-02-01"
        assert result.run_requests[1].partition_key == "2024-03-01"

        # When we re-materialise a daily partition, we should get a run request to
        # rebuild the relevant monthly parquet file
        self._materialize_raw_file(
            "https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-13.csv",
            instance,
            api_client,
            raw_files_resource,
        )

        new_context = build_sensor_context(instance=instance, cursor=result.cursor)
        result = ssen_lv_feeder_monthly_parquet_sensor(new_context)
        assert len(result.run_requests) == 1
        assert result.run_requests[0].partition_key == "2024-02-01"

        # When we add a new file for a new month, we should get a run request to
        # rebuild the relevant monthly parquet file

        instance.add_dynamic_partitions(
            ssen_lv_feeder_files_partitions_def.name,
            [
                "https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2023-12-10.csv"
            ],
        )
        self._materialize_raw_file(
            "https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2023-12-10.csv",
            instance,
            api_client,
            raw_files_resource,
        )

        new_context = build_sensor_context(instance=instance, cursor=result.cursor)
        result = ssen_lv_feeder_monthly_parquet_sensor(new_context)
        assert len(result.run_requests) == 1
        assert result.run_requests[0].partition_key == "2023-12-01"
