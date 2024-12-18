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
from weave.resources.nged import StubNGEDAPICLient
from weave.resources.output_files import OutputFilesResource
from weave.resources.ssen import StubSSENAPICLient
from weave.sensors import (
    nged_lv_feeder_files_sensor,
    ssen_lv_feeder_files_sensor,
    ssen_lv_feeder_monthly_parquet_sensor,
    ssen_lv_feeder_postcode_mapping_sensor,
)

FIXTURE_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    "fixtures",
)


@pytest.fixture
def instance():
    return DagsterInstance.ephemeral()


class TestSSENLVFeederFilesSensor:
    @pytest.fixture
    def api_client(self):
        return StubSSENAPICLient(
            available_files_url=os.path.join(
                FIXTURE_DIR, "ssen", "available_files.json"
            ),
        )

    def test_clean_slate(self, instance, api_client):
        context = build_sensor_context(instance=instance)
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

    def test_with_cursor(self, instance, api_client):
        context = build_sensor_context(instance=instance, cursor="2024-08-31.csv")
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

    def test_no_results(self, instance, api_client):
        context = build_sensor_context(instance=instance, cursor="2024-09-27.csv")
        result = ssen_lv_feeder_files_sensor(context, ssen_api_client=api_client)
        assert isinstance(result, SkipReason)


class TestNGEDLVFeederFilesSensor:
    @pytest.fixture
    def api_client(self):
        return StubNGEDAPICLient(
            api_token="TEST",
            lv_feeder_datapackage_url=os.path.join(
                FIXTURE_DIR, "nged", "datapackage.json"
            ),
        )

    def test_clean_slate(self, instance, api_client):
        context = build_sensor_context(instance=instance)
        result = nged_lv_feeder_files_sensor(context, nged_api_client=api_client)
        assert len(result.run_requests) == 1553
        assert (
            result.run_requests[0].partition_key
            == "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/105a7821-7f5c-4591-90e8-5915f253b1ff/download/aggregated-smart-meter-data-lv-feeder-2024-01-part0000.csv"
        )

    def test_with_cursor(self, instance, api_client):
        context = build_sensor_context(instance=instance, cursor="2024-11-30T19:53:57Z")
        result = nged_lv_feeder_files_sensor(context, nged_api_client=api_client)
        assert len(result.run_requests) == 1
        assert (
            result.run_requests[0].partition_key
            == "https://connecteddata.nationalgrid.co.uk/dataset/a920c581-9c6f-4788-becc-9d2caf20050c/resource/a34789d4-258e-4fa4-9232-0988d0980ad1/download/aggregated-smart-meter-data-lv-feeder-2024-10-part0227.csv"
        )

    def test_no_results(self, instance, api_client):
        context = build_sensor_context(instance=instance, cursor="2024-12-18T00:00:00Z")
        result = nged_lv_feeder_files_sensor(context, nged_api_client=api_client)
        assert isinstance(result, SkipReason)


class TestSSENLVFeederMonthlyParquetSensor:
    @pytest.fixture
    def api_client(self):
        return StubSSENAPICLient(
            file_to_download=os.path.join(
                FIXTURE_DIR, "ssen", "lv_feeder_files", "2024-02-12_head.csv"
            ),
        )

    @pytest.fixture
    def raw_files_resource(self, tmp_path):
        output_dir = tmp_path / "raw" / "ssen"
        output_dir.mkdir(parents=True)
        return OutputFilesResource(url=tmp_path.as_uri())

    def test_clean_slate(self, instance):
        context = build_sensor_context(instance=instance)
        result = ssen_lv_feeder_monthly_parquet_sensor(context)
        assert isinstance(result, SkipReason)

    @pytest.fixture
    def materialize_raw_file(self, instance, api_client, raw_files_resource):
        def _materialize_raw_file(raw_file):
            materialize_to_memory(
                [ssen_lv_feeder_files],
                instance=instance,
                partition_key=raw_file,
                resources={
                    "ssen_api_client": api_client,
                    "raw_files_resource": raw_files_resource,
                },
            )

        return _materialize_raw_file

    def test_with_cursor(self, instance, materialize_raw_file):
        context = build_sensor_context(instance=instance)
        instance.add_dynamic_partitions(
            ssen_lv_feeder_files_partitions_def.name,
            [
                "https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-12.csv",
                "https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-13.csv",
                "https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-03-01.csv",
            ],
        )
        materialize_raw_file(
            "https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-12.csv"
        )
        materialize_raw_file(
            "https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-13.csv"
        )
        materialize_raw_file(
            "https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-03-01.csv"
        )
        result = ssen_lv_feeder_monthly_parquet_sensor(context)
        assert len(result.run_requests) == 2
        # Multiple new partitions in a month should be deduped
        assert result.run_requests[0].partition_key == "2024-02-01"
        assert result.run_requests[1].partition_key == "2024-03-01"

        # When we re-materialise a daily partition, we should get a run request to
        # rebuild the relevant monthly parquet file
        materialize_raw_file(
            "https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-13.csv"
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
        materialize_raw_file(
            "https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2023-12-10.csv"
        )

        new_context = build_sensor_context(instance=instance, cursor=result.cursor)
        result = ssen_lv_feeder_monthly_parquet_sensor(new_context)
        assert len(result.run_requests) == 1
        assert result.run_requests[0].partition_key == "2023-12-01"


class TestSSENLVFeederPostcodeMappingSensor:
    @pytest.fixture
    def api_client(self):
        return StubSSENAPICLient(
            last_modified="2024-02-01T00:00:00Z",
        )

    def test_clean_slate(self, instance, api_client):
        context = build_sensor_context(instance=instance)
        result = ssen_lv_feeder_postcode_mapping_sensor(
            context, ssen_api_client=api_client
        )
        assert len(result.run_requests) == 1
        assert result.run_requests[0].job_name == "ssen_lv_feeder_postcode_mapping_job"

    def test_with_cursor(self, instance, api_client):
        context = build_sensor_context(instance=instance, cursor="2024-01-01T00:00:00Z")
        result = ssen_lv_feeder_postcode_mapping_sensor(
            context, ssen_api_client=api_client
        )
        assert len(result.run_requests) == 1
        assert result.run_requests[0].job_name == "ssen_lv_feeder_postcode_mapping_job"

        new_context = build_sensor_context(
            instance=instance, cursor="2024-03-01T00:00:00Z"
        )
        result = ssen_lv_feeder_postcode_mapping_sensor(
            new_context, ssen_api_client=api_client
        )
        assert isinstance(result, SkipReason)
