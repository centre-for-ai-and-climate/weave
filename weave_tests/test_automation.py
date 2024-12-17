import os
from datetime import datetime

import pytest
from dagster import (
    AssetKey,
    AssetMaterialization,
    DagsterInstance,
    evaluate_automation_conditions,
)

from weave.definitions import defs

FIXTURE_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    "fixtures",
)


@pytest.fixture
def instance():
    return DagsterInstance.ephemeral()


def assert_materialization_requested(result, asset: str):
    expected_asset = AssetKey(asset)
    assert result.total_requested == 1
    assert len(result.get_requested_partitions(expected_asset)) == 1


def assert_not_materialised(instance: DagsterInstance, asset: str):
    assert instance.get_materialized_partitions(AssetKey(asset)) == set()


def materialize(instance: DagsterInstance, asset: str, partition: str | None = None):
    instance.report_runless_asset_event(
        AssetMaterialization(asset_key=AssetKey(asset), partition=partition)
    )


def test_nothing_happens_automatically(instance: DagsterInstance):
    result = evaluate_automation_conditions(defs, instance)
    assert result.total_requested == 0, "No assets should be eagerly requested"


def test_ssen_transformer_load_model_lookup_automation(instance):
    # Given we have materialised nothing, and started the automation
    assert_not_materialised(
        instance, "ssen_substation_location_lookup_transformer_load_model"
    )
    result = evaluate_automation_conditions(defs, instance)

    # When we have the transformer load model
    materialize(instance, "ssen_transformer_load_model")
    result = evaluate_automation_conditions(defs, instance, cursor=result.cursor)
    # Then the transformer location lookup should be requested
    assert_materialization_requested(
        result, "ssen_substation_location_lookup_transformer_load_model"
    )


def test_ssen_postcode_lookup_automation(instance):
    # Given we have materialised nothing, and started the automation
    assert_not_materialised(
        instance, "ssen_substation_location_lookup_feeder_postcodes"
    )
    result = evaluate_automation_conditions(defs, instance)

    # When we have just ONSPD
    materialize(instance, "onspd")
    result = evaluate_automation_conditions(defs, instance, cursor=result.cursor)
    # Then, nothing should be requested
    assert (
        result.total_requested == 0
    ), "Postcode lookup shouldn't be requested when dependencies are missing"

    # When we have ONSPD and the postcode mapping file
    materialize(instance, "ssen_lv_feeder_postcode_mapping")
    result = evaluate_automation_conditions(defs, instance, cursor=result.cursor)
    # Then, we should request the postcode location lookup
    assert_materialization_requested(
        result, "ssen_substation_location_lookup_feeder_postcodes"
    )


def test_ssen_monthly_files_automation(instance):
    # Given we have materialised nothing, and started the automation
    assert_not_materialised(instance, "ssen_lv_feeder_monthly_parquet")
    result = evaluate_automation_conditions(defs, instance)

    # When we materialise a raw file
    materialize(
        instance,
        "ssen_lv_feeder_files",
        "https://ssen-smart-meter-prod.portaljs.com/LV_FEEDER_USAGE/2024-02-12.csv",
    )
    result = evaluate_automation_conditions(defs, instance, cursor=result.cursor)
    # Then, nothing should be requested
    assert (
        result.total_requested == 0
    ), "Monthly files shouldn't be requested by automation after raw files update, that's controlled by a sensor"

    # When we materialise just the transformer location lookup
    materialize(instance, "ssen_substation_location_lookup_transformer_load_model")
    result = evaluate_automation_conditions(defs, instance, cursor=result.cursor)
    # Then, nothing should be requested - because we're missing the postcode lookup
    assert (
        result.total_requested == 0
    ), "Monthly files shouldn't be requested if dependencies are missing"

    # When we materialise the postcode location lookup
    materialize(instance, "ssen_substation_location_lookup_feeder_postcodes")
    result = evaluate_automation_conditions(
        instance=instance, defs=defs, cursor=result.cursor
    )
    # Then, we should request updates of all the monthly files
    start_date = datetime(2024, 2, 1)
    today = datetime.today()
    months_difference = (today.year - start_date.year) * 12 + (
        today.month - start_date.month
    )
    expected_partitions = months_difference + 1
    # Combined geoparguet gets requested too, hence doubling the expected partitions
    assert result.total_requested == expected_partitions * 2
    assert (
        len(result.get_requested_partitions(AssetKey("ssen_lv_feeder_monthly_parquet")))
        == expected_partitions
    )


def test_combined_geoparquet_automation(instance):
    # Given we have materialised nothing, and started the automation
    assert_not_materialised(instance, "lv_feeder_combined_geoparquet")
    result = evaluate_automation_conditions(defs, instance)

    # When we materialise one of the monthly files and run the automation tick
    materialize(instance, "ssen_lv_feeder_monthly_parquet", "2024-02-01")
    result = evaluate_automation_conditions(defs, instance, cursor=result.cursor)

    # Then, we should materialise the same partition in the combined geoparquet
    assert_materialization_requested(result, "lv_feeder_combined_geoparquet")
    assert result.get_requested_partitions(
        AssetKey("lv_feeder_combined_geoparquet")
    ) == {"2024-02-01"}
