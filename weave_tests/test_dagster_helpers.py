from dagster import AssetKey, AssetMaterialization, DagsterInstance, build_asset_context

from weave.dagster_helpers import get_materialisations


def test_get_materialisations_pagination():
    instance = DagsterInstance.ephemeral()
    context = build_asset_context(instance=instance)
    log = context.log
    asset_key = AssetKey("test")
    for i in range(1001):
        instance.report_runless_asset_event(AssetMaterialization(asset_key=asset_key))
    materialisations = get_materialisations(instance, log, asset_key)
    assert len(materialisations) == 1001


def test_get_materialisations_storage_id_filtering():
    instance = DagsterInstance.ephemeral()
    context = build_asset_context(instance=instance)
    log = context.log
    asset_key = AssetKey("test")
    instance.report_runless_asset_event(AssetMaterialization(asset_key=asset_key))
    materialisations = get_materialisations(instance, log, asset_key)
    assert len(materialisations) == 1

    first_storage_id = materialisations[0].storage_id
    materialisations = get_materialisations(instance, log, asset_key, first_storage_id)
    assert len(materialisations) == 0

    instance.report_runless_asset_event(AssetMaterialization(asset_key=asset_key))
    materialisations = get_materialisations(instance, log, asset_key, first_storage_id)
    assert len(materialisations) == 1
    assert materialisations[0].storage_id != first_storage_id
