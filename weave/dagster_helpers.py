from dagster import AssetKey, AssetRecordsFilter, DagsterInstance, LoggerDefinition


def get_materialisations(
    instance: DagsterInstance,
    log: LoggerDefinition,
    asset_key: AssetKey,
    after_storage_id: int = None,
):
    has_more = True
    materialisations = []
    cursor = None
    while has_more:
        result = instance.fetch_materializations(
            AssetRecordsFilter(
                asset_key=asset_key,
                after_storage_id=after_storage_id,
            ),
            # Dagster cloud has a limit of 1000
            limit=1000,
            cursor=cursor,
            ascending=True,
        )
        materialisations.extend(result.records)
        cursor = result.cursor
        has_more = result.has_more

    log.info(
        f"Found {len(materialisations)} matching materialisation events for {asset_key}"
    )

    return materialisations
