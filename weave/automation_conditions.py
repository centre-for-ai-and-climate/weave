from dagster import AssetSelection, AutomationCondition


def needs_updating() -> AutomationCondition:
    """
    A version of Dagster's AutomationCondition.eager() but with two important
    changes:
    1. Removing the top level in_latest_time_window() condition so every partition is
       updated, not just the latest one.
    2. Removing the newly_missing() condition so that we counteract an unwanted
       side-effect of 1. where removing that time-window constraint could trigger
       massive backfills.
    """
    return (
        AutomationCondition.any_deps_updated().since_last_handled()
        & ~AutomationCondition.any_deps_missing()
        & ~AutomationCondition.any_deps_in_progress()
        & ~AutomationCondition.in_progress()
    ).with_label("needs_updating")


def lv_feeder_monthly_parquet_needs_updating(
    raw_files_asset_selection: AssetSelection,
) -> AutomationCondition:
    """
    A version of needs_updating() specific to monthly lv feeder parquet files, which
    is not triggered by raw file updates and can tolerate missing upstream partitions
    in those raw files, whilst still giving us the nice properties
    of waiting for in progress updates in those raw file assets .
    """
    return (
        AutomationCondition.any_deps_updated()
        .ignore(raw_files_asset_selection)
        .since_last_handled()
        & ~AutomationCondition.any_deps_missing().ignore(raw_files_asset_selection)
        & ~AutomationCondition.any_deps_in_progress()
        & ~AutomationCondition.in_progress()
    ).with_label("lv_feeder_monthly_parquet_needs_updating")
