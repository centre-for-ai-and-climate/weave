from datetime import datetime, timezone

from dagster import (
    AssetRecordsFilter,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    sensor,
)

from .assets.dno_lv_feeder_files import (
    nged_lv_feeder_files,
    nged_lv_feeder_files_job,
    nged_lv_feeder_files_partitions_def,
    ssen_lv_feeder_files,
    ssen_lv_feeder_files_job,
    ssen_lv_feeder_files_partitions_def,
)
from .assets.dno_lv_feeder_monthly_parquet import (
    nged_lv_feeder_monthly_parquet_job,
    ssen_lv_feeder_monthly_parquet_job,
)
from .assets.ssen_substation_locations import ssen_lv_feeder_postcode_mapping_job
from .resources.nged import NGEDAPIClient
from .resources.ssen import SSENAPIClient


@sensor(
    job=ssen_lv_feeder_files_job,
    minimum_interval_seconds=60 * 60 * 24,
)
def ssen_lv_feeder_files_sensor(
    context: SensorEvaluationContext,
    ssen_api_client: SSENAPIClient,
) -> SensorResult | SkipReason:
    """Sensor for new files from SSEN's raw data API.

    Hits SSEN's file listing API and triggers run requests for any new daily files it
    finds. The files are named in a sortable way - 2024-01-01 etc so that becomes our
    cursor.
    """
    available = ssen_api_client.get_available_files()
    latest_filename = available[-1].filename
    cursor = context.cursor or ""  # Cursors are optional
    new = [str(af.url) for af in available if af.filename > cursor]
    if len(new) > 0:
        return SensorResult(
            run_requests=[RunRequest(partition_key=url) for url in new],
            dynamic_partitions_requests=[
                ssen_lv_feeder_files_partitions_def.build_add_request(new)
            ],
            cursor=latest_filename,
        )
    else:
        return SkipReason(skip_message="No new files found")


@sensor(
    job=nged_lv_feeder_files_job,
    minimum_interval_seconds=60 * 60 * 24,
)
def nged_lv_feeder_files_sensor(
    context: SensorEvaluationContext,
    nged_api_client: NGEDAPIClient,
) -> SensorResult | SkipReason:
    """Sensor for new files from NGED's raw data API.

    NGED seem to release a large batch of new files at a time, and they can be out of
    order (e.g. part0999 is released before part0998) so we can't just use the filenames
    as a cursor. Luckily, their API gives created times for each file, so we can use
    that instead.
    """
    available = nged_api_client.get_available_files()
    latest = max(af.created for af in available)
    cursor = datetime.min.replace(tzinfo=timezone.utc)
    if context.cursor is not None and context.cursor != "":
        cursor = datetime.fromisoformat(context.cursor)
    new = [str(af.url) for af in available if af.created > cursor]
    if len(new) > 0:
        return SensorResult(
            run_requests=[RunRequest(partition_key=url) for url in new],
            dynamic_partitions_requests=[
                nged_lv_feeder_files_partitions_def.build_add_request(new)
            ],
            cursor=latest.isoformat(),
        )
    else:
        return SkipReason(skip_message="No new files found")


@sensor(job=ssen_lv_feeder_monthly_parquet_job, minimum_interval_seconds=60 * 5)
def ssen_lv_feeder_monthly_parquet_sensor(context: SensorEvaluationContext):
    """Sensor for monthly partitioned parquet files from SSEN's raw data.

    This should be possible with Dagster natively really, but there's no way to map the
    relationship from dynamically partitioned assets to monthly partitioned ones. You
    can't supply your own PartitionMapping class."""
    new_materialisations = _get_new_materialisations(context, ssen_lv_feeder_files.key)
    # Map them to the monthly partitions we need to create or update
    partitions = sorted(
        {
            SSENAPIClient.month_partition_from_url(m.partition_key)
            for m in new_materialisations
        }
    )

    context.log.info(f"Mapped events to {partitions} monthly partitions")

    if len(partitions) > 0:
        latest_materialisation = new_materialisations[-1]
        return SensorResult(
            run_requests=[RunRequest(partition_key=p) for p in partitions],
            cursor=str(latest_materialisation.storage_id),
        )
    else:
        return SkipReason(skip_message="No new files found")


@sensor(job=nged_lv_feeder_monthly_parquet_job, minimum_interval_seconds=60 * 5)
def nged_lv_feeder_monthly_parquet_sensor(context: SensorEvaluationContext):
    """Sensor for monthly partitioned parquet files from NGED's raw data.

    This should be possible with Dagster natively really, but there's no way to map the
    relationship from dynamically partitioned assets to monthly partitioned ones. You
    can't supply your own PartitionMapping class."""

    new_materialisations = _get_new_materialisations(context, nged_lv_feeder_files.key)
    # Map them to the monthly partitions we need to create or update
    partitions = sorted(
        {
            NGEDAPIClient.month_partition_from_url(m.partition_key)
            for m in new_materialisations
        }
    )

    context.log.info(f"Mapped events to {partitions} monthly partitions")

    if len(partitions) > 0:
        latest_materialisation = new_materialisations[-1]
        return SensorResult(
            run_requests=[RunRequest(partition_key=p) for p in partitions],
            cursor=str(latest_materialisation.storage_id),
        )
    else:
        return SkipReason(skip_message="No new files found")


def _get_new_materialisations(context, asset_key):
    # Turn the string cursor into one we can query with. Figured out from reading the
    # source code: https://docs.dagster.io/_modules/dagster/_core/event_api
    cursor = None
    if context.cursor:
        cursor = int(context.cursor)
        context.log.info(f"Parsed cursor: {cursor}")

    # Find new materialisations of the raw files
    new_materialisations, _cursor, has_more = context.instance.fetch_materializations(
        AssetRecordsFilter(
            asset_key=asset_key,
            after_storage_id=cursor,
        ),
        # This can't be None, but we want all of them and it's never going to be many
        # so I can't be bothered trying to iterate over pages
        limit=9999,
        ascending=True,
    )

    assert has_more is False, "We should never have more than 9999 new materialisations"

    context.log.info(
        f"Found {len(new_materialisations)} new materialisation events for {asset_key} since cursor {cursor}"
    )

    return new_materialisations


@sensor(
    job=ssen_lv_feeder_postcode_mapping_job,
    minimum_interval_seconds=60 * 60 * 24,
)
def ssen_lv_feeder_postcode_mapping_sensor(
    context: SensorEvaluationContext, ssen_api_client: SSENAPIClient
) -> SensorResult | SkipReason:
    last_modified = ssen_api_client.get_last_modified("ssen_lv_feeder_postcode_mapping")
    previous_last_modified = None
    if context.cursor is not None and context.cursor != "":
        previous_last_modified = datetime.fromisoformat(context.cursor)

    if previous_last_modified is None or last_modified > previous_last_modified:
        return SensorResult(
            run_requests=[RunRequest(job_name="ssen_lv_feeder_postcode_mapping_job")],
            cursor=last_modified.isoformat(),
        )
    else:
        return SkipReason(skip_message="Feeder postcode mapping file has not changed")
