import warnings

from dagster import (
    DagsterEventType,
    EventRecordsFilter,
    ExperimentalWarning,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    sensor,
)

from .assets.dno_lv_feeder_files import (
    ssen_lv_feeder_files,
    ssen_lv_feeder_files_job,
    ssen_lv_feeder_files_partitions_def,
)
from .assets.dno_lv_feeder_monthly_parquet import ssen_lv_feeder_monthly_parquet_job
from .resources.ssen import SSENAPIClient

warnings.filterwarnings("ignore", category=ExperimentalWarning)


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


@sensor(job=ssen_lv_feeder_monthly_parquet_job, minimum_interval_seconds=60 * 5)
def ssen_lv_feeder_monthly_parquet_sensor(context: SensorEvaluationContext):
    """Sensor for monthly partitioned parquet files from SSEN's raw data.

    This should be possible with Dagster natively really, but there's no way to map the
    relationship from dynamically partitioned assets to monthly partitioned ones. You
    can't supply your own PartitionMapping class."""

    # Turn the string cursor into one we can query with. Figured out from reading the
    # source code: https://docs.dagster.io/_modules/dagster/_core/event_api
    cursor = None
    if context.cursor:
        cursor = int(context.cursor)
        context.log.info(f"Parsed cursor: {cursor}")
    # Find new materialisations of the raw files
    new_materialisations = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.ASSET_MATERIALIZATION,
            asset_key=ssen_lv_feeder_files.key,
            after_cursor=cursor,
        ),
        limit=None,
        ascending=True,
    )

    context.log.info(
        f"Found {len(new_materialisations)} new materialisation events for raw SSEN lv feeder files since cursor"
    )
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
        context.log.info(f"New cursor is {cursor}")
        return SensorResult(
            run_requests=[RunRequest(partition_key=p) for p in partitions],
            cursor=str(latest_materialisation.storage_id),
        )
    else:
        return SkipReason(skip_message="No new files found")
