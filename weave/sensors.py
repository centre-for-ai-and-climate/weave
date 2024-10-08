from dagster import (
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    sensor,
)

from .assets.dno_lv_feeder_files import (
    ssen_lv_feeder_files_job,
    ssen_lv_feeder_files_partitions_def,
)
from .resources.ssen import SSENAPIClient


@sensor(
    job=ssen_lv_feeder_files_job,
    minimum_interval_seconds=60 * 60 * 24,
)
def ssen_lv_feeder_files_sensor(
    context: SensorEvaluationContext,
    ssen_api_client: SSENAPIClient,
) -> SensorResult | SkipReason:
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
