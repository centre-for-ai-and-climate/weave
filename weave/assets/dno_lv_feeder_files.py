from dagster import (
    AssetExecutionContext,
    Backoff,
    DynamicPartitionsDefinition,
    RetryPolicy,
    asset,
    define_asset_job,
)

from ..core import DNO
from ..resources.raw_files import RawFilesResource
from ..resources.ssen import SSENAPIClient

ssen_lv_feeder_files_partitions_def = DynamicPartitionsDefinition(
    name="ssen_lv_feeder_files_partitions_def"
)


@asset(
    description="LV Feeder files from SSEN",
    partitions_def=ssen_lv_feeder_files_partitions_def,
    retry_policy=RetryPolicy(max_retries=3, delay=10, backoff=Backoff.EXPONENTIAL),
)
def ssen_lv_feeder_files(
    context: AssetExecutionContext,
    raw_files_resource: RawFilesResource,
    ssen_api_client: SSENAPIClient,
) -> None:
    url = context.partition_key
    filename = f"{ssen_api_client.filename_for_url(url)}.gz"
    with raw_files_resource.open(DNO.SSEN.value, filename, mode="wb") as f:
        ssen_api_client.download_file(context, url, f)


ssen_lv_feeder_files_job = define_asset_job(
    "ssen_lv_feeder_files_job",
    [ssen_lv_feeder_files],
    config={
        "execution": {
            "config": {
                "multiprocess": {"max_concurrent": 3},
            }
        }
    },
)
