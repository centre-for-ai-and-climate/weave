import pyarrow as pa
from dagster import (
    AssetExecutionContext,
    Backoff,
    DynamicPartitionsDefinition,
    MaterializeResult,
    RetryPolicy,
    asset,
    define_asset_job,
)
from dagster_pandas.data_frame import create_table_schema_metadata_from_dataframe

from ..core import DNO
from ..resources.output_files import OutputFilesResource
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
    raw_files_resource: OutputFilesResource,
    ssen_api_client: SSENAPIClient,
) -> MaterializeResult:
    url = context.partition_key
    filename = f"{SSENAPIClient.filename_for_url(url)}.gz"
    metadata = {}
    with raw_files_resource.open(DNO.SSEN.value, filename, mode="wb") as f:
        ssen_api_client.download_file(context, url, f)
    with raw_files_resource.open(DNO.SSEN.value, filename, mode="rb") as f:
        try:
            table = ssen_api_client.lv_feeder_file_pyarrow_table(f)
            df = table.to_pandas()
            metadata["dagster/uri"] = raw_files_resource.path(DNO.SSEN.value, filename)
            metadata["dagster/row_count"] = len(df)
            metadata["dagster/column_schema"] = (
                create_table_schema_metadata_from_dataframe(df)
            )
            metadata["weave/source"] = url
            metadata["weave/nunique_feeders"] = df["dataset_id"].nunique()
        except pa.lib.ArrowInvalid as e:
            context.log.error(f"Failed to read {url} into a PyArrow table: {e}")
            context.log.info(f"Attempting to delete {filename}")
            raw_files_resource.delete(DNO.SSEN.value, filename)
            raise

    return MaterializeResult(metadata=metadata)


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
