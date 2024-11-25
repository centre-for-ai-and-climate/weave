from dagster import (
    AllPartitionMapping,
    AssetDep,
    AssetExecutionContext,
    AutomationCondition,
    MaterializeResult,
    asset,
)

from ..resources.output_files import OutputFilesResource


@asset(
    description="""Combined LV Feeder GeoParquet from all DNOs.

    An ever-growing monthly-partitioned geoparquet file containing all the low-voltage
    feeder data we have.""",
    deps=[
        AssetDep(
            "ssen_lv_feeder_monthly_parquet", partition_mapping=AllPartitionMapping()
        )
    ],
    automation_condition=AutomationCondition.on_missing(),
)
def lv_feeder_combined_geoparquet(
    context: AssetExecutionContext,
    staging_files_resource: OutputFilesResource,
    output_files_resource: OutputFilesResource,
) -> MaterializeResult:
    metadata = {}
    return MaterializeResult(metadata=metadata)
