from dagster import (
    AssetExecutionContext,
    AutomationCondition,
    MaterializeResult,
    asset,
)
from dagster_pandas.data_frame import create_table_schema_metadata_from_dataframe

from ..resources.ons import ONSAPIClient
from ..resources.output_files import OutputFilesResource


@asset(
    description="ONS Postcode Directory",
    automation_condition=AutomationCondition.on_cron("@monthly"),
)
def onspd(
    context: AssetExecutionContext,
    ons_api_client: ONSAPIClient,
    raw_files_resource: OutputFilesResource,
) -> MaterializeResult:
    metadata = {}
    filename = "onspd.zip"
    with raw_files_resource.open("ons", filename, mode="wb") as f:
        ons_api_client.download_onspd(f, context)
        metadata["dagster/uri"] = raw_files_resource.path("ons", filename)
        metadata["weave/source"] = ons_api_client.onspd_url

    with raw_files_resource.open("ons", "onspd.zip", mode="rb") as f:
        df = ons_api_client.onspd_dataframe(f)
        metadata["dagster/row_count"] = len(df)
        metadata["dagster/column_schema"] = create_table_schema_metadata_from_dataframe(
            df
        )
    return MaterializeResult(metadata=metadata)
