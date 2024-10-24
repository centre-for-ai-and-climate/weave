import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, asset
from dagster_pandas.data_frame import create_table_schema_metadata_from_dataframe

from ..core import DNO
from ..resources.output_files import OutputFilesResource
from ..resources.ssen import SSENAPIClient


@asset(description="SSEN's LV Feeder -> Postcode mapping file")
def ssen_lv_feeder_postcode_mapping(
    context: AssetExecutionContext,
    raw_files_resource: OutputFilesResource,
    ssen_api_client: SSENAPIClient,
) -> MaterializeResult:
    filename = "lv_feeder_postcode_mapping.csv.gz"
    url = ssen_api_client.postcode_mapping_url
    with raw_files_resource.open(DNO.SSEN.value, filename, mode="wb") as f:
        ssen_api_client.download_file(context, url, f)

    metadata = {}
    with raw_files_resource.open(DNO.SSEN.value, filename, mode="rb") as f:
        df = pd.read_csv(f, compression="gzip", engine="pyarrow")
        metadata["dagster/uri"] = f.name
        metadata["dagster/row_count"] = len(df)
        metadata["dagster/column_schema"] = create_table_schema_metadata_from_dataframe(
            df
        )
        metadata["weave/source"] = url
        metadata["weave/dataset_id"] = str(df["dataset_id"].iloc[0])
        metadata["weave/nunique_feeders"] = df["lv_feeder_name"].nunique()
        metadata["weave/nunique_substations"] = df[
            "secondary_substation_name"
        ].nunique()
        metadata["weave/nunique_postcodes"] = df["postcode"].nunique()

    return MaterializeResult(metadata=metadata)
