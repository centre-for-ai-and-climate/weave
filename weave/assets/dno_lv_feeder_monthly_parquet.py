import calendar
from datetime import datetime

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from dagster import (
    AllPartitionMapping,
    AssetDep,
    AssetExecutionContext,
    AssetRecordsFilter,
    AssetSelection,
    DagsterInstance,
    MaterializeResult,
    MonthlyPartitionsDefinition,
    asset,
    define_asset_job,
)

from ..automation_conditions import lv_feeder_monthly_parquet_needs_updating
from ..core import DNO, lv_feeder_parquet_schema
from ..resources.nged import NGEDAPIClient
from ..resources.output_files import OutputFilesResource
from ..resources.ssen import SSENAPIClient
from .dno_lv_feeder_files import nged_lv_feeder_files


@asset(
    description="""
    Monthly partitioned parquet files from SSEN's raw low-voltage feeder data, with
    substation locations added""",
    partitions_def=MonthlyPartitionsDefinition(start_date="2024-02-01", end_offset=1),
    deps=[
        # Each partition is not really dependent on all of the partitions of the raw
        # files, but we don't have a way to express that in Dagster yet. This stops it
        # breaking when it tries to check there are valid partitions.
        AssetDep("ssen_lv_feeder_files", partition_mapping=AllPartitionMapping()),
        "ssen_substation_location_lookup_feeder_postcodes",
        "ssen_substation_location_lookup_transformer_load_model",
    ],
    # See also sensors.ssen_lv_feeder_monthly_parquet_sensor
    automation_condition=lv_feeder_monthly_parquet_needs_updating(
        AssetSelection.assets("ssen_lv_feeder_files")
    ),
)
def ssen_lv_feeder_monthly_parquet(
    context: AssetExecutionContext,
    raw_files_resource: OutputFilesResource,
    staging_files_resource: OutputFilesResource,
    ssen_api_client: SSENAPIClient,
) -> MaterializeResult:
    metadata = {}
    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d")
    year = partition_date.year
    month = partition_date.month
    monthly_file = f"{year}-{month:02d}.parquet"
    daily_files = _ssen_files_for_month(year, month)
    location_lookup = _substation_location_lookup(staging_files_resource)
    context.log.info(f"Producing {monthly_file} from {len(daily_files)} daily files")
    with staging_files_resource.open(DNO.SSEN.value, monthly_file, mode="wb") as out:
        parquet_writer = pq.ParquetWriter(out, lv_feeder_parquet_schema)
        metadata["dagster/uri"] = staging_files_resource.path(
            DNO.SSEN.value, monthly_file
        )
        metadata["dagster/row_count"] = 0
        unique_feeders = set()
        for csv in daily_files:
            try:
                table = _read_csv_into_pyarrow_table(
                    context, DNO.SSEN, raw_files_resource, ssen_api_client, csv
                )
                table = table.append_column(
                    "substation_nrn",
                    [pc.utf8_slice_codeunits(table.column("dataset_id"), 0, 10)],
                )
                table = table.join(
                    location_lookup,
                    keys="substation_nrn",
                    join_type="left outer",
                    right_suffix="_lookup",
                )
                # Join adds a new column, we want to overwrite the old one
                table = table.set_column(
                    table.column_names.index("substation_geo_location"),
                    pa.field("substation_geo_location", pa.string()),
                    table.column("substation_geo_location_lookup"),
                )
                table = table.drop_columns(
                    ["substation_nrn", "substation_geo_location_lookup"]
                )

                metadata["dagster/row_count"] += table.num_rows
                unique_feeders.update(pc.unique(table.column("dataset_id")).to_pylist())
                parquet_writer.write_table(table)
            except FileNotFoundError:
                context.log.info(
                    f"Ignoring missing SSEN daily file {csv} when building monthly file {monthly_file}"
                )
        parquet_writer.close()
        metadata["weave/nunique_feeders"] = len(unique_feeders)

    if metadata["dagster/row_count"] == 0:
        context.log.info(f"No data found for {monthly_file}, deleting empty file")
        staging_files_resource.delete(DNO.SSEN.value, monthly_file)

    return MaterializeResult(metadata=metadata)


def _ssen_files_for_month(year: int, month: int):
    """SSEN produces one file per day, so we need to get all the files for a month."""
    last_day_of_month = calendar.monthrange(year, month)[1] + 1
    files = []
    for day in range(1, last_day_of_month):
        files.append(f"{year}-{month:02d}-{day:02d}.csv.gz")
    return files


def _read_csv_into_pyarrow_table(
    context: AssetExecutionContext,
    dno: DNO,
    raw_files_resource: OutputFilesResource,
    api_client: SSENAPIClient | NGEDAPIClient,
    filename: str,
):
    """Shared helper function for opening CSV files from DNOs as a pyarrow table."""
    context.log.info(f"Opening {filename}")
    with raw_files_resource.open(dno.value, filename, mode="rb") as gzipf:
        return api_client.lv_feeder_file_pyarrow_table(gzipf)


def _substation_location_lookup(
    staging_files_resource: OutputFilesResource,
) -> pa.Table:
    load_model_lookup = None
    postcode_lookup = None

    with staging_files_resource.open(
        DNO.SSEN.value,
        "substation_location_lookup_transformer_load_model.parquet",
        mode="rb",
    ) as f:
        load_model_lookup = pq.read_table(f)

    with staging_files_resource.open(
        DNO.SSEN.value, "substation_location_lookup_feeder_postcodes.parquet", mode="rb"
    ) as f:
        postcode_lookup = pq.read_table(f)

    lookup = postcode_lookup.join(
        load_model_lookup,
        keys="substation_nrn",
        join_type="full outer",
        left_suffix="_feeder_postcodes",
        right_suffix="_load_model",
    )

    substation_geo_location = pc.coalesce(
        lookup.column("substation_geo_location_load_model"),
        lookup.column("substation_geo_location_feeder_postcodes"),
    )

    return lookup.drop_columns(
        [
            "substation_geo_location_load_model",
            "substation_geo_location_feeder_postcodes",
        ]
    ).append_column("substation_geo_location", substation_geo_location)


ssen_lv_feeder_monthly_parquet_job = define_asset_job(
    "ssen_lv_feeder_monthly_parquet_job",
    [ssen_lv_feeder_monthly_parquet],
)


@asset(
    description="""Monthly partitioned parquet files from NGED's raw low-voltage feeder data""",
    partitions_def=MonthlyPartitionsDefinition(start_date="2024-01-01"),
    deps=[
        # Each partition is not really dependent on all of the partitions of the raw
        # files, but we don't have a way to express that in Dagster yet. This stops it
        # breaking when it tries to check there are valid partitions.
        AssetDep("nged_lv_feeder_files", partition_mapping=AllPartitionMapping())
    ],
)
def nged_lv_feeder_monthly_parquet(
    context: AssetExecutionContext,
    raw_files_resource: OutputFilesResource,
    staging_files_resource: OutputFilesResource,
    nged_api_client: NGEDAPIClient,
) -> MaterializeResult:
    metadata = {"dagster/row_count": 0, "weave/nunique_feeders": 0}
    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d")
    year = partition_date.year
    month = partition_date.month
    monthly_file = f"{year}-{month:02d}.parquet"
    part_files = _nged_files_for_month(
        context.instance, nged_api_client, context.partition_key
    )
    context.log.info(f"Producing {monthly_file} from {len(part_files)} part files")
    if len(part_files) == 0:
        context.log.info(f"No data found for {monthly_file}, not creating parquet file")
        return MaterializeResult(metadata=metadata)

    with staging_files_resource.open(DNO.NGED.value, monthly_file, mode="wb") as out:
        parquet_writer = pq.ParquetWriter(out, lv_feeder_parquet_schema)
        metadata["dagster/uri"] = staging_files_resource.path(
            DNO.NGED.value, monthly_file
        )
        unique_feeders = set()
        for csv in part_files:
            table = _read_csv_into_pyarrow_table(
                context, DNO.NGED, raw_files_resource, nged_api_client, csv
            )

            table = table.rename_columns(
                {
                    "dataset_id": "dataset_id",
                    "dno_alias": "dno_alias",
                    "secondary_substation_id": "secondary_substation_id",
                    "secondary_substation_name": "secondary_substation_name",
                    "LV_feeder_ID": "lv_feeder_id",
                    "LV_feeder_name": "lv_feeder_name",
                    "substation_geo_location": "substation_geo_location",
                    "aggregated_device_count_Active": "aggregated_device_count_active",
                    "Total_consumption_active_import": "total_consumption_active_import",
                    "data_collection_log_timestamp": "data_collection_log_timestamp",
                    "Insert_time": "insert_time",
                    "last_modified_time": "last_modified_time",
                }
            )

            metadata["dagster/row_count"] += table.num_rows

            unique_feeders.update(
                pc.unique(
                    pc.binary_join_element_wise(
                        table.column("secondary_substation_id"),
                        table.column("lv_feeder_id"),
                        "-",
                    )
                ).to_pylist()
            )
            parquet_writer.write_table(table)
        parquet_writer.close()
        metadata["weave/nunique_feeders"] = len(unique_feeders)

    return MaterializeResult(metadata=metadata)


def _nged_files_for_month(
    instance: DagsterInstance, client: NGEDAPIClient, partition_key: str
) -> set[str]:
    """NGED produces a random number of files per month, split into parts, so we need
    to query dagster to find out what we have."""
    files = set()
    has_more = True
    cursor = None
    while has_more:
        materialisations, cursor, has_more = instance.fetch_materializations(
            AssetRecordsFilter(
                asset_key=nged_lv_feeder_files.key, after_storage_id=cursor
            ),
            limit=9999,
            ascending=True,
        )
        for m in materialisations:
            if client.month_partition_from_url(m.partition_key) == partition_key:
                files.add(f"{client.filename_for_url(m.partition_key)}.gz")

    return files


nged_lv_feeder_monthly_parquet_job = define_asset_job(
    "nged_lv_feeder_monthly_parquet_job",
    [nged_lv_feeder_monthly_parquet],
)
