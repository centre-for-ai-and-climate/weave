import calendar
from datetime import datetime

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from dagster import (
    AllPartitionMapping,
    AssetDep,
    AssetExecutionContext,
    AssetSelection,
    MaterializeResult,
    MonthlyPartitionsDefinition,
    asset,
    define_asset_job,
)

from ..automation_conditions import lv_feeder_monthly_parquet_needs_updating
from ..core import DNO
from ..resources.output_files import OutputFilesResource
from ..resources.ssen import SSENAPIClient


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
        parquet_writer = pq.ParquetWriter(out, ssen_api_client.lv_feeder_pyarrow_schema)
        metadata["dagster/uri"] = staging_files_resource.path(
            DNO.SSEN.value, monthly_file
        )
        metadata["dagster/row_count"] = 0
        metadata["weave/nunique_feeders"] = 0
        for csv in daily_files:
            try:
                table = _read_csv_into_pyarrow_table(
                    context, raw_files_resource, ssen_api_client, csv
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
                metadata["weave/nunique_feeders"] += pc.count_distinct(
                    table.column("dataset_id")
                ).as_py()
                parquet_writer.write_table(table)
            except FileNotFoundError:
                context.log.info(
                    f"Ignoring missing SSEN daily file {csv} when building monthly file {monthly_file}"
                )
        parquet_writer.close()

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
    context, raw_files_resource, ssen_api_client, filename
):
    context.log.info(f"Processing {filename}")
    with raw_files_resource.open(DNO.SSEN.value, filename, mode="rb") as gzipf:
        return ssen_api_client.lv_feeder_file_pyarrow_table(gzipf)


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
