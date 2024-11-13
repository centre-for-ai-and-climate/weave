import calendar
from datetime import datetime

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
from dagster import (
    AssetExecutionContext,
    MonthlyPartitionsDefinition,
    asset,
    define_asset_job,
)
from zlib_ng import gzip_ng_threaded

from ..core import DNO
from ..resources.output_files import OutputFilesResource

# Matches the data "as-is". I wanted to add some space-saving optimizations
# like dictionaries for the name columns, but it doesn't work with joining for some
# reason.
pyarrow_schema = pa.schema(
    [
        ("dataset_id", pa.string()),
        ("dno_alias", pa.string()),
        ("secondary_substation_id", pa.string()),
        ("secondary_substation_name", pa.string()),
        ("lv_feeder_id", pa.string()),
        ("lv_feeder_name", pa.string()),
        ("substation_geo_location", pa.string()),
        ("aggregated_device_count_active", pa.float64()),
        ("total_consumption_active_import", pa.float64()),
        ("data_collection_log_timestamp", pa.timestamp("ms", tz="UTC")),
        ("insert_time", pa.timestamp("ms", tz="UTC")),
        ("last_modified_time", pa.timestamp("ms", tz="UTC")),
    ]
)
pyarrow_csv_convert_options = pa_csv.ConvertOptions(
    column_types=pyarrow_schema,
    include_columns=[
        "dataset_id",
        "dno_alias",
        "secondary_substation_id",
        "secondary_substation_name",
        "lv_feeder_id",
        "lv_feeder_name",
        "substation_geo_location",
        "aggregated_device_count_active",
        "total_consumption_active_import",
        "data_collection_log_timestamp",
        "insert_time",
        "last_modified_time",
    ],
)


@asset(
    description="""
    Monthly partitioned parquet files from SSEN's raw low-voltage feeder data, with
    substation locations added""",
    partitions_def=MonthlyPartitionsDefinition(start_date="2024-02-01", end_offset=1),
    deps=[
        "ssen_lv_feeder_files",
        "ssen_substation_location_lookup_feeder_postcodes",
        "ssen_substation_location_lookup_transformer_load_model",
    ],
)
def ssen_lv_feeder_monthly_parquet(
    context: AssetExecutionContext,
    raw_files_resource: OutputFilesResource,
    staging_files_resource: OutputFilesResource,
) -> None:
    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d")
    year = partition_date.year
    month = partition_date.month
    monthly_file = f"{year}-{month:02d}.parquet"
    daily_files = _ssen_files_for_month(year, month)
    location_lookup = _substation_location_lookup(staging_files_resource)
    context.log.info(f"Producing {monthly_file} from {len(daily_files)} daily files")
    with staging_files_resource.open(DNO.SSEN.value, monthly_file, mode="wb") as out:
        parquet_writer = pq.ParquetWriter(out, pyarrow_schema)
        for csv in daily_files:
            try:
                table = _read_csv_into_pyarrow_table(context, raw_files_resource, csv)
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
                parquet_writer.write_table(table)
            except FileNotFoundError:
                context.log.info(
                    f"Ignoring missing SSEN daily file {csv} when building monthly file {monthly_file}"
                )
        parquet_writer.close()


def _ssen_files_for_month(year: int, month: int):
    """SSEN produces one file per day, so we need to get all the files for a month."""
    last_day_of_month = calendar.monthrange(year, month)[1] + 1
    files = []
    for day in range(1, last_day_of_month):
        files.append(f"{year}-{month:02d}-{day:02d}.csv.gz")
    return files


def _read_csv_into_pyarrow_table(context, raw_files_resource, filename):
    context.log.info(f"Processing {filename}")
    with raw_files_resource.open(DNO.SSEN.value, filename, mode="rb") as gzipf:
        with gzip_ng_threaded.open(gzipf, "rb", threads=pa.io_thread_count()) as f:
            return pa_csv.read_csv(f, convert_options=pyarrow_csv_convert_options)


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
