import calendar
from datetime import datetime

import pyarrow as pa
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

# Matches the data "as-is" with some minor optimisations, e.g. the
# dictionary/categorical columns
pyarrow_schema = pa.schema(
    [
        ("dataset_id", pa.string()),
        ("dno_alias", pa.dictionary(pa.int32(), pa.string())),
        ("secondary_substation_id", pa.string()),
        ("secondary_substation_name", pa.dictionary(pa.int32(), pa.string())),
        ("lv_feeder_id", pa.string()),
        ("lv_feeder_name", pa.dictionary(pa.int32(), pa.string())),
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
        "aggregated_device_count_active",
        "total_consumption_active_import",
        "data_collection_log_timestamp",
        "insert_time",
        "last_modified_time",
    ],
)


@asset(
    description="Monthly partitioned parquet files from SSEN's raw data",
    partitions_def=MonthlyPartitionsDefinition(start_date="2024-02-01", end_offset=1),
    deps=["ssen_lv_feeder_files"],
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
    context.log.info(f"Producing {monthly_file} from {len(daily_files)} daily files")
    with staging_files_resource.open(DNO.SSEN.value, monthly_file, mode="wb") as out:
        parquet_writer = pq.ParquetWriter(out, pyarrow_schema)
        for csv in daily_files:
            try:
                append_csv_to_parquet(context, raw_files_resource, parquet_writer, csv)
            except FileNotFoundError:
                context.log.info(
                    f"Ignoring missing SSEN daily file {csv} when building monthly file {monthly_file}"
                )
        parquet_writer.close()


def append_csv_to_parquet(context, raw_files_resource, parquet_writer, filename):
    context.log.info(f"Processing {filename}")
    with raw_files_resource.open(DNO.SSEN.value, filename, mode="rb") as gzipf:
        with gzip_ng_threaded.open(gzipf, "rb", threads=pa.io_thread_count()) as f:
            table = pa_csv.read_csv(f, convert_options=pyarrow_csv_convert_options)
            parquet_writer.write_table(table)


def _ssen_files_for_month(year: int, month: int):
    """SSEN produces one file per day, so we need to get all the files for a month."""
    last_day_of_month = calendar.monthrange(year, month)[1] + 1
    files = []
    for day in range(1, last_day_of_month):
        files.append(f"{year}-{month:02d}-{day:02d}.csv.gz")
    return files


ssen_lv_feeder_monthly_parquet_job = define_asset_job(
    "ssen_lv_feeder_monthly_parquet_job",
    [ssen_lv_feeder_monthly_parquet],
)
