import calendar
from datetime import datetime, timedelta, timezone
from typing import Generator

import geopandas as gpd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MonthlyPartitionsDefinition,
    asset,
)
from geopandas.io.arrow import _geopandas_to_arrow

from ..automation_conditions import needs_updating
from ..core import DNO, lv_feeder_geoparquet_schema
from ..resources.output_files import OutputFilesResource


@asset(
    description="""Combined LV Feeder GeoParquet from all DNOs.

    An ever-growing monthly-partitioned geoparquet file containing all the low-voltage
    feeder data we have.""",
    partitions_def=MonthlyPartitionsDefinition(start_date="2024-01-01", end_offset=1),
    deps=["ssen_lv_feeder_monthly_parquet", "nged_lv_feeder_monthly_parquet"],
    automation_condition=needs_updating(),
)
def lv_feeder_combined_geoparquet(
    context: AssetExecutionContext,
    staging_files_resource: OutputFilesResource,
    output_files_resource: OutputFilesResource,
) -> MaterializeResult:
    metadata = {}
    metadata["dagster/row_count"] = 0
    metadata["weave/nunique_feeders"] = 0
    metadata["weave/nunique_substations"] = 0

    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d")
    year = partition_date.year
    month = partition_date.month
    monthly_file = f"{year}-{month:02d}.parquet"
    days_in_month = calendar.monthrange(year, month)[1]

    unique_feeder_ids = set()
    unique_substation_ids = set()

    try:
        with output_files_resource.open("smart-meter", monthly_file, mode="wb") as out:
            parquet_writer = _create_parquet_writer(out)
            metadata["dagster/uri"] = output_files_resource.path(
                "smart-meter", monthly_file
            )
            for day in range(1, days_in_month + 1):
                context.log.info(f"Processing day: {day}")
                start_of_day = datetime(year, month, day, 0, 0, 0, tzinfo=timezone.utc)
                daily_filters = (
                    [
                        (
                            "data_collection_log_timestamp",
                            ">=",
                            pa.scalar(start_of_day),
                        ),
                        (
                            "data_collection_log_timestamp",
                            "<",
                            pa.scalar(start_of_day + timedelta(days=1)),
                        ),
                    ],
                )
                daily_table = None

                for dno in [DNO.NGED, DNO.SSEN]:
                    context.log.info(f"Processing DNO: {dno.value}")
                    with staging_files_resource.open(
                        dno.value, monthly_file, mode="rb"
                    ) as in_file:
                        daily_dno_table = _dno_to_combined_geoparquet(
                            dno, pq.read_table(in_file, filters=daily_filters)
                        )
                        if daily_table is None:
                            daily_table = daily_dno_table
                        else:
                            daily_table = pa.concat_tables(
                                [daily_table, daily_dno_table]
                            )

                daily_table = daily_table.sort_by(
                    [
                        ("data_collection_log_timestamp", "ascending"),
                        ("lv_feeder_unique_id", "ascending"),
                    ]
                )
                parquet_writer.write_table(daily_table)

                metadata["dagster/row_count"] += daily_table.num_rows
                unique_feeder_ids.update(
                    pc.unique(daily_table.column("lv_feeder_unique_id")).to_pylist()
                )
                unique_substation_ids.update(
                    pc.unique(
                        daily_table.column("secondary_substation_unique_id")
                    ).to_pylist()
                )

            parquet_writer.close()
    except FileNotFoundError:
        context.log.error("Failed to open monthly input file: {e}")
        context.log.info(
            f"Attempting to delete {output_files_resource.path("smart-meter", monthly_file)}"
        )
        output_files_resource.delete("smart-meter", monthly_file)

    metadata["weave/nunique_feeders"] = len(unique_feeder_ids)
    metadata["weave/nunique_substations"] = len(unique_substation_ids)

    return MaterializeResult(metadata=metadata)


def _create_parquet_writer(out) -> pq.ParquetWriter:
    # Technically, we only sort the data by timestamp and feeder id, but because of how
    # the feeder ids are created (concatenating alias + substation + feeder), we can
    # say that the data is sorted by all three columns and potentially speed up
    # filtering on any of them.
    sorting_columns = [
        pq.SortingColumn(
            lv_feeder_geoparquet_schema.names.index("data_collection_log_timestamp")
        ),
        pq.SortingColumn(lv_feeder_geoparquet_schema.names.index("dno_alias")),
        pq.SortingColumn(
            lv_feeder_geoparquet_schema.names.index("secondary_substation_unique_id")
        ),
        pq.SortingColumn(
            lv_feeder_geoparquet_schema.names.index("lv_feeder_unique_id")
        ),
    ]
    return pq.ParquetWriter(
        out,
        schema=lv_feeder_geoparquet_schema,
        compression="zstd",
        compression_level=22,
        coerce_timestamps="ms",
        allow_truncated_timestamps=True,
        sorting_columns=sorting_columns,
        store_decimal_as_integer=True,
    )


def _generate_parquet_batches(
    parquet_file: pq.ParquetFile,
) -> Generator[pa.RecordBatch, None, None]:
    return parquet_file.iter_batches(
        batch_size=1024 * 1024,
        columns=[
            "dataset_id",
            "dno_alias",
            "aggregated_device_count_active",
            "total_consumption_active_import",
            "data_collection_log_timestamp",
            "substation_geo_location",
            "secondary_substation_id",
            "lv_feeder_id",
        ],
    )


def _dno_to_combined_geoparquet(dno: DNO, batch: pa.RecordBatch) -> pa.Table:
    # Clear any existing metadata to avoid clashes
    batch = batch.replace_schema_metadata(None)
    table = _add_geoparquet_columns(batch)
    table = _cast_columns(table)
    table = _add_unique_id_columns(dno, table)

    return table.select(
        [
            "dataset_id",
            "dno_alias",
            "aggregated_device_count_active",
            "total_consumption_active_import",
            "data_collection_log_timestamp",
            "geometry",
            "secondary_substation_unique_id",
            "lv_feeder_unique_id",
        ]
    )


def _add_geoparquet_columns(batch: pa.RecordBatch) -> pa.Table:
    # I tried adopting the code from geopandas directly, so that we could stick to
    # pyarrow throughout, but it is dense and hard to adapt. I got stuck on the fact
    # that we have some null locations, which I think geopandas works around through
    # the use of masks, but it was a bit over my head.
    # This is one to come back to, I don't like using _geopandas_to_arrow.

    # This is also unreliable in terms of data type conversions, so we do it first and
    # then apply our specific casts after to make sure things stay as we want them
    df = batch.to_pandas(self_destruct=True)
    df[["lat", "lng"]] = df["substation_geo_location"].str.split(",", n=1, expand=True)
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df["lng"], df["lat"], crs="EPSG:4326")
    )
    del df
    # Public to_arrow method does not include the schema_version or bbox options, nor
    # does it do all the fancy metadata for parquet output.
    table = _geopandas_to_arrow(
        gdf,
        index=False,
        geometry_encoding="geoarrow",
        schema_version="1.1.0",
        write_covering_bbox=True,
    )
    del gdf

    return table


def _cast_columns(table: pa.Table) -> pa.Table:
    # Cast floats to int
    table = table.set_column(
        table.column_names.index("aggregated_device_count_active"),
        pa.field("aggregated_device_count_active", pa.int64()),
        pc.cast(pc.round(table.column("aggregated_device_count_active")), pa.int64()),
    )
    table = table.set_column(
        table.column_names.index("total_consumption_active_import"),
        pa.field("total_consumption_active_import", pa.int64()),
        pc.cast(pc.round(table.column("total_consumption_active_import")), pa.int64()),
    )

    # Cast datetime to timestamp
    table = table.set_column(
        table.column_names.index("data_collection_log_timestamp"),
        pa.field("data_collection_log_timestamp", pa.timestamp("ms", tz="UTC")),
        pc.cast(
            table.column("data_collection_log_timestamp"), pa.timestamp("ms", tz="UTC")
        ),
    )

    return table


def _add_unique_id_columns(dno: DNO, table: pa.Table) -> pa.Table:
    if dno == DNO.NGED:
        # Add unique id columns by combining separate columns
        table = table.append_column(
            pa.field("secondary_substation_unique_id", pa.string()),
            pc.binary_join_element_wise(
                table.column("dno_alias"),
                table.column("secondary_substation_id"),
                "-",
            ),
        )
        table = table.append_column(
            pa.field("lv_feeder_unique_id", pa.string()),
            pc.binary_join_element_wise(
                table.column("secondary_substation_unique_id"),
                table.column("lv_feeder_id"),
                "-",
            ),
        )
    elif dno == DNO.SSEN:
        # Add unique id columns using SSEN's dataset_id
        table = table.append_column(
            pa.field("secondary_substation_unique_id", pa.string()),
            pc.binary_join_element_wise(
                table.column("dno_alias"),
                pc.utf8_slice_codeunits(table.column("dataset_id"), 0, 10),
                "-",
            ),
        )
        table = table.append_column(
            pa.field("lv_feeder_unique_id", pa.string()),
            pc.binary_join_element_wise(
                table.column("dno_alias"),
                table.column("dataset_id"),
                "-",
            ),
        )
    else:
        raise ValueError(f"Unsupported DNO: {dno} for combined geoparquet")

    return table
