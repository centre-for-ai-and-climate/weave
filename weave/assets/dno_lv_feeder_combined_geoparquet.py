from datetime import datetime

import geopandas as gpd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from dagster import (
    AssetExecutionContext,
    AutomationCondition,
    MaterializeResult,
    MonthlyPartitionsDefinition,
    asset,
)
from geopandas.io.arrow import _geopandas_to_arrow

from ..core import DNO, lv_feeder_geoparquet_schema
from ..resources.output_files import OutputFilesResource


@asset(
    description="""Combined LV Feeder GeoParquet from all DNOs.

    An ever-growing monthly-partitioned geoparquet file containing all the low-voltage
    feeder data we have.""",
    partitions_def=MonthlyPartitionsDefinition(start_date="2024-02-01", end_offset=1),
    deps=["ssen_lv_feeder_monthly_parquet"],
    automation_condition=AutomationCondition.on_missing(),
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

    with output_files_resource.open("smart-meter", monthly_file, mode="wb") as out:
        sorting_columns = [
            pq.SortingColumn(4),
            pq.SortingColumn(1),
            pq.SortingColumn(6),
            pq.SortingColumn(7),
        ]
        parquet_writer = pq.ParquetWriter(
            out,
            schema=lv_feeder_geoparquet_schema,
            compression="zstd",
            compression_level=22,
            coerce_timestamps="ms",
            allow_truncated_timestamps=True,
            sorting_columns=sorting_columns,
            store_decimal_as_integer=True,
        )
        metadata["dagster/uri"] = output_files_resource.path(
            "smart-meter", monthly_file
        )
        # Eventually this should loop over several DNOs and do different things for each
        with staging_files_resource.open(
            DNO.SSEN.value, monthly_file, mode="rb"
        ) as in_file:
            parquet_file = pq.ParquetFile(in_file)
            total_rows = parquet_file.metadata.num_rows
            processed_rows = 0
            context.log.info(f"Processing file: {in_file}, total_rows: {total_rows}")
            for batch in parquet_file.iter_batches(
                batch_size=1024 * 1024,
                columns=[
                    "dataset_id",
                    "dno_alias",
                    "aggregated_device_count_active",
                    "total_consumption_active_import",
                    "data_collection_log_timestamp",
                    "substation_geo_location",
                ],
            ):
                table = _ssen_to_combined_geoparquet(batch, context)
                table = table.sort_by(
                    [
                        ("data_collection_log_timestamp", "ascending"),
                        ("dno_alias", "ascending"),
                        ("secondary_substation_unique_id", "ascending"),
                        ("lv_feeder_unique_id", "ascending"),
                    ]
                )
                parquet_writer.write_table(table)
                metadata["dagster/row_count"] += table.num_rows
                metadata["weave/nunique_feeders"] += pc.count_distinct(
                    table.column("lv_feeder_unique_id")
                ).as_py()
                metadata["weave/nunique_substations"] += pc.count_distinct(
                    table.column("secondary_substation_unique_id")
                ).as_py()

                processed_rows += table.num_rows
                percentage_processed = int(processed_rows / total_rows * 100)
                context.log.info(
                    f"Processed {processed_rows} rows ({percentage_processed}% of total)"
                )
        parquet_writer.close()

    return MaterializeResult(metadata=metadata)


def _ssen_to_combined_geoparquet(
    batch: pa.RecordBatch, context: AssetExecutionContext
) -> pa.Table:
    # Clear any existing metadata
    batch = batch.replace_schema_metadata(None)

    # Add geoarrow-encoded geometry columns.
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

    # Add unique id columns
    table = table.append_column(
        pa.field("secondary_substation_unique_id", pa.string()),
        pc.utf8_slice_codeunits(table.column("dataset_id"), 0, 10),
    )
    table = table.append_column(
        pa.field("lv_feeder_unique_id", pa.string()),
        table.column("dataset_id"),
    )

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
