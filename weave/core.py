import json
from datetime import datetime
from enum import Enum

import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
from pydantic import BaseModel, HttpUrl


class DNO(Enum):
    NGED = "nged"
    NPG = "npg"
    UKPN = "ukpn"
    SSEN = "ssen"
    ENWL = "enwl"
    SPEN = "spen"


class AvailableFile(BaseModel):
    filename: str
    url: HttpUrl
    created: datetime = None

    def __hash__(self):
        return hash((self.filename, self.url, self.created))

    def __eq__(self, other):
        return (
            self.filename == other.filename
            and self.url == other.url
            and self.created == other.created
        )


lv_feeder_parquet_schema = pa.schema(
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

lv_feeder_parquet_sort_order = [
    ("data_collection_log_timestamp", "ascending"),
    ("dno_alias", "ascending"),
    ("secondary_substation_id", "ascending"),
    ("lv_feeder_id", "ascending"),
]

lv_feeder_parquet_sorting_columns = pq.SortingColumn.from_ordering(
    lv_feeder_parquet_schema, lv_feeder_parquet_sort_order
)

lv_feeder_geoparquet_schema = pa.schema(
    [
        ("dataset_id", pa.string()),
        ("dno_alias", pa.string()),
        ("aggregated_device_count_active", pa.int64()),
        ("total_consumption_active_import", pa.int64()),
        ("data_collection_log_timestamp", pa.timestamp("ms", tz="UTC")),
        (
            pa.field(
                "geometry",
                pa.struct(
                    [
                        pa.field("x", pa.float64(), nullable=False),
                        pa.field("y", pa.float64(), nullable=False),
                    ]
                ),
                metadata={
                    "ARROW:extension:name": "geoarrow.point",
                    "ARROW:extension:metadata": json.dumps(
                        {"crs": pyproj.CRS.from_string("EPSG:4326").to_json()}
                    ),
                },
            )
        ),
        ("secondary_substation_unique_id", pa.string()),
        ("lv_feeder_unique_id", pa.string()),
    ],
    metadata={
        b"geo": json.dumps(
            {
                "primary_column": "geometry",
                "columns": {
                    "geometry": {
                        "encoding": "point",
                        "crs": pyproj.CRS.from_string("EPSG:4326").to_json(),
                        "geometry_types": ["Point"],
                    }
                },
                "schema_version": "1.1.0",
            }
        ).encode("utf-8"),
    },
)

lv_feeder_geoparquet_sort_order = [
    ("data_collection_log_timestamp", "ascending"),
    ("dno_alias", "ascending"),
    ("secondary_substation_unique_id", "ascending"),
    ("lv_feeder_unique_id", "ascending"),
]

lv_feeder_geoparquet_sorting_columns = pq.SortingColumn.from_ordering(
    lv_feeder_geoparquet_schema, lv_feeder_geoparquet_sort_order
)
