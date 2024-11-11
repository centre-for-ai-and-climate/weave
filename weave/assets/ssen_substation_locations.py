import geopandas as gpd
import pandas as pd
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    asset,
)
from dagster_pandas.data_frame import create_table_schema_metadata_from_dataframe

from ..core import DNO
from ..resources.ons import ONSAPIClient
from ..resources.output_files import OutputFilesResource
from ..resources.ssen import SSENAPIClient
from .ons import onspd


@asset(description="SSEN's raw LV Feeder -> Postcode mapping file")
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
        metadata["weave/nunique_feeders"] = df["lv_feeder_name"].nunique()
        metadata["weave/nunique_postcodes"] = df["postcode"].nunique()

    return MaterializeResult(metadata=metadata)


@asset(
    description="SSEN Substation location lookup table, built from their LV Feeder -> Substation mapping",
    deps=[ssen_lv_feeder_postcode_mapping, onspd],
)
def ssen_substation_location_lookup_feeder_postcodes(
    context: AssetExecutionContext,
    raw_files_resource: OutputFilesResource,
    staging_files_resource: OutputFilesResource,
    ssen_api_client: SSENAPIClient,
    ons_api_client: ONSAPIClient,
) -> MaterializeResult:
    filename = "substation_location_lookup_feeder_postcodes.parquet"
    metadata = {}
    df, onspd_df = None, None
    # Read in the LV Feeder -> Postcode mapping and ONSPD
    with raw_files_resource.open(
        DNO.SSEN.value, "lv_feeder_postcode_mapping.csv.gz", mode="rb"
    ) as f:
        df = ssen_api_client.lv_feeder_postcode_lookup_dataframe(f)
    with raw_files_resource.open("ons", "onspd.zip", mode="rb") as f:
        onspd_df = ons_api_client.onspd_dataframe(f, cols=["pcd", "lat", "long"])

    # Standardise the postcodes and add substation ids, so we can match them
    df = _add_standardised_postcode_to_dataframe(df, postcode_column="postcode")

    metadata["weave/invalid_feeder_postcodes"] = len(
        df[
            df["standardised_postcode"].isna()
            | (df["standardised_postcode"].str.len() < 5)
        ]
    )

    # Drop rows which are clearly invalid, or at least unusable for our purposes
    # Doesn't get rid of every invalid postcode, but 5 is the shortest valid UK postcode so
    # it's on the safe side.
    df = df.drop(
        df[
            df["standardised_postcode"].isna()
            | (df["standardised_postcode"].str.len() < 5)
        ].index,
    )

    df["substation_nrn"] = df.apply(_substation_nrn_from_lv_feeder_lookup, axis=1)

    df = df[["standardised_postcode", "substation_nrn"]]

    onspd_df = _add_standardised_postcode_to_dataframe(onspd_df, postcode_column="pcd")
    onspd_df = onspd_df.drop(columns=["pcd"]).dropna()

    # Merge the two together
    df = df.merge(onspd_df, on="standardised_postcode", how="left")

    metadata["weave/unmatched_feeder_postcodes"] = df[
        df["lat"].isna()
    ].standardised_postcode.nunique()

    df = df.dropna(subset=["lat", "long"])

    # Make it geographical, so we can do some geo-filtering
    gdf = (
        gpd.GeoDataFrame(
            df, geometry=gpd.points_from_xy(df.long, df.lat), crs="EPSG:4326"
        )
        # Group by substation NRN
        .dissolve("substation_nrn")
        # Combine all the points into one geometry
        .convex_hull
        # GeoPandas does some weirdness when we do this, so get back to a normal
        # GeoDataFrame with the convex hull as the geometry
        .reset_index()
        .rename(columns={0: "geometry"})
        .set_geometry("geometry")
        # We want to filter out bad substations using measurements in meters, so we need
        # a CRS that can do that - OSGB36/British National Grid/EPSG:27700 is ideal.
        .to_crs("EPSG:27700")
    )

    metadata["weave/nunique_substations"] = len(gdf)
    metadata["weave/single_point_locations"] = len(gdf[gdf.geometry.type == "Point"])
    metadata["weave/line_locations"] = len(gdf[gdf.geometry.type == "LineString"])
    metadata["weave/polygon_locations"] = len(gdf[gdf.geometry.type == "Polygon"])
    metadata["weave/excluded_lines"] = len(
        gdf[(gdf.geometry.type == "LineString") & (gdf.geometry.length > 100)]
    )
    metadata["weave/excluded_polygons"] = len(
        gdf[
            (gdf.geometry.type == "Polygon")
            & (gdf.geometry.area > 10000)
            & (gdf.geometry.length / gdf.geometry.count_coordinates() > 100)
        ]
    )

    # Filter out locations we can't really trust because they cover too broad an area
    # to be useful for a centroid-based location
    gdf = gdf[
        (gdf.geometry.type == "Point")
        | ((gdf.geometry.type == "LineString") & (gdf.geometry.length <= 100))
        | (
            (gdf.geometry.type == "Polygon")
            & (gdf.geometry.area <= 10000)
            & (gdf.geometry.length / gdf.geometry.count_coordinates() <= 100)
        )
    ]

    # Create our output lookup table, which uses the centroid of the geometry as the
    # location of the substation and is in a more useful CRS for downstream users
    gdf["centroid"] = gdf.geometry.centroid
    gdf = (
        gdf[["substation_nrn", "centroid"]]
        .rename(columns={"centroid": "geometry"})
        .set_geometry("geometry")
        .to_crs("EPSG:4326")
    )

    metadata["dagster/row_count"] = len(gdf)
    metadata["dagster/column_schema"] = create_table_schema_metadata_from_dataframe(gdf)

    with staging_files_resource.open(DNO.SSEN.value, filename, mode="wb") as output:
        gdf.to_parquet(output, index=False)
        metadata["dagster/uri"] = output.name

    return MaterializeResult(metadata=metadata)


def _add_standardised_postcode_to_dataframe(
    df: pd.DataFrame, postcode_column: str
) -> pd.DataFrame:
    df["standardised_postcode"] = (
        df[postcode_column].str.strip().str.replace(r"\s", "", regex=True).str.upper()
    )
    return df


def _substation_nrn_from_lv_feeder_lookup(row):
    """Format the Network Reference Number for a substation from component columns
    in the LV Feeder postcode lookup file"""
    return f"{row["primary_substation_id"]}_{row["hv_feeder_id"]}_{row["secondary_substation_id"]}"
