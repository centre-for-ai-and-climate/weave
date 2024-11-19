import geopandas as gpd
import pandas as pd
import pyproj
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    asset,
)
from dagster_pandas.data_frame import create_table_schema_metadata_from_dataframe
from pyproj import Transformer
from pyproj.transformer import TransformerGroup
from shapely import Point

from ..core import DNO
from ..resources.ons import ONSAPIClient
from ..resources.output_files import OutputFilesResource
from ..resources.ssen import SSENAPIClient
from .ons import onspd

# Allow pyproj to download grid shift files from their CDN for more accurate conversions
# See ADR 0003 for more information on why we're doing this.
pyproj.network.set_network_enabled(True)
tg = TransformerGroup("EPSG:27700", "EPSG:4326")
assert tg.best_available, "PyProj could not get the OSNT15 grid shift file for OSGB36 -> WGS84 conversion, this is essential for accurate location conversion"


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
        metadata["dagster/uri"] = raw_files_resource.path(DNO.SSEN.value, filename)
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
    df, metadata = _filter_invalid_postcodes(df, "standardised_postcode", metadata)
    df["substation_nrn"] = df.apply(_substation_nrn_from_lv_feeder_lookup, axis=1)
    df = df[["standardised_postcode", "substation_nrn"]]

    onspd_df = _add_standardised_postcode_to_dataframe(onspd_df, postcode_column="pcd")
    onspd_df = onspd_df.drop(columns=["pcd"]).dropna()

    # Merge the two together
    df = df.merge(onspd_df, on="standardised_postcode", how="left")
    df, metadata = _filter_missing_locations(df, metadata)

    # Turn feeder locations into substation locations
    df, metadata = _centroid_substation_locations(df, metadata)

    metadata["dagster/row_count"] = len(df)
    metadata["dagster/column_schema"] = create_table_schema_metadata_from_dataframe(df)

    with staging_files_resource.open(DNO.SSEN.value, filename, mode="wb") as output:
        df.to_parquet(output, index=False)
        metadata["dagster/uri"] = staging_files_resource.path(DNO.SSEN.value, filename)

    return MaterializeResult(metadata=metadata)


def _add_standardised_postcode_to_dataframe(
    df: pd.DataFrame, postcode_column: str
) -> pd.DataFrame:
    df["standardised_postcode"] = (
        df[postcode_column].str.strip().str.replace(r"\s", "", regex=True).str.upper()
    )
    return df


def _filter_invalid_postcodes(
    df: pd.DataFrame, postcode_column: str, metadata: dict
) -> tuple[pd.DataFrame, dict]:
    """Drop rows which are clearly invalid, or at least unusable for our purposes of
    postcode matching. Doesn't get rid of every invalid postcode, but 5 is the shortest
    valid UK postcode so it's on the safe side."""
    metadata["weave/invalid_feeder_postcodes"] = len(
        df[df[postcode_column].isna() | (df[postcode_column].str.len() < 5)]
    )
    df = df.drop(
        df[df[postcode_column].isna() | (df[postcode_column].str.len() < 5)].index,
    )
    return (df, metadata)


def _filter_missing_locations(
    df: pd.DataFrame, metadata: dict
) -> tuple[pd.DataFrame, dict]:
    """Drop rows which don't have lat/lng, thanks to no matching postcode in the ONSPD
    data"""
    metadata["weave/unmatched_feeder_postcodes"] = df[
        df["lat"].isna() | df["long"].isna()
    ].standardised_postcode.nunique()
    df = df.dropna(subset=["lat", "long"])
    return (df, metadata)


def _substation_nrn_from_lv_feeder_lookup(row):
    """Format the Network Reference Number for a substation from component columns
    in the LV Feeder postcode lookup file"""
    primary_substation_id = row["primary_substation_id"].zfill(4)
    hv_feeder_id = row["hv_feeder_id"].zfill(3)
    secondary_substation_id = row["secondary_substation_id"].zfill(3)
    return f"{primary_substation_id}{hv_feeder_id}{secondary_substation_id}"


def _centroid_substation_locations(
    df: pd.DataFrame, metadata: dict
) -> tuple[pd.DataFrame, dict]:
    """Convert a DataFrame of feeder locations into a GeoDataFrame of substation
    locations in the "lat,long" string format used in the lv feeder data.

    Groups individual rows by substation NRN, then finds the centroid of the convex hull
    of the individual point(s).

    Excludes groups of points that are too far apart for a centroid to be a useful
    location approximation."""
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

    gdf, metadata = _filter_unlikely_substation_areas(gdf, metadata)

    # Create our output lookup table, which uses the centroid of the geometry as the
    # location of the substation and is in a more useful CRS for downstream users
    gdf["centroid"] = gdf.geometry.centroid
    gdf = (
        gdf[["substation_nrn", "centroid"]]
        .rename(columns={"centroid": "geometry"})
        .set_geometry("geometry")
        .to_crs("EPSG:4326")
    )

    df = _gdf_to_df_with_substation_geo_location(gdf)

    return (df, metadata)


def _filter_unlikely_substation_areas(
    gdf: gpd.GeoDataFrame, metadata: dict
) -> tuple[gpd.GeoDataFrame, dict]:
    metadata["weave/nunique_substations"] = len(gdf)
    metadata["weave/single_point_locations"] = len(gdf[gdf.geometry.type == "Point"])
    metadata["weave/line_locations"] = len(gdf[gdf.geometry.type == "LineString"])
    metadata["weave/polygon_locations"] = len(gdf[gdf.geometry.type == "Polygon"])
    metadata["weave/excluded_lines"] = len(
        gdf[(gdf.geometry.type == "LineString") & (gdf.geometry.length > 100)]
    )
    metadata["weave/excluded_polygons"] = len(
        gdf[
            (
                (gdf.geometry.type == "Polygon")
                & (gdf.geometry.area > 10000)
                & (gdf.geometry.length / gdf.geometry.count_coordinates() > 100)
            )
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

    return (gdf, metadata)


def _gdf_to_df_with_substation_geo_location(gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """Convert a GeoDataFrame with point geometry column into a DataFrame with a
    'substation_geo_location' string lat,lng column, like the lv feeder data schema, for
    easier downstream use without GeoPandas."""

    gdf["substation_geo_location"] = gdf.geometry.apply(
        # Output in Lat,Lng i.e. y,x
        lambda geometry: f"{geometry.y:.6f},{geometry.x:.6f}"
    )

    return pd.DataFrame(gdf[["substation_nrn", "substation_geo_location"]])


@asset(description="SSEN's raw Transformer Load Model file")
def ssen_transformer_load_model(
    context: AssetExecutionContext,
    raw_files_resource: OutputFilesResource,
    ssen_api_client: SSENAPIClient,
) -> MaterializeResult:
    metadata = {}
    filename = "transformer_load_model.zip"
    url = ssen_api_client.transformer_load_model_url
    with raw_files_resource.open(DNO.SSEN.value, filename, mode="wb") as f:
        ssen_api_client.download_file(context, url, f, gzip=False)

    with raw_files_resource.open(DNO.SSEN.value, filename, mode="rb") as f:
        df = ssen_api_client.transformer_load_model_dataframe(f)
        metadata["dagster/uri"] = raw_files_resource.path(DNO.SSEN.value, filename)
        metadata["dagster/row_count"] = len(df)
        metadata["dagster/column_schema"] = create_table_schema_metadata_from_dataframe(
            df
        )
        metadata["weave/source"] = url
        metadata["weave/nunique_substations"] = df["full_nrn"].nunique()
        metadata["weave/number_of_na_locations"] = len(
            df[df.latitude.isna() | df.longitude.isna()]
        )
    return MaterializeResult(metadata=metadata)


@asset(
    description="SSEN substation lookup table, built from their Transformer Load Model",
    deps=[ssen_transformer_load_model],
)
def ssen_substation_location_lookup_transformer_load_model(
    context: AssetExecutionContext,
    raw_files_resource: OutputFilesResource,
    staging_files_resource: OutputFilesResource,
    ssen_api_client: SSENAPIClient,
) -> MaterializeResult:
    metadata = {}
    filename = "substation_location_lookup_transformer_load_model.parquet"
    gdf = None
    with raw_files_resource.open(
        DNO.SSEN.value, "transformer_load_model.zip", mode="rb"
    ) as f:
        df = ssen_api_client.transformer_load_model_dataframe(
            f, cols=["full_nrn", "latitude", "longitude"]
        )
        # The load model contains forecasts for various years and scenarios for the same
        # substation but the locations are the same throughout, so we can dedupe
        df = df.drop_duplicates(subset=["full_nrn"])
        # Some have no location, which is no use to us
        df = df.dropna(subset=["latitude", "longitude"])
        gdf = gpd.GeoDataFrame(
            df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326"
        )
        gdf = gdf.drop(columns=["latitude", "longitude"])
        gdf = gdf.rename(columns={"full_nrn": "substation_nrn"})
        # Match the nrn format in lv feeder data
        gdf["substation_nrn"] = gdf.apply(
            _substation_nrn_from_transformer_load_model, axis=1
        )
        gdf = _fix_bng_grid_shift(gdf)
        # Output location in the same format as the lv feeder data
        df = _gdf_to_df_with_substation_geo_location(gdf)

        metadata["dagster/row_count"] = len(df)
        metadata["dagster/column_schema"] = create_table_schema_metadata_from_dataframe(
            df
        )
        metadata["weave/nunique_substations"] = df["substation_nrn"].nunique()

    with staging_files_resource.open(DNO.SSEN.value, filename, mode="wb") as output:
        df.to_parquet(output, index=False)
        metadata["dagster/uri"] = staging_files_resource.path(DNO.SSEN.value, filename)

    return MaterializeResult(metadata=metadata)


def _substation_nrn_from_transformer_load_model(row):
    """The full NRNs in the Transformer Load Model do not always match the format in the
    LV feeder data, so we need to reformat them"""
    components = row["substation_nrn"].split("_")
    assert len(components) == 3, f"Unexpected NRN format: {row['substation_nrn']}"

    primary_substation_id = components[0].zfill(4)
    hv_feeder_id = components[1].zfill(3)
    secondary_substation_id = components[2].zfill(3)

    return f"{primary_substation_id}{hv_feeder_id}{secondary_substation_id}"


def _fix_bng_grid_shift(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Fixes Point geometries that have been badly converted from BNG to WGS84 without
    the necessary grid shift file."""
    # Create the transformers we need from explicit proj pipeline strings so that we're
    # sure we're using (or not using) the correct grid shift file
    bad_transformer = Transformer.from_pipeline(
        "proj=pipeline step proj=axisswap order=2,1 step proj=unitconvert xy_in=deg xy_out=rad step proj=tmerc lat_0=49 lon_0=-2 k=0.9996012717 x_0=400000 y_0=-100000 ellps=airy"
    )
    good_transformer = Transformer.from_pipeline(
        "proj=pipeline step inv proj=tmerc lat_0=49 lon_0=-2 k=0.9996012717 x_0=400000 y_0=-100000 ellps=airy step proj=hgridshift grids=uk_os_OSTN15_NTv2_OSGBtoETRS.tif step proj=unitconvert xy_in=rad xy_out=deg step proj=axisswap order=2,1"
    )

    def fix_ssen_transforms(point):
        # Make sure we're messing with the Geometry types we expect
        assert isinstance(point, Point)
        # Note the y, x (lat, lon) order here - pyproj transformers respect the CRS' order
        easting, northing = bad_transformer.transform(point.y, point.x)
        x, y = good_transformer.transform(easting, northing)
        return Point(y, x)

    gdf["geometry"] = gdf.geometry.apply(fix_ssen_transforms)
    return gdf
