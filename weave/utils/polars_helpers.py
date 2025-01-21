import polars as pl
from typing import Optional

def read_weave_parquet(
    month: str,
    dno: Optional[str] = None,
    bbox: Optional[tuple[float, float, float, float]] = None
) -> pl.LazyFrame:
    """Read Weave parquet data efficiently using Polars.
    
    Args:
        month: Month to load in format YYYY-MM
        dno: Optional DNO to filter by
        bbox: Optional bounding box as (min_lon, min_lat, max_lon, max_lat)
    
    Returns:
        LazyFrame with requested data
    """
    df = pl.scan_parquet(f"s3://weave.energy/beta/smart-meter/{month}.parquet")
    
    if dno:
        df = df.filter(pl.col("dno_alias") == dno.upper())
        
    if bbox:
        min_lon, min_lat, max_lon, max_lat = bbox
        # Parse geometry string into lon/lat columns
        df = df.with_columns([
            pl.col("geometry").str.extract(r"POINT\((.*) (.*)\)", 1).alias("lon"),
            pl.col("geometry").str.extract(r"POINT\((.*) (.*)\)", 2).alias("lat")
        ]).filter(
            (pl.col("lon") >= min_lon) & 
            (pl.col("lon") <= max_lon) &
            (pl.col("lat") >= min_lat) & 
            (pl.col("lat") <= max_lat)
        )
    
    return df