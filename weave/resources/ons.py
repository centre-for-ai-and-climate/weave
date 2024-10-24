import io
import re
import zipfile
from abc import ABC, abstractmethod

import humanize
import numpy as np
import pandas as pd
import requests
from dagster import AssetExecutionContext, ConfigurableResource
from fsspec.core import OpenFile

CSV_REGEX = r"data/onspd_.*\.csv"
NULL_LAT = "99.999999"
NULL_LONG = "0.000000"


class ONSAPIClient(ConfigurableResource, ABC):
    """API Client for ONS data"""

    onspd_url: str

    @abstractmethod
    def download_onspd(self, output_file: OpenFile) -> None:
        pass

    def onspd_dataframe(self, input_file: OpenFile) -> pd.DataFrame:
        """Turn the ONSPD CSV file into a DataFrame of the data we actually use, i.e.
        postcode, latitude and longitude"""
        with zipfile.ZipFile(io.BytesIO(input_file.read())) as z:
            filename = next(
                filter(
                    lambda x: re.match(CSV_REGEX, x, flags=re.IGNORECASE),
                    z.namelist(),
                )
            )
            with z.open(filename) as f:
                # Need to use Pandas for this because pyarrow doesn't support different null
                # values for different columns
                df = pd.read_csv(
                    f,
                    usecols=["pcd", "lat", "long"],
                    dtype={"pcd": str, "lat": np.float64, "long": np.float64},
                    na_values={"lat": NULL_LAT, "long": NULL_LONG},
                )
                df.dropna(inplace=True)
                return df

    def _log_download_progress(self, context, total_size, downloaded_size):
        if total_size > 0:
            progress = int(downloaded_size / total_size * 100)
            context.log.info(
                f"{progress}% ({humanize.naturalsize(downloaded_size)}) downloaded"
            )


class LiveONSAPIClient(ONSAPIClient):
    def download_onspd(
        self, output_file: OpenFile, context: AssetExecutionContext
    ) -> None:
        with requests.get(self.onspd_url, stream=True) as r:
            r.raise_for_status()
            total_size = int(r.headers.get("content-length", 0))
            downloaded_size = 0
            context.log.info(
                f"Downloading {self.onspd_url} into {output_file} - total size: {humanize.naturalsize(total_size)}"
            )
            for chunk in r.iter_content(chunk_size=10 * 1024 * 1024):
                downloaded_size += len(chunk)
                self._log_download_progress(context, total_size, downloaded_size)
                output_file.write(chunk)
            context.log.info(
                f"Downloaded {self.onspd_url} - total size: {humanize.naturalsize(total_size)}"
            )
