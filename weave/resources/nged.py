import json
import re
import zlib
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import ClassVar

import humanize
import pyarrow as pa
import pyarrow.csv as pa_csv
import requests
from dagster import AssetExecutionContext, ConfigurableResource
from fsspec.core import OpenFile
from zlib_ng import gzip_ng_threaded, zlib_ng

from .ssen import AvailableFile


class NGEDAPIClient(ConfigurableResource, ABC):
    """API Client for NGED's open data"""

    lv_feeder_datapackage_url: str = "https://connecteddata.nationalgrid.co.uk/dataset/aggregated-smart-meter-data-lv-feeder/datapackage.json"
    ckan_base_url: str = "https://connecteddata.nationalgrid.co.uk"
    api_token: str

    lv_feeder_pyarrow_schema: ClassVar = pa.schema(
        [
            ("dataset_id", pa.string()),
            ("dno_alias", pa.string()),
            ("secondary_substation_id", pa.string()),
            ("secondary_substation_name", pa.string()),
            ("LV_feeder_ID", pa.string()),
            ("LV_feeder_name", pa.string()),
            ("substation_geo_location", pa.string()),
            ("aggregated_device_count_Active", pa.float64()),
            ("Total_consumption_active_import", pa.float64()),
            ("data_collection_log_timestamp", pa.timestamp("ms", tz="UTC")),
            ("Insert_time", pa.timestamp("ms", tz="UTC")),
            ("last_modified_time", pa.timestamp("ms", tz="UTC")),
        ]
    )

    def _request_headers(self):
        return {"Authorization": self.api_token}

    @abstractmethod
    def get_available_files(self) -> list[AvailableFile]:
        pass

    @abstractmethod
    def download_file(
        self,
        context: AssetExecutionContext,
        url: str,
        output_file: OpenFile,
        gzip: bool = True,
    ):
        pass

    @classmethod
    def filename_for_url(cls, url: str) -> str:
        return url.split("/")[-1]

    @classmethod
    def month_partition_from_url(cls, url: str) -> str:
        filename = cls.filename_for_url(url)
        bare_filename = re.sub(r"-part\d{4}\.csv$", "", filename)
        year, month, _day = bare_filename.split("-")
        return f"{year}-{month}-01"

    def _map_available_files(self, api_response: dict) -> list[AvailableFile]:
        available = []
        for resource in api_response["resources"]:
            url = resource["url"]
            filename = self.filename_for_url(url)
            created = datetime.fromisoformat(resource["created"]).replace(
                tzinfo=timezone.utc
            )
            if re.match(
                r"aggregated-smart-meter-data-lv-feeder-\d{4}-\d{2}-part\d{4}\.csv",
                filename,
            ):
                available.append(
                    AvailableFile(filename=filename, url=url, created=created)
                )
        return sorted(available, key=lambda f: f.filename)

    def lv_feeder_file_pyarrow_table(self, input_file: OpenFile):
        """Read an LV Feeder CSV file into a PyArrow Table."""
        pyarrow_csv_convert_options = pa_csv.ConvertOptions(
            column_types=self.lv_feeder_pyarrow_schema,
            # Note seemingly random capitalisation of column names
            include_columns=[
                "dataset_id",
                "dno_alias",
                "secondary_substation_id",
                "secondary_substation_name",
                "LV_feeder_ID",
                "LV_feeder_name",
                "substation_geo_location",
                "aggregated_device_count_Active",
                "Total_consumption_active_import",
                "data_collection_log_timestamp",
                "Insert_time",
                "last_modified_time",
            ],
        )
        with gzip_ng_threaded.open(input_file, "rb", threads=pa.io_thread_count()) as f:
            return pa_csv.read_csv(f, convert_options=pyarrow_csv_convert_options)


class LiveNGEDAPIClient(NGEDAPIClient):
    def get_available_files(self) -> list[AvailableFile]:
        r = requests.get(
            self.lv_feeder_datapackage_url, timeout=5, headers=self._request_headers()
        )
        r.raise_for_status()
        return self._map_available_files(r.json())

    def download_file(
        self,
        context: AssetExecutionContext,
        url: str,
        output_file: OpenFile,
        gzip: bool = True,
    ) -> None:
        """Stream a file from the given URL to the given file, optionally gzip
        compressing on the fly"""
        with requests.get(url, stream=True, headers=self._request_headers()) as r:
            r.raise_for_status()
            total_size = int(r.headers.get("content-length", 0))
            downloaded_size = 0
            context.log.info(
                f"Downloading {url} into {output_file} - total size: {humanize.naturalsize(total_size)}"
            )

            if gzip:
                compressor = zlib_ng.compressobj(
                    level=9, method=zlib.DEFLATED, wbits=zlib.MAX_WBITS | 16
                )
            for chunk in r.iter_content(chunk_size=10 * 1024 * 1024):
                downloaded_size += len(chunk)
                self._log_download_progress(context, total_size, downloaded_size)
                if gzip:
                    output_file.write(compressor.compress(chunk))
                else:
                    output_file.write(chunk)
            if gzip:
                output_file.write(compressor.flush())
            context.log.info(
                f"Downloaded {url} - total size: {humanize.naturalsize(total_size)}"
            )

    def _log_download_progress(self, context, total_size, downloaded_size):
        if total_size > 0:
            progress = int(downloaded_size / total_size * 100)
            context.log.info(
                f"{progress}% ({humanize.naturalsize(downloaded_size)}) downloaded"
            )


class StubNGEDAPICLient(NGEDAPIClient):
    file_to_download: str | None

    def get_available_files(self) -> list[AvailableFile]:
        with open(self.lv_feeder_datapackage_url) as f:
            return self._map_available_files(json.load(f))

    def download_file(self, _context, _url, output_file, gzip=True) -> None:
        with open(self.file_to_download, "rb") as f:
            if gzip:
                output_file.write(
                    zlib_ng.compress(f.read(), level=1, wbits=zlib_ng.MAX_WBITS | 16)
                )
            else:
                output_file.write(f.read())
