import json
import zlib
from abc import ABC, abstractmethod

import humanize
import requests
from dagster import AssetExecutionContext, ConfigurableResource
from fsspec.core import OpenFile
from zlib_ng import zlib_ng

from ..core import AvailableFile


class SSENAPIClient(ConfigurableResource, ABC):
    """API Client for SSEN's Smart Meter data"""

    available_files_url: str

    @abstractmethod
    def get_available_files(self) -> list[AvailableFile]:
        pass

    @abstractmethod
    def download_file(
        self, context: AssetExecutionContext, url: str, output_file: OpenFile
    ):
        pass

    def filename_for_url(self, url: str) -> str:
        return url.split("/")[-1]

    def _map_available_files(self, api_response: dict) -> list[AvailableFile]:
        available = [self._map_available_file(o) for o in api_response["objects"]]
        return sorted(available, key=lambda f: f.filename)

    def _map_available_file(self, response_object: dict) -> AvailableFile:
        url = response_object["downloadLink"]
        filename = self.filename_for_url(url)
        return AvailableFile(filename=filename, url=url)


class LiveSSENAPIClient(SSENAPIClient):
    # "https://ssen-smart-meter-prod.datopian.workers.dev/LV_FEEDER_USAGE/"
    def get_available_files(self) -> list[AvailableFile]:
        r = requests.get(self.available_files_url, timeout=5)
        r.raise_for_status()
        return self._map_available_files(r.json())

    def download_file(
        self,
        context: AssetExecutionContext,
        url: str,
        output_file: OpenFile,
    ) -> None:
        """Stream a file from the given URL to the given file, compressing on the fly"""
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            total_size = int(r.headers.get("content-length", 0))
            downloaded_size = 0
            context.log.info(
                f"Downloading {url} into {output_file} - total size: {humanize.naturalsize(total_size)}"
            )
            compressor = zlib_ng.compressobj(
                level=9, method=zlib.DEFLATED, wbits=zlib.MAX_WBITS | 16
            )
            for chunk in r.iter_content(chunk_size=10 * 1024 * 1024):
                self._log_download_progress(context, total_size, downloaded_size, chunk)
                output_file.write(compressor.compress(chunk))
            output_file.write(compressor.flush())
            context.log.info(
                f"Downloaded {url} - total size: {humanize.naturalsize(total_size)}"
            )

    def _log_download_progress(self, context, total_size, downloaded_size, chunk):
        if total_size > 0:
            downloaded_size += len(chunk)
            progress = int(downloaded_size / total_size * 100)
            context.log.info(
                f"{progress}% ({humanize.naturalsize(downloaded_size)}) downloaded"
            )


class TestSSENAPIClient(SSENAPIClient):
    file_to_download: str | None

    # ../weave_tests/fixtures/ssen_files.json
    def get_available_files(self) -> list[AvailableFile]:
        with open(self.available_files_url) as f:
            return self._map_available_files(json.load(f))

    def download_file(self, _context, _url, output_file) -> None:
        with open(self.file_to_download, "rb") as f:
            output_file.write(
                zlib_ng.compress(f.read(), level=1, wbits=zlib_ng.MAX_WBITS | 16)
            )
