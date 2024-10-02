from collections import defaultdict

import fsspec
import requests
from dagster import Config, asset
from pydantic import BaseModel, HttpUrl

from .core import DNO


class DNOFilesConfig(Config):
    url: str  # Dagster can't use Pydantic's full suite of types :(


class AvailableFile(BaseModel):
    filename: str
    url: HttpUrl

    def __hash__(self):
        return hash((self.filename, self.url))

    def __eq__(self, other):
        return self.filename == other.filename and self.url == other.url

    def __lt__(self, other):
        return self.filename < other.filename


@asset(description="List of available SSEN files")
def ssen_files(config: DNOFilesConfig) -> set[AvailableFile]:
    r = requests.get(config.url)
    r.raise_for_status()
    return {_ssen_available_files(o) for o in r.json()["objects"]}


def _ssen_available_files(response_object: dict) -> AvailableFile:
    url = response_object["downloadLink"]
    filename = url.split("/")[-1]
    return AvailableFile(filename=filename, url=url)


@asset(description="List of available UKPN files")
def ukpn_files(config: DNOFilesConfig) -> set[AvailableFile]:
    # There is just one file for UKPN for now, but we've made it configurable
    # just in case it changes
    file = AvailableFile(filename=config.url.split("/")[-1], url=config.url)
    return {file}


class NPGFilesConfig(DNOFilesConfig):
    api_key: str


@asset(description="List of available NPG files")
def npg_files(config: NPGFilesConfig) -> set[AvailableFile]:
    r = requests.get(config.url, headers={"Authorization": f"Apikey {config.api_key}"})
    r.raise_for_status()
    return {
        _npg_filename_and_url(a)
        for a in r.json()["attachments"]
        if "feeder" in a["metas"]["id"]
    }


def _npg_filename_and_url(response_attachment: dict) -> AvailableFile:
    url = response_attachment["href"]
    filename = response_attachment["metas"]["title"].lower()
    return AvailableFile(filename=filename, url=url)


class NGEDFilesConfig(DNOFilesConfig):
    api_token: str


@asset(description="List of available NGED files")
def nged_files(config: NGEDFilesConfig) -> set[AvailableFile]:
    r = requests.get(config.url, headers={"Authorization": config.api_token})
    r.raise_for_status()
    return {_nged_filename_and_url(r) for r in r.json()["result"]["resources"]}


def _nged_filename_and_url(response_resource):
    url = response_resource["url"]
    filename = response_resource["name"].lower()
    return AvailableFile(filename=filename, url=url)


class DownloadedFilesConfig(Config):
    filesystem: str
    root_directory: str


@asset(description="List of already downloaded files for all DNOs")
def downloaded_files(config: DownloadedFilesConfig) -> dict[DNO, set[str]]:
    # Using fsspec to make this portable across S3 and local files
    fs = fsspec.filesystem(config.filesystem)
    existing = fs.find(config.root_directory)
    parts = list(map(_dno_and_filename, existing))
    downloaded = defaultdict(set)
    for dno, file in parts:
        downloaded[dno].add(file)
    return downloaded


def _dno_and_filename(path: str) -> tuple[DNO, str]:
    parts = path.split("/")
    return DNO(parts[-2]), parts[-1]


@asset(description="List of new files from SSEN")
def new_ssen_files(
    downloaded_files: dict[DNO, set[str]], ssen_files: set[AvailableFile]
) -> set[AvailableFile]:
    return _new_files_for_DNO(downloaded_files, ssen_files, DNO.SSEN)


@asset(description="List of new files from UKPN")
def new_ukpn_files(
    downloaded_files: dict[DNO, set[str]], ukpn_files: set[AvailableFile]
) -> set[AvailableFile]:
    return _new_files_for_DNO(downloaded_files, ukpn_files, DNO.UKPN)


@asset(description="List of new files from NPG")
def new_npg_files(
    downloaded_files: dict[DNO, set[str]], npg_files: set[AvailableFile]
) -> set[AvailableFile]:
    return _new_files_for_DNO(downloaded_files, npg_files, DNO.NPG)


@asset(description="List of new files from NGED")
def new_nged_files(
    downloaded_files: dict[DNO, set[str]], nged_files: set[AvailableFile]
) -> set[AvailableFile]:
    return _new_files_for_DNO(downloaded_files, nged_files, DNO.NGED)


def _new_files_for_DNO(
    downloaded_files: dict[DNO, set[str]], available_files: set[AvailableFile], dno: DNO
) -> set[AvailableFile]:
    downloaded = downloaded_files.get(dno, set())
    return {f for f in available_files if f.filename not in downloaded}
