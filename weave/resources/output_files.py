import fsspec
from dagster import ConfigurableResource


class OutputFilesResource(ConfigurableResource):
    """Wrap fsspec to be aware of our naming conventions for raw files"""

    url: str

    def open(self, dno, filename, **open_kwargs):
        return fsspec.open(f"{self.url}/{dno}/{filename}", **open_kwargs)
