import fsspec
from dagster import ConfigurableResource


class OutputFilesResource(ConfigurableResource):
    """Wrap fsspec to be aware of our naming conventions for raw files"""

    url: str

    def open(self, dno, filename, **open_kwargs):
        return fsspec.open(self.path(dno, filename), **open_kwargs)

    def path(self, dno, filename):
        return f"{self.url}/{dno}/{filename}"

    def delete(self, dno, filename):
        file = self.open(dno, filename)
        return file.fs.delete(file.path)
