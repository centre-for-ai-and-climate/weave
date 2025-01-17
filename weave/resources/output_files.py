import fsspec
from dagster import ConfigurableResource


class OutputFilesResource(ConfigurableResource):
    """Wrap fsspec to be aware of our naming conventions for raw files"""

    url: str

    def fs(self):
        return fsspec.url_to_fs(self.url)[0]

    def open(self, dno, filename, **open_kwargs):
        return fsspec.open(self.path(dno, filename), **open_kwargs)

    def get_file(self, dno, rfilename, lpath):
        return self.fs().get_file(self.path(dno, rfilename), lpath)

    def put_file(self, lpath, dno, rfilename):
        return self.fs().put_file(lpath, self.path(dno, rfilename))

    def path(self, dno, filename):
        return f"{self.url}/{dno}/{filename}"

    def delete(self, dno, filename):
        file = self.open(dno, filename)
        return file.fs.delete(file.path)
