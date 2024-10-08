from enum import Enum

from pydantic import BaseModel, HttpUrl


class DNO(Enum):
    NGED = "nged"
    NPG = "npg"
    UKPN = "ukpn"
    SSEN = "ssen"
    ENWL = "enwl"
    SPEN = "spen"


class AvailableFile(BaseModel):
    filename: str
    url: HttpUrl

    def __hash__(self):
        return hash((self.filename, self.url))

    def __eq__(self, other):
        return self.filename == other.filename and self.url == other.url

    def __lt__(self, other):
        return self.filename < other.filename
