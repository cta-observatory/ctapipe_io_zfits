"""Functions for dealing with path names."""
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path


@dataclass
class FileNameInfo:
    """Components of a filename."""

    data_source_id: str
    ae_type: str | None = None
    ae_id: int | None = None
    subarray_id: int | None = None
    sb_id: int | None = None
    obs_id: int | None = None
    type_: str | None = None
    subtype: str | None = None
    chunk_id: int | None = None
    file_id: int | None = None
    suffix: str | None = None
    timestamp: datetime | date | None = None


#: regex to match filenames according to the ACADA DPPS ICD naming pattern
FILENAME_RE = re.compile(
    r"(?:(?:SUB(?P<subarray_id>\d+))|(?:(?P<ae_type>TEL|AUX)(?P<ae_id>\d+)))"
    r"(?:_(?P<data_source_id>[A-Z]+\d*))"
    r"(?:_(?P<date>[0-9]{8})(?:T(?P<time>[0-9]{6}))?)"
    r"(?:_SBID(?P<sb_id>\d+))?"
    r"(?:_OBSID(?P<obs_id>\d+))?"
    # need to prevent that FILE001 / CHUNK001 becomes the type
    r"(?:_(?P<type_>(?!FILE)(?!CHUNK)[A-Z0-9]+))?"
    r"(?:_(?P<subtype>SHOWER|CAL|MUON))?"
    r"(?:_CHUNK(?P<chunk_id>\d+))?"
    r"(?:_FILE(?P<file_id>\d+))?"
    r"(?P<suffix>[.].*)?"
)


def parse_filename(path):
    """Extract components from a path."""
    name = Path(path).name

    match = FILENAME_RE.match(name)
    if not match:
        raise ValueError(f"File {name} does not match pattern: {FILENAME_RE}")

    info = match.groupdict()

    for key in (
        "ae_id",
        "subarray_id",
        "sb_id",
        "obs_id",
        "chunk_id",
        "file_id",
    ):
        info[key] = int(info[key]) if info[key] is not None else None

    date = info.pop("date")
    time = info.pop("time")

    if time:
        info["timestamp"] = datetime.strptime(f"{date}T{time}", "%Y%m%dT%H%M%S")
    else:
        info["timestamp"] = datetime.strptime(date, "%Y%m%d").date()

    return FileNameInfo(**info)
