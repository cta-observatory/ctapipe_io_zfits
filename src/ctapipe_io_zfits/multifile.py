"""Load multiple stream files in parallel and iterate over events in order."""

import re
from copy import copy
from dataclasses import asdict, dataclass, field
from pathlib import Path
from queue import Empty, PriorityQueue
from typing import Any

from ctapipe.core import Component, Provenance
from ctapipe.core.traits import Bool, CaselessStrEnum
from protozfits import File

__all__ = [
    "MultiFiles",
]


@dataclass(order=True)
class NextEvent:
    """Class to get sorted access to events from multiple files."""

    priority: int
    event: Any = field(compare=False)
    data_source: str = field(compare=False)


@dataclass()
class FileInfo:
    tel_id: int
    data_source: str
    timestamp: str
    sb_id: int
    obs_id: int
    chunk: int
    data_type: str = ""
    sb_id_padding: int = 0
    obs_id_padding: int = 0
    chunk_padding: int = 0
    extra_suffix: str = ""


def acada_rel1_filename(info):
    template = "Tel{tel_id:03d}_{data_source}_{timestamp}_sbid{sb_id:0{sb_id_padding}d}_obid{obs_id:0{obs_id_padding}d}_{chunk:0{chunk_padding}d}{extra_suffix}.fits.fz"  # noqa
    return template.format(**asdict(info))


def acada_dpps_icd_filename(info):
    name = f"TEL{info.tel_id:03d}_{info.data_source}_{info.timestamp}"
    if info.sb_id is not None:
        name += f"_SBID{info.sb_id:0{info.sb_id_padding}d}"
    if info.obs_id is not None:
        name += f"_OBSID{info.obs_id:0{info.obs_id_padding}d}"

    if info.data_type is not None:
        name += f"_{info.data_type}"

    name += f"_CHUNK{info.chunk:0{info.chunk_padding}d}{info.extra_suffix}.fits.fz"
    return name


filename_conventions = {
    # Tel001_SDH_3001_20231003T204445_sbid2000000008_obid2000000016_9.fits.fz
    "acada_rel1": {
        "re": re.compile(
            r"Tel(?P<tel_id>\d+)_(?P<data_source>SDH_\d+)_(?P<timestamp>\d{8}T\d{6})_sbid(?P<sb_id>\d+)_obid(?P<obs_id>\d+)_(?P<chunk>\d+)(?P<extra_suffix>.*)\.fits\.fz$"  # noqa
        ),
        "template": acada_rel1_filename,
    },
    "acada_dpps_icd": {
        # TEL001_SDH0001_20231013T220427_SBID0000000002000000013_OBSID0000000002000000027_CHUNK000.fits.fz
        "re": re.compile(
            r"TEL(?P<tel_id>\d+)_(?P<data_source>SDH\d+)_(?P<timestamp>\d{8}T\d{6})(?:_SBID(?P<sb_id>\d+))?(?:_OBSID(?P<obs_id>\d+))?(:?_(?P<data_type>[a-zA-Z0-9_]+))?_CHUNK(?P<chunk>\d+)(?P<extra_suffix>.*)\.fits\.fz$"  # noqa
        ),
        "template": acada_dpps_icd_filename,
    },
}


def optional_int(val):
    if val is None:
        return val
    return int(val)


def get_file_info(path, convention):
    path = Path(path)

    regex = filename_conventions[convention]["re"]
    m = regex.match(path.name)
    if m is None:
        raise ValueError(
            f"Filename {path.name} did not match convention"
            f" {convention} with regex {regex}"
        )

    groups = m.groupdict()
    sb_id = optional_int(groups["sb_id"])
    obs_id = optional_int(groups["obs_id"])
    chunk = int(groups["chunk"])

    sb_id_padding = len(groups["sb_id"]) if groups["sb_id"] is not None else 0
    obs_id_padding = len(groups["obs_id"]) if groups["obs_id"] is not None else 0
    chunk_padding = len(groups["chunk"])

    return FileInfo(
        tel_id=int(groups["tel_id"]),
        data_source=groups["data_source"],
        timestamp=groups["timestamp"],
        sb_id=sb_id,
        obs_id=obs_id,
        chunk=chunk,
        data_type=groups.get("data_type"),
        sb_id_padding=sb_id_padding,
        obs_id_padding=obs_id_padding,
        chunk_padding=chunk_padding,
        extra_suffix=groups["extra_suffix"],
    )


def get_file_name(info, convention):
    return filename_conventions[convention]["template"](info)


class MultiFiles(Component):
    """Open data sources in parallel and iterate over events in order."""

    all_source_ids = Bool(
        default_value=True,
        help=(
            "If true, open all files for different source_ids"
            "(e.g. SDH001, SDH002) in parallel."
        ),
    ).tag(config=True)

    all_chunks = Bool(
        default_value=True,
        help="If true, open subsequent chunks when current one is exhausted",
    ).tag(config=True)

    filename_convention = CaselessStrEnum(
        values=list(filename_conventions.keys()),
        default_value="acada_dpps_icd",
    ).tag(config=True)

    ignore_timestamp = Bool(
        default_value=True,
        help="If True, do not require parallel streams to have identical timestamps",
    ).tag(config=True)

    pure_protobuf = Bool(
        default_value=False,
    ).tag(config=True)

    def __init__(self, path, *args, **kwargs):
        """
        Load multiple data sources in parallel, yielding events in order.

        Parameters
        ----------
        path : str or pathlib.Path
            Path to the first chunk for one of the data sources.
            Path must match the given ``filename_convention``.
            Data sources of the same sb_id / obs_id will be opened in parallel
        """
        super().__init__(*args, **kwargs)

        self.path = Path(path)

        if not self.path.is_file():
            raise OSError(f"input path {path} is not a file")

        file_info = get_file_info(path, convention=self.filename_convention)
        self.directory = self.path.parent
        convention = filename_conventions[self.filename_convention]
        self.filename_template = convention["template"]

        # figure out how many data sources we have:
        pattern_info = copy(file_info)
        pattern_info.data_source = "*"
        if self.ignore_timestamp:
            pattern_info.timestamp = "*"
        data_source_pattern = self.filename_template(pattern_info)

        self.log.debug(
            "Looking for parallel data source using pattern: %s", data_source_pattern
        )
        paths = sorted(self.directory.glob(data_source_pattern))
        if len(paths) == 0:
            raise ValueError(
                f"Did not find any files matching pattern: {data_source_pattern}"
            )
        self.log.debug("Found %d matching paths: %s", len(paths), paths)
        self.data_sources = {
            get_file_info(path, convention=self.filename_convention).data_source
            for path in paths
        }
        self.log.debug("Found the following data sources: %s", self.data_sources)

        self._current_chunk = {
            data_source: file_info.chunk - 1 for data_source in self.data_sources
        }
        self._open_files = {}

        self._first_file_info = file_info
        self._events = PriorityQueue()
        self._events_tables = {}
        self._events_headers = {}
        self.camera_config = None
        self.data_stream = None

        for data_source in self.data_sources:
            self._load_next_chunk(data_source)

    @property
    def n_open_files(self):
        """Number of currently open files."""
        return len(self._open_files)

    def _load_next_chunk(self, data_source):
        """Open the next (or first) subrun.

        Parameters
        ----------
        stream : int or None
            If None, assume the single-file case and just open it.
        """
        if data_source in self._open_files:
            self._open_files.pop(data_source).close()

        self._current_chunk[data_source] += 1
        chunk = self._current_chunk[data_source]

        next_info = copy(self._first_file_info)
        next_info.data_source = data_source
        next_info.chunk = chunk
        if self.ignore_timestamp:
            next_info.timestamp = "*"
        pattern = self.filename_template(next_info)

        try:
            # currently there is a timing issue between acada / EVB resulting
            # in two files with chunk000, the first file technically has the last
            # events of the previous ob, so we sort and take the last entry
            path = sorted(self.directory.glob(pattern))[-1]
        except IndexError:
            raise FileNotFoundError(
                f"No file found for pattern {self.directory}/{pattern}"
            ) from None

        Provenance().add_input_file(str(path), "DL0")
        self.log.info("Opening file %s", path)
        file_ = File(str(path), pure_protobuf=self.pure_protobuf)
        self._open_files[data_source] = file_

        events_table = file_.Events
        self._events_tables[data_source] = events_table
        self._events_headers[data_source] = events_table.header

        # load first event from each stream
        event = next(events_table)
        self._events.put_nowait(NextEvent(event.event_id, event, data_source))

        if self.data_stream is None:
            self.data_stream = file_.DataStream[0]

        if self.camera_config is None:
            self.camera_config = file_.CameraConfiguration[0]

    def close(self):
        """Close the underlying files."""
        for f in self._open_files.values():
            f.close()

    def __enter__(self):  # noqa: D105
        return self

    def __exit__(self, exc_type, exc_value, traceback):  # noqa: D105
        self.close()

    def __iter__(self):  # noqa: D105
        return self

    def __next__(self):  # noqa: D105
        # check for the minimal event id
        if not self._events:
            raise StopIteration

        try:
            next_event = self._events.get_nowait()
        except Empty:
            raise StopIteration from None

        data_source = next_event.data_source
        event = next_event.event

        try:
            new = next(self._events_tables[data_source])
            self._events.put_nowait(NextEvent(new.event_id, new, data_source))
        except StopIteration:
            if self.all_chunks:
                try:
                    self._load_next_chunk(data_source)
                except FileNotFoundError:
                    pass

        return event
