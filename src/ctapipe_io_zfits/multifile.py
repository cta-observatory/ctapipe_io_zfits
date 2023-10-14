import re
from dataclasses import dataclass, field
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
    """Class to get sorted access to events from multiple files"""

    priority: int
    event: Any = field(compare=False)
    data_source: str = field(compare=False)


@dataclass()
class FileInfo:
    tel_id: int
    data_source: str
    sb_id: int
    obs_id: int
    chunk: int
    sb_id_padding: int = 0
    obs_id_padding: int = 0
    chunk_padding: int = 0


filename_conventions = {
    # Tel001_SDH_3001_20231003T204445_sbid2000000008_obid2000000016_9.fits.fz
    "acada_rel1": {
        "re": re.compile(r"Tel(?P<tel_id>\d+)_(?P<data_source>SDH_\d+)_(?P<timestamp>\d{8}T\d{6})_sbid(?P<sb_id>\d+)_obid(?P<obs_id>\d+)_(?P<chunk>\d+)\.fits\.fz"),
        "template": "Tel{tel_id:03d}_{data_source}_{timestamp}_sbid{sb_id:0{sb_id_padding}d}_obid{obs_id:0{obs_id_padding}d}_{chunk:0{chunk_padding}d}.fits.fz",
    },
    "acada_dpps_icd": {
        # TEL001_SDH0001_20231013T220427_SBID0000000002000000013_OBSID0000000002000000027_CHUNK000.fits.fz
        "re": re.compile(r"TEL(?P<tel_id>\d+)_(?P<data_source>SDH\d+)_(?P<timestamp>\d{8}T\d{6})_SBID(?P<sb_id>\d+)_OBSID(?P<obs_id>\d+)_CHUNK(?P<chunk>\d+)\.fits\.fz"),
        "template": "TEL{tel_id:03d}_{data_source}_{timestamp}_SBID{sb_id:0{sb_id_padding}d}_OBSID{obs_id:0{obs_id_padding}d}_CHUNK{chunk:0{chunk_padding}d}.fits.fz",
    }

}


def get_file_info(path, convention):
    path = Path(path)

    regex = filename_conventions[convention]["re"]
    m = regex.match(path.name)
    if m is None:
        raise ValueError(f"Filename {path.name} did not match convention {convention} with regex {regex}")

    groups = m.groupdict()
    sb_id = int(groups["sb_id"])
    obs_id = int(groups["obs_id"])
    chunk = int(groups["chunk"])

    sb_id_padding = len(groups["sb_id"])
    obs_id_padding = len(groups["obs_id"])
    chunk_padding = len(groups["chunk"])

    return FileInfo(
        tel_id=int(groups["tel_id"]),
        data_source=groups["data_source"],
        sb_id=sb_id,
        obs_id=obs_id,
        chunk=chunk,
        sb_id_padding=sb_id_padding,
        obs_id_padding=obs_id_padding,
        chunk_padding=chunk_padding,
    )


class MultiFiles(Component):
    """Open multiple stream files and iterate over events in order"""

    all_source_ids = Bool(
        default_value=True,
        help=(
            "If true, open all files for different source_ids"
            "(e.g. SDH001, SDH002) in parallel."
        ),
    ).tag(config=True)

    all_chunks = Bool(
        default_value=False,
        help="If true, open subsequent chunks when current one is exhausted",
    ).tag(config=True)

    filename_convention = CaselessStrEnum(
        values=list(filename_conventions.keys()),
        default_value="acada_rel1",
    ).tag(config=True)

    pure_protobuf = Bool(
        default_value=False,
    ).tag(config=True)


    def __init__(self, path, *args, **kwargs):
        """
        Create a new MultiFiles object from a path fullfilling the given filename convention
        """
        super().__init__(*args, **kwargs)

        self.path = Path(path)

        if not self.path.is_file():
            raise IOError(f"input path {path} is not a file")

        file_info = get_file_info(path, convention=self.filename_convention)
        self.directory = self.path.parent
        self.filename_template = filename_conventions[self.filename_convention]["template"]

        # figure out how many data sources we have:
        data_source_pattern = self.filename_template.format(
            tel_id=file_info.tel_id,
            data_source="*",
            timestamp="*",
            sb_id=file_info.sb_id,
            obs_id=file_info.obs_id,
            chunk=file_info.chunk,
            sb_id_padding=file_info.sb_id_padding,
            obs_id_padding=file_info.obs_id_padding,
            chunk_padding=file_info.chunk_padding,
        )

        self.log.debug("Looking for parallel data source using pattern: %s", data_source_pattern)
        paths = sorted(self.directory.glob(data_source_pattern))
        self.log.debug("Found matching paths: %s", paths)
        self.data_sources = [get_file_info(path, convention=self.filename_convention).data_source for path in paths]
        self.log.debug("Found the following data sources: %s", self.data_sources)

        self._current_chunk = {data_source: file_info.chunk - 1 for data_source in self.data_sources}
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

        pattern = self.filename_template.format(
            tel_id=self._first_file_info.tel_id,
            data_source=data_source,
            timestamp="*",
            sb_id=self._first_file_info.sb_id,
            obs_id=self._first_file_info.obs_id,
            chunk=chunk,
            sb_id_padding=self._first_file_info.sb_id_padding,
            obs_id_padding=self._first_file_info.obs_id_padding,
            chunk_padding=self._first_file_info.chunk_padding,
        )
        try:
            path = next(self.directory.glob(pattern))
        except StopIteration:
            raise FileNotFoundError(f"No file found for pattern {self.directory}/{pattern}")

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
        """Close the underlying files"""
        for f in self._open_files.values():
            f.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __iter__(self):
        return self

    def __next__(self):
        # check for the minimal event id
        if not self._events:
            raise StopIteration

        try:
            next_event = self._events.get_nowait()
        except Empty:
            raise StopIteration

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
