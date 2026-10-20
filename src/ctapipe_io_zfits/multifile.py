"""Load multiple stream files in parallel and iterate over events in order."""

import re
from copy import copy
from dataclasses import asdict, dataclass, field
from pathlib import Path
from queue import Empty, PriorityQueue
from typing import Any

from ctapipe.core import Component, Provenance
from ctapipe.core.traits import Bool, Unicode
from protozfits import File
from traitlets import TraitError, validate

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
    obs_id: int | None = None
    sb_id: int | None = None
    local_run_id: int | str | None = None
    chunk: int | str | None = None
    timestamp: str | None = None
    data_type: str | None = None
    sb_id_padding: int = 0
    obs_id_padding: int = 0
    local_run_id_padding: int = 0
    chunk_padding: int = 0
    extra_suffix: str = ""


def acada_rel1_filename(info):
    template = "Tel{tel_id:03d}_{data_source}_{timestamp}_sbid{sb_id:0{sb_id_padding}d}_obid{obs_id:0{obs_id_padding}d}_{chunk:0{chunk_padding}}{extra_suffix}.fits.fz"  # noqa
    return template.format(**asdict(info))


def acada_dpps_icd_filename(info):
    name = f"TEL{info.tel_id:03d}_{info.data_source}"

    if info.timestamp is not None:
        name += f"_{info.timestamp}"

    if info.sb_id is not None:
        name += f"_SBID{info.sb_id:0{info.sb_id_padding}d}"

    if info.obs_id is not None:
        name += f"_OBSID{info.obs_id:0{info.obs_id_padding}d}"

    if info.local_run_id is not None:
        name += f"_RUNID{info.local_run_id:0{info.local_run_id_padding}}"

    if info.data_type is not None:
        name += f"_{info.data_type}"

    name += f"_CHUNK{info.chunk:0{info.chunk_padding}}{info.extra_suffix}.fits.fz"
    return name


def lst_filename(file_info: FileInfo):
    chunk = "*" if file_info.chunk == "*" else f"{file_info.chunk:04d}"
    return (
        f"LST-{file_info.tel_id:d}.{file_info.data_source}"
        f".Run{file_info.obs_id}.{chunk}"
        f"{file_info.extra_suffix}.fits.fz"
    )


ACADA_DPPS_ICD_FILENAME_RE = re.compile(
    r"TEL(?P<tel_id>\d+)_(?P<data_source>SDH\d+)(?:_(?P<timestamp>\d{8}T\d{6}))"
    r"(?:_SBID(?P<sb_id>\d+))?(?:_OBSID(?P<obs_id>\d+))?(?:_RUNID(?P<local_run_id>\d+))?"
    r"(:?_(?P<data_type>[a-zA-Z0-9_]+))?_CHUNK(?P<chunk>\d+)"
    r"(?P<extra_suffix>.*)\.fits\.fz$"
)


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
        "re": re.compile(ACADA_DPPS_ICD_FILENAME_RE),
        "template": acada_dpps_icd_filename,
    },
    "lst": {
        "re": re.compile(
            r"LST-(?P<tel_id>\d+)\.(?P<data_source>\d+)\.Run(?P<obs_id>\d+)\.(?P<chunk>\d+)(?P<extra_suffix>.*)\.fits\.fz",
        ),
        "template": lst_filename,
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
    obs_id = optional_int(groups["obs_id"])
    obs_id_padding = len(groups["obs_id"]) if groups["obs_id"] is not None else 0

    chunk = int(groups["chunk"])
    chunk_padding = len(groups["chunk"])

    sb_id = optional_int(groups.get("sb_id"))
    sb_id_padding = len(groups["sb_id"]) if sb_id is not None else 0

    local_run_id = optional_int(groups.get("local_run_id"))
    local_run_id_padding = (
        len(groups["local_run_id"]) if local_run_id is not None else 0
    )

    data_type = groups.get("data_type")
    if data_type == "":
        data_type = None

    return FileInfo(
        tel_id=int(groups["tel_id"]),
        data_source=groups["data_source"],
        timestamp=groups.get("timestamp"),
        sb_id=sb_id,
        obs_id=obs_id,
        local_run_id=local_run_id,
        chunk=chunk,
        data_type=data_type,
        sb_id_padding=sb_id_padding,
        obs_id_padding=obs_id_padding,
        local_run_id_padding=local_run_id_padding,
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
        default_value=False,
        help="If true, open subsequent chunks when current one is exhausted",
    ).tag(config=True)

    all_runs = Bool(
        default_value=False,
        help=(
            "If true, open the lowest chunk of the next available local run "
            "with the same obs_id when the current run is exhausted. "
            "Use all_chunks to also read subsequent chunks within each run."
        ),
    ).tag(config=True)

    filename_convention = Unicode(
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

        Notes
        -----
        Available files are indexed at initialization. Files added later are
        not included in iteration.
        """
        super().__init__(*args, **kwargs)

        self.path = Path(path)

        if not self.path.is_file():
            raise OSError(f"input path {path} is not a file")

        file_info = get_file_info(path, convention=self.filename_convention)
        self.directory = self.path.parent
        convention = filename_conventions[self.filename_convention]
        self.filename_template = convention["template"]

        self._discover_files(file_info)

        self._current_chunk = {
            data_source: file_info.chunk - 1 for data_source in self.data_sources
        }
        self._current_run = {
            data_source: file_info.local_run_id for data_source in self.data_sources
        }
        self._open_files = {}

        self._first_file_info = file_info
        self._events = PriorityQueue()
        self._events_tables = {}
        self._events_headers = {}
        self.camera_config = None
        self.data_stream = None
        self._exhausted_sources = set()

        for data_source in self.data_sources:
            self._load_next_chunk(data_source)

    def _discover_files(self, file_info):
        """Discover matching files and index them by data source, run, and chunk."""
        pattern_info = copy(file_info)
        pattern_info.data_source = "*"
        if self.all_chunks or self.all_runs:
            pattern_info.chunk = "*"
            pattern_info.chunk_padding = 0
        if self.all_runs and file_info.local_run_id is not None:
            pattern_info.local_run_id = "*"
            pattern_info.local_run_id_padding = 0
            pattern_info.timestamp = "*"
        elif self.ignore_timestamp:
            pattern_info.timestamp = "*"
        pattern = self.filename_template(pattern_info)

        self.log.debug("Looking for files using pattern: %s", pattern)
        self._files = {}
        file_infos = {}
        self.data_sources = set()
        for candidate in self.directory.glob(pattern):
            try:
                info = (
                    file_info
                    if candidate == self.path
                    else get_file_info(candidate, convention=self.filename_convention)
                )
            except ValueError:
                continue
            if any(
                getattr(info, key) != getattr(file_info, key)
                for key in ("tel_id", "sb_id", "obs_id", "data_type", "extra_suffix")
            ):
                continue
            if file_info.local_run_id is None:
                if info.local_run_id is not None:
                    continue
            elif (
                info.local_run_id is None or info.local_run_id < file_info.local_run_id
            ):
                continue

            runs = self._files.setdefault(info.data_source, {})
            chunks = runs.setdefault(info.local_run_id, {})
            chunks.setdefault(info.chunk, []).append(candidate)
            file_infos[candidate] = info
            if (
                info.local_run_id == file_info.local_run_id
                and info.chunk == file_info.chunk
                and (self.ignore_timestamp or info.timestamp == file_info.timestamp)
            ):
                self.data_sources.add(info.data_source)

        if not self.data_sources:
            raise ValueError(f"Did not find any files matching pattern: {pattern}")
        self.log.debug("Found the following data sources: %s", self.data_sources)

        for source in self.data_sources:
            for run, chunks in self._files[source].items():
                if not self.ignore_timestamp:
                    # Later runs choose their own timestamp from the lowest chunk.
                    timestamp = (
                        file_info.timestamp
                        if run == file_info.local_run_id
                        else file_infos[max(chunks[min(chunks)])].timestamp
                    )
                    chunks = {
                        chunk: [
                            path
                            for path in paths
                            if file_infos[path].timestamp == timestamp
                        ]
                        for chunk, paths in chunks.items()
                    }

                # Duplicate chunks can contain the previous observation's last
                # events, so prefer the last timestamp for each chunk.
                self._files[source][run] = {
                    chunk: max(paths) for chunk, paths in chunks.items() if paths
                }

        self._next_runs = {
            source: iter(
                sorted(
                    run
                    for run in self._files[source]
                    if file_info.local_run_id is not None
                    and run is not None
                    and run > file_info.local_run_id
                )
            )
            for source in self.data_sources
        }

    @validate("filename_convention")
    def _valid_filename_convention(self, proposal):
        value = proposal["value"]
        known = set(filename_conventions.keys())
        if value not in known:
            msg = f"filename_convention {value} not in known conventions: {known}"
            raise TraitError(msg)
        return value

    @property
    def event_proto(self):
        """Protobuf message type of the telescope events."""
        return next(iter(self._events_headers.values()))["PBFHEAD"]

    @property
    def exhausted(self):
        """Whether all data sources / chunks available have been consumed."""
        return len(self._exhausted_sources) == len(self.data_sources)

    @property
    def n_open_files(self):
        """Number of currently open files."""
        return len(self._open_files)

    def _load_next_chunk(self, data_source):
        """Open the next (or first) chunk.

        Parameters
        ----------
        data_source : str or None
            If None, assume the single-file case and just open it.
        """
        if data_source in self._open_files:
            self._open_files.pop(data_source).close()

        self._current_chunk[data_source] += 1
        chunk = self._current_chunk[data_source]

        path = self._files[data_source][self._current_run[data_source]].get(chunk)
        if path is None:
            raise FileNotFoundError(
                f"No chunk {chunk} found for {data_source} "
                f"in local run {self._current_run[data_source]}"
            )

        self._open_file(path, data_source)

    def _open_next_run(self, data_source):
        """Open the lowest chunk of the next local run for this data source."""
        run = next(self._next_runs[data_source], None)
        if run is None:
            raise FileNotFoundError(
                f"No next run found for {data_source} "
                f"with obs_id {self._first_file_info.obs_id}"
            )

        chunks = self._files[data_source][run]
        chunk = min(chunks)
        path = chunks[chunk]
        self._current_run[data_source] = run
        self._current_chunk[data_source] = chunk
        self._open_file(path, data_source)

    def _open_file(self, path, data_source):
        """Open a file and enqueue its first event."""
        if data_source in self._open_files:
            self._open_files.pop(data_source).close()

        self.log.info("Opening file %s", path)
        file_ = File(str(path), pure_protobuf=self.pure_protobuf)
        self._open_files[data_source] = file_

        events_table = file_.Events
        self._events_tables[data_source] = events_table
        self._events_headers[data_source] = events_table.header
        datalevel = events_table.header["PBFHEAD"].split("v", 1)[0]
        Provenance().add_input_file(str(path), datalevel)

        # load first event from each stream
        event = next(events_table)
        self._events.put_nowait(NextEvent(event.event_id, event, data_source))

        if self.data_stream is None:
            self.data_stream = file_.DataStream[0]

        if self.camera_config is None:
            self.camera_config = file_.CameraConfiguration[0]

    def _advance_source(self, data_source):
        """Try the next chunk, then the next run, before exhausting a source."""
        if self.all_chunks:
            try:
                self._load_next_chunk(data_source)
                return
            except FileNotFoundError:
                pass

        if self.all_runs:
            try:
                self._open_next_run(data_source)
                return
            except FileNotFoundError:
                pass

        self._exhausted_sources.add(data_source)

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
            self._advance_source(data_source)

        return event
