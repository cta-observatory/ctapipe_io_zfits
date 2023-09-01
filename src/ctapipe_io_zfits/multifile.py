from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from queue import Empty, PriorityQueue
from typing import Any

from ctapipe.core import Component, Provenance
from ctapipe.core.traits import Bool
from protozfits import File
from traitlets import CRegExp

__all__ = ["MultiFiles"]


@dataclass(order=True)
class NextEvent:
    """Class to get sorted access to events from multiple files"""

    priority: int
    event: Any = field(compare=False)
    data_source: str = field(compare=False)


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
        default_value=True,
        help="If true, open subsequent chunks when current one is exhausted",
    ).tag(config=True)

    data_source_re = CRegExp(r"SDH\d+").tag(config=True)
    chunk_re = CRegExp(r"CHUNK\d+").tag(config=True)

    def __init__(self, path, *args, **kwargs):
        """
        Create a new MultiFiles object from an iterable of paths

        Parameters
        ----------
        paths: Iterable[string|Path]
            The input paths
        """
        super().__init__(*args, **kwargs)

        self.path = Path(path)

        if not self.path.is_file():
            raise IOError(f"input path {path} is not a file")

        self.directory = self.path.parent

        # glob for files and group by data_source
        pattern = self.chunk_re.sub(self.data_source_re.sub(self.path.name, "*"), "*")
        paths = self.directory.glob(pattern)

        self._files = defaultdict(list)
        for p in paths:
            data_source = self.data_source_re.search(p.name).group(0)
            self._files[data_source].append(p)

        # for now we assume we got the first chunk
        # and go through the chunks in lexicographic order
        self._files = {
            data_source: sorted(paths) for data_source, paths in self._files.items()
        }

        self._open_files = {}
        self._current_chunk = {data_source: -1 for data_source in self._files}

        self._events = PriorityQueue()
        self._events_tables = {}
        self._events_headers = {}
        self.camera_config = None
        self.data_stream = None

        for data_source in self._files:
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

        files = self._files[data_source]
        self._current_chunk[data_source] += 1
        chunk_idx = self._current_chunk[data_source]

        if self._current_chunk[data_source] >= len(files):
            path = files[chunk_idx - 1]
            raise FileNotFoundError(f"No further file after: {path}")

        path = files[chunk_idx]
        Provenance().add_input_file(str(path), "DL0")
        file_ = File(str(path))
        self._open_files[data_source] = file_
        self.log.info("Opened file %s", path)

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
