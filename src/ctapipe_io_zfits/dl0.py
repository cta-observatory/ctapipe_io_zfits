"""
DL0 Protozfits EventSource
"""
import logging
from contextlib import ExitStack
from pathlib import Path
from typing import Dict, Tuple

from ctapipe.containers import (
    ArrayEventContainer,
    DL0CameraContainer,
    EventIndexContainer,
    EventType,
    ObservationBlockContainer,
    SchedulingBlockContainer,
    TriggerContainer,
)
from ctapipe.instrument import SubarrayDescription
from ctapipe.io import DataLevel, EventSource
from protozfits import File

from .instrument import build_subarray_description, get_array_elements_by_id
from .multifile import MultiFiles
from .time import cta_high_res_to_time

__all__ = [
    "ProtozfitsDL0EventSource",
]

log = logging.getLogger(__name__)

ARRAY_ELEMENTS = get_array_elements_by_id()


class ProtozfitsDL0EventSource(EventSource):
    """
    DL0 Protozfits EventSource.

    The ``input_url`` must be the subarray trigger file, the source
    will then look for the other data files according to the filename and
    directory schema layed out in the draft of the ACADA - DPPS ICD.
    """

    def __init__(self, input_url, **kwargs):
        super().__init__(input_url=input_url, **kwargs)
        # we will open a lot of files, this helps keeping it clean
        self._exit_stack = ExitStack()
        self._subarray_trigger_file = self._exit_stack.enter_context(
            File(str(input_url))
        )
        self._subarray_trigger_stream = self._subarray_trigger_file.DataStream[0]

        self._subarray = build_subarray_description(
            self._subarray_trigger_stream.subarray_id
        )

        self.obs_id = self._subarray_trigger_stream.obs_id
        self.sb_id = self._subarray_trigger_stream.sb_id

        self._observation_blocks = {
            self.obs_id: ObservationBlockContainer(obs_id=self.obs_id, sb_id=self.sb_id)
        }
        self._scheduling_blocks = {
            self.sb_id: SchedulingBlockContainer(sb_id=self.sb_id)
        }

        self._open_telescope_files()

    def _open_telescope_files(self):
        self._telescope_files = {}
        for tel_id in self.subarray.tel:
            name = ARRAY_ELEMENTS[tel_id]["name"]

            # get the directory, where we should look for files
            tel_dir = Path(
                str(self.input_url.parent)
                .replace("triggers", "events")
                .replace("array", name)
            )
            try:
                first_file = sorted(
                    tel_dir.glob(f"*_SBID*{self.sb_id}_OBSID*{self.obs_id}*.fits.fz")
                )[0]
            except IndexError:
                self.log.warning("No events file found for tel_id %d", tel_id)
                continue

            self._telescope_files[tel_id] = self._exit_stack.enter_context(
                MultiFiles(first_file)
            )

    def close(self):
        self._exit_stack.__exit__(None, None, None)

    def __exit__(self, exc_type, exc_value, traceback):
        self._exit_stack.__exit__(exc_type, exc_value, traceback)

    @property
    def is_simulation(self) -> bool:
        return False

    @property
    def datalevels(self) -> Tuple[DataLevel]:
        return (DataLevel.DL0,)

    @property
    def subarray(self) -> SubarrayDescription:
        return self._subarray

    @property
    def observation_blocks(self) -> Dict[int, ObservationBlockContainer]:
        return self._observation_blocks

    @property
    def scheduling_blocks(self) -> Dict[int, SchedulingBlockContainer]:
        return self._scheduling_blocks

    def _generator(self):
        for subarray_trigger in self._subarray_trigger_file.Events:
            array_event = ArrayEventContainer(
                index=EventIndexContainer(
                    obs_id=subarray_trigger.obs_id, event_id=subarray_trigger.event_id
                ),
                trigger=TriggerContainer(
                    time=cta_high_res_to_time(
                        subarray_trigger.event_time_s, subarray_trigger.event_time_qns
                    ),
                    tels_with_trigger=subarray_trigger.tel_ids.tolist(),
                ),
            )

            for tel_id in array_event.trigger.tels_with_trigger:
                tel_event = next(self._telescope_files[tel_id])
                if tel_event.event_id != array_event.index.event_id:
                    raise ValueError(
                        f"Telescope event for tel_id {tel_id} has different event id!"
                        f" event_id of subarray event: {array_event.index.event_id}"
                        f" event_id of telescope event: {tel_event.event_id}"
                    )

                array_event.dl0.tel[tel_id] = DL0CameraContainer(
                    pixel_status=tel_event.pixel_status,
                    event_type=EventType(tel_event.event_type),
                    event_time=cta_high_res_to_time(
                        tel_event.event_time_s,
                        tel_event.event_time_qns,
                    ),
                    waveform=tel_event.waveform,
                    first_cell_id=tel_event.first_cell_id,
                    # module_hires_local_clock_counter=tel_event.module_hires_local_clock_counter,
                )

            yield array_event

    @classmethod
    def is_compatible(cls, input_url):
        from astropy.io import fits

        # this allows us to just use try/except around the opening of the fits file,
        stack = ExitStack()

        with stack:
            try:
                hdul = stack.enter_context(fits.open(input_url))
            except Exception as e:
                log.debug(f"Error trying to open input file as fits: {e}")
                return False

            if "DataStream" not in hdul:
                log.debug(
                    "FITS file does not contain a DataStream HDU, returning False"
                )
                return False

            if "Events" not in hdul:
                log.debug("FITS file does not contain an Events HDU, returning False")
                return False

            header = hdul["Events"].header

        if header["XTENSION"] != "BINTABLE":
            log.debug("Events HDU is not a bintable")
            return False

        if not header.get("ZTABLE", False):
            log.debug("ZTABLE is not in header or False")
            return False

        if header.get("ORIGIN", "") != "CTA":
            log.debug("ORIGIN != CTA")
            return False

        proto_class = header.get("PBFHEAD")
        if proto_class is None:
            log.debug("Missing PBFHEAD key")
            return False

        if proto_class != "DL0v1.Subarray.Event":
            log.debug(f"Unsupported PBDHEAD: {proto_class}")
            return False

        return True
