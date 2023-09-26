"""
DL0 Protozfits EventSource
"""
import logging
from contextlib import ExitStack
from typing import Dict, Tuple

import numpy as np
from ctapipe.containers import (
    ArrayEventContainer,
    DL0CameraContainer,
    EventIndexContainer,
    EventType,
    ObservationBlockContainer,
    SchedulingBlockContainer,
    TelescopeTriggerContainer,
    TriggerContainer,
)
from ctapipe.core.traits import Integer
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


def _is_compatible(input_url, extname, allowed_protos):
    from astropy.io import fits

    # this allows us to just use try/except around the opening of the fits file,
    stack = ExitStack()

    with stack:
        try:
            hdul = stack.enter_context(fits.open(input_url))
        except Exception as e:
            log.debug(f"Error trying to open input file as fits: {e}")
            return False

        if extname not in hdul:
            log.debug("FITS file does not contain '%s' HDU", extname)
            return False

        header = hdul[extname].header

    if header["XTENSION"] != "BINTABLE":
        log.debug("%s HDU is not a bintable", extname)
        return False

    if not header.get("ZTABLE", False):
        log.debug("ZTABLE is not in header or False")
        return False

    proto_class = header.get("PBFHEAD")
    if proto_class is None:
        log.debug("Missing PBFHEAD key")
        return False

    if proto_class not in allowed_protos:
        log.debug(f"Unsupported PBFHEAD: {proto_class} not in {allowed_protos}")
        return False

    return True


def _fill_dl0_container(tel_event, data_stream):
    n_channels = tel_event.num_channels
    n_pixels = tel_event.num_pixels_survived
    n_samples = tel_event.num_samples
    shape = (n_channels, n_pixels, n_samples)
    # FIXME: ctapipe  can only handle a single gain
    waveform = tel_event.waveform.reshape(shape)[0]
    offset = data_stream.waveform_offset
    scale = data_stream.waveform_scale
    waveform = waveform.astype(np.float32) / scale - offset

    return DL0CameraContainer(
        pixel_status=tel_event.pixel_status,
        event_type=EventType(tel_event.event_type),
        selected_gain_channel=np.zeros(n_pixels, dtype=np.int8),
        event_time=cta_high_res_to_time(
            tel_event.event_time_s,
            tel_event.event_time_qns,
        ),
        waveform=waveform,
        first_cell_id=tel_event.first_cell_id,
        # module_hires_local_clock_counter=tel_event.module_hires_local_clock_counter,
    )


class ProtozfitsDL0EventSource(EventSource):
    """
    DL0 Protozfits EventSource.

    The ``input_url`` must be the subarray trigger file, the source
    will then look for the other data files according to the filename and
    directory schema layed out in the draft of the ACADA - DPPS ICD.
    """

    subarray_id = Integer(default_value=1).tag(config=True)

    def __init__(self, input_url=None, **kwargs):
        if input_url is not None:
            kwargs["input_url"] = input_url

        super().__init__(**kwargs)
        # we will open a lot of files, this helps keeping it clean
        self._exit_stack = ExitStack()
        self._subarray_trigger_file = self._exit_stack.enter_context(
            File(str(self.input_url))
        )
        self._subarray_trigger_stream = None
        if hasattr(self._subarray_trigger_file, "DataStream"):
            self._subarray_trigger_stream = self._subarray_trigger_file.DataStream[0]
            self.sb_id = self._subarray_trigger_stream.sb_id
            self.obs_id = self._subarray_trigger_stream.obs_id
            self.subarray_id = self._subarray_trigger_stream.subarray_id
        else:
            first_event = self._subarray_trigger_file.SubarrayEvents[0]
            self.sb_id = first_event.sb_id
            self.obs_id = first_event.obs_id

        self._subarray = build_subarray_description(self.subarray_id)

        self._observation_blocks = {
            self.obs_id: ObservationBlockContainer(
                obs_id=np.uint64(self.obs_id), sb_id=np.uint64(self.sb_id)
            )
        }
        self._scheduling_blocks = {
            self.sb_id: SchedulingBlockContainer(sb_id=np.uint64(self.sb_id))
        }

        # <prefix>/DL0/<ae-id>/<acada-user>/acada-adh/events/<YYYY>/<MM>/<DD>/
        self._dl0_base = self.input_url.parents[7]
        self._acada_user = self.input_url.parents[5].name
        self._date_dirs = self.input_url.parent.relative_to(self.input_url.parents[3])

        self._open_telescope_files()

    def _get_tel_events_directory(self, tel_id):
        return (
            self._dl0_base
            / f"TEL{tel_id:03d}"
            / self._acada_user
            / "acada-adh/events"
            / self._date_dirs
        )

    @classmethod
    def is_compatible(cls, input_url):
        return _is_compatible(
            input_url,
            extname="SubarrayEvents",
            allowed_protos={"DL0v1.Subarray.Event"},
        )

    def _open_telescope_files(self):
        self._telescope_files = {}
        for tel_id in self.subarray.tel:
            # get the directory, where we should look for files
            tel_dir = self._get_tel_events_directory(tel_id)

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
        for subarray_trigger in self._subarray_trigger_file.SubarrayEvents:
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
                tel_file = self._telescope_files[tel_id]
                tel_event = next(tel_file)
                if tel_event.event_id != array_event.index.event_id:
                    raise ValueError(
                        f"Telescope event for tel_id {tel_id} has different event id!"
                        f" event_id of subarray event: {array_event.index.event_id}"
                        f" event_id of telescope event: {tel_event.event_id}"
                    )

                array_event.dl0.tel[tel_id] = _fill_dl0_container(
                    tel_event,
                    tel_file.data_stream,
                )

            yield array_event


class ProtozfitsDL0TelescopeEventSource(EventSource):
    """
    DL0 Protozfits Telescope EventSource.

    The ``input_url`` is one of the telescope events files.
    """

    subarray_id = Integer(default_value=1).tag(config=True)

    @classmethod
    def is_compatible(cls, input_url):
        return _is_compatible(
            input_url,
            extname="Events",
            allowed_protos={"DL0v1.Telescope.Event"},
        )

    def __init__(self, input_url=None, **kwargs):
        # this enables passing input_url as posarg, kwarg and via the config/parent
        if input_url is not None:
            kwargs["input_url"] = input_url

        super().__init__(**kwargs)

        # we will open a lot of files, this helps keeping it clean
        self._exit_stack = ExitStack()
        self._subarray = build_subarray_description(self.subarray_id)

        self._multi_file = self._exit_stack.enter_context(MultiFiles(self.input_url))
        self.sb_id = self._multi_file.data_stream.sb_id
        self.obs_id = self._multi_file.data_stream.obs_id
        self.tel_id = self._multi_file.data_stream.tel_id

        self._observation_blocks = {
            self.obs_id: ObservationBlockContainer(
                obs_id=np.uint64(self.obs_id), sb_id=np.uint64(self.sb_id)
            )
        }
        self._scheduling_blocks = {
            self.sb_id: SchedulingBlockContainer(sb_id=np.uint64(self.sb_id))
        }

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

    def _fill_event(self, zfits_event) -> ArrayEventContainer:
        tel_id = self.tel_id
        # until ctapipe allows telescope event sources
        # we have to fill an arrayevent with just one telescope here
        time = cta_high_res_to_time(
            zfits_event.event_time_s, zfits_event.event_time_qns
        )
        array_event = ArrayEventContainer(
            index=EventIndexContainer(
                obs_id=self.obs_id,
                event_id=zfits_event.event_id,
            ),
            trigger=TriggerContainer(
                tels_with_trigger=[self.tel_id],
                time=time,
            ),
        )
        array_event.trigger.tel[tel_id] = TelescopeTriggerContainer(time=time)
        array_event.dl0.tel[tel_id] = _fill_dl0_container(
            zfits_event,
            self._multi_file.data_stream,
        )
        return array_event

    def _generator(self):
        for event in self._multi_file:
            yield self._fill_event(event)
