"""DL0 Protozfits EventSource."""

import logging
from contextlib import ExitStack

import ctapipe
import numpy as np
from ctapipe.containers import (
    CameraCalibrationContainer,
    CameraMonitoringContainer,
    DL0SubarrayContainer,
    DL0TelescopeContainer,
    EventType,
    ObservationBlockContainer,
    PixelStatus,
    SchedulingBlockContainer,
    SubarrayEventContainer,
    SubarrayEventIndexContainer,
    SubarrayTriggerContainer,
    TelescopeEventContainer,
    TelescopeEventIndexContainer,
    TelescopeMonitoringContainer,
)
from ctapipe.core.traits import Bool, Integer
from ctapipe.instrument import SubarrayDescription
from ctapipe.io import DataLevel, EventSource
from ctapipe.io.simteleventsource import GainChannel
from packaging.version import Version
from protozfits import File

from .instrument import build_subarray_description, get_array_elements_by_id
from .multifile import MultiFiles
from .time import cta_high_res_to_time

__all__ = [
    "ProtozfitsDL0EventSource",
    "ProtozfitsDL0TelescopeEventSource",
]

log = logging.getLogger(__name__)

ARRAY_ELEMENTS = get_array_elements_by_id()


CTAPIPE_GE_0_27 = Version(ctapipe.__version__) >= Version("0.27.0a0")
if CTAPIPE_GE_0_27:
    from ctapipe.containers import CameraCalibrationContainer


def _is_compatible(input_url, extname, allowed_protos):
    from astropy.io import fits

    # this allows us to just use try/except around the opening of the fits file,
    stack = ExitStack()

    with stack:
        try:
            hdul = stack.enter_context(fits.open(input_url))
        except Exception as e:
            log.debug("Error trying to open input file as fits: %s", e)
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
        log.debug("Unsupported PBFHEAD: %s not in %s", proto_class, allowed_protos)
        return False

    return True


def _fill_calibration_container(pixel_status, n_channels):
    broken = PixelStatus.get_channel_info(pixel_status) == 0
    mask = np.zeros((n_channels, len(broken)), dtype=bool)
    mask[:, broken] = True
    return CameraCalibrationContainer(outlier_mask=mask)


def _fill_telescope_event(
    zfits_event,
    *,
    data_stream,
    camera_config,
    camera,
    ignore_samples_start=0,
    ignore_samples_end=0,
):
    n_channels = zfits_event.num_channels
    n_pixels_stored = zfits_event.num_pixels_survived
    n_samples = zfits_event.num_samples
    shape = (n_channels, n_pixels_stored, n_samples)
    waveform = zfits_event.waveform.reshape(shape)
    offset = data_stream.waveform_offset
    scale = data_stream.waveform_scale

    zfits_waveform = waveform.astype(np.float32) / scale - offset

    pixel_status_original = zfits_event.pixel_status

    # ACADA does not (yet) set pixels to "stored" when no DVR is applied
    if n_pixels_stored == camera_config.num_pixels and np.all(
        PixelStatus.get_dvr_status(pixel_status_original) == 0
    ):
        pixel_status_original = pixel_status_original | PixelStatus.DVR_1

    pixel_stored = PixelStatus.get_dvr_status(pixel_status_original) != 0
    n_pixels_nominal = camera.geometry.n_pixels

    # fill not readout pixels with 0, reorder pixels
    waveform = np.zeros((n_channels, n_pixels_nominal, n_samples), dtype=np.float32)
    waveform[:, camera_config.pixel_id_map[pixel_stored]] = zfits_waveform

    if ignore_samples_start != 0 or ignore_samples_end != 0:
        start = ignore_samples_start
        end = n_samples - ignore_samples_end
        waveform = waveform[..., start:end]

    # reorder to nominal pixel order
    pixel_status = np.zeros(n_pixels_nominal, dtype=zfits_event.pixel_status.dtype)
    pixel_status[camera_config.pixel_id_map] = pixel_status_original

    channel_info = PixelStatus.get_channel_info(pixel_status)
    if n_channels == 1:
        selected_gain_channel = np.where(
            channel_info == PixelStatus.HIGH_GAIN_STORED,
            GainChannel.HIGH,
            GainChannel.LOW,
        )
    else:
        selected_gain_channel = None

    calibration = _fill_calibration_container(pixel_status, camera.readout.n_channels)

    dl0 = DL0TelescopeContainer(
        pixel_status=pixel_status,
        event_type=EventType(zfits_event.event_type),
        selected_gain_channel=selected_gain_channel,
        event_time=cta_high_res_to_time(
            zfits_event.event_time_s,
            zfits_event.event_time_qns,
        ),
        waveform=waveform,
        first_cell_id=zfits_event.first_cell_id,
    )

    return TelescopeEventContainer(
        index=TelescopeEventIndexContainer(
            obs_id=data_stream.obs_id,
            event_id=zfits_event.event_id,
            tel_id=zfits_event.tel_id,
        ),
        dl0=dl0,
        monitoring=TelescopeMonitoringContainer(
            camera=CameraMonitoringContainer(coefficients=calibration)
        ),
    )


class ProtozfitsDL0EventSource(EventSource):
    """
    DL0 Protozfits EventSource.

    The ``input_url`` must be the subarray trigger file, the source
    will then look for the other data files according to the filename and
    directory schema laid out in the draft of the ACADA - DPPS ICD.
    """

    subarray_id = Integer(default_value=1).tag(config=True)
    warn_missing = Bool(default_value=True).tag(config=True)

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
            # still missing in current ACADA files
            if self._subarray_trigger_stream.subarray_id != 0:
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

        if self.warn_missing:
            self.log_missing = self.log.warning
        else:
            self.log_missing = self.log.debug

        self._open_telescope_files()
        self._tel_event_buffer = {}

    def _get_tel_events_directory(self, tel_id):
        tel_name = ARRAY_ELEMENTS[tel_id]["name"]
        return (
            self._dl0_base
            / tel_name
            / self._acada_user
            / "acada-adh/events"
            / self._date_dirs
        )

    @staticmethod
    def is_compatible(file_path):
        """Return True if the given file can be read by this source."""
        return _is_compatible(
            file_path,
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
                MultiFiles(first_file, parent=self)
            )

    def close(self):
        """Close underlying files."""
        self._exit_stack.__exit__(None, None, None)

    def __exit__(self, exc_type, exc_value, traceback):  # noqa: D105
        self._exit_stack.__exit__(exc_type, exc_value, traceback)

    @property
    def is_simulation(self) -> bool:  # noqa: D102
        return False

    @property
    def datalevels(self) -> tuple[DataLevel]:  # noqa: D102
        return (DataLevel.DL0,)

    @property
    def subarray(self) -> SubarrayDescription:  # noqa: D102
        return self._subarray

    @property
    def observation_blocks(self) -> dict[int, ObservationBlockContainer]:  # noqa: D102
        return self._observation_blocks

    @property
    def scheduling_blocks(self) -> dict[int, SchedulingBlockContainer]:  # noqa: D102
        return self._scheduling_blocks

    def _get_next_tel_event(self, tel_id, event_id):
        tel_event = self._tel_event_buffer.pop((tel_id, event_id), None)
        if tel_event is not None:
            return tel_event

        tel_file = self._telescope_files[tel_id]
        if len(self._tel_event_buffer) < tel_file.n_open_files:
            try:
                tel_event = next(tel_file)
            except StopIteration:
                pass

        if tel_event is None:
            self.log_missing(
                "No telescope data for event_id=%d, tel_id=%d, and no events left in file",
                event_id,
                tel_id,
            )
            return None

        if tel_event.event_id != event_id:
            self._tel_event_buffer[(tel_id, tel_event.event_id)] = tel_event
            self.log_missing(
                "No telescope data for event_id=%d, tel_id=%d, got event_id=%d",
                event_id,
                tel_id,
                tel_event.event_id,
            )
            return None

        return tel_event

    def _generator(self):
        for count, subarray_trigger in enumerate(
            self._subarray_trigger_file.SubarrayEvents
        ):
            subarray_event = SubarrayEventContainer(
                count=count,
                index=SubarrayEventIndexContainer(
                    obs_id=subarray_trigger.obs_id, event_id=subarray_trigger.event_id
                ),
                dl0=DL0SubarrayContainer(
                    trigger=SubarrayTriggerContainer(
                        time=cta_high_res_to_time(
                            subarray_trigger.event_time_s,
                            subarray_trigger.event_time_qns,
                        ),
                        tels_with_trigger=subarray_trigger.tel_ids_with_trigger.tolist(),
                    ),
                ),
            )

            for tel_id in subarray_trigger.tel_ids_with_data:
                tel_file = self._telescope_files[tel_id]
                camera = self.subarray.tel[tel_id].camera

                zfits_tel_event = self._get_next_tel_event(
                    tel_id, subarray_trigger.event_id
                )
                if zfits_tel_event is None:
                    continue

                tel_event = _fill_telescope_event(
                    zfits_tel_event,
                    data_stream=tel_file.data_stream,
                    camera_config=tel_file.camera_config,
                    camera=camera,
                )
                subarray_event.tel[tel_id] = tel_event

            yield subarray_event


class ProtozfitsDL0TelescopeEventSource(EventSource):
    """
    DL0 Protozfits Telescope EventSource.

    The ``input_url`` is one of the telescope events files.
    """

    subarray_id = Integer(default_value=1).tag(config=True)
    ignore_samples_start = Integer(default_value=0).tag(config=True)
    ignore_samples_end = Integer(default_value=0).tag(config=True)

    @classmethod
    def is_compatible(cls, input_url):  # noqa: D102
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

        self._multi_file = self._exit_stack.enter_context(
            MultiFiles(self.input_url, parent=self)
        )
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

    def close(self):  # noqa: D102
        self._exit_stack.__exit__(None, None, None)

    def __exit__(self, exc_type, exc_value, traceback):  # noqa: D105
        self._exit_stack.__exit__(exc_type, exc_value, traceback)

    @property
    def is_simulation(self) -> bool:  # noqa: D102
        return False

    @property
    def datalevels(self) -> tuple[DataLevel]:  # noqa: D102
        return (DataLevel.DL0,)

    @property
    def subarray(self) -> SubarrayDescription:  # noqa: D102
        return self._subarray

    @property
    def observation_blocks(self) -> dict[int, ObservationBlockContainer]:  # noqa: D102
        return self._observation_blocks

    @property
    def scheduling_blocks(self) -> dict[int, SchedulingBlockContainer]:  # noqa: D102
        return self._scheduling_blocks

    def _fill_event(self, count, zfits_event) -> SubarrayEventContainer:
        tel_id = self.tel_id
        camera = self.subarray.tel[tel_id].camera

        # until ctapipe allows telescope event sources
        # we have to fill an arrayevent with just one telescope here
        time = cta_high_res_to_time(
            zfits_event.event_time_s, zfits_event.event_time_qns
        )
        subarray_event = SubarrayEventContainer(
            count=count,
            index=SubarrayEventIndexContainer(
                obs_id=self.obs_id,
                event_id=zfits_event.event_id,
            ),
            dl0=DL0SubarrayContainer(
                trigger=SubarrayTriggerContainer(
                    tels_with_trigger=[self.tel_id],
                    event_type=EventType(int(zfits_event.event_type)),
                    time=time,
                ),
            ),
        )
        subarray_event.tel[tel_id] = _fill_telescope_event(
            zfits_event,
            data_stream=self._multi_file.data_stream,
            camera_config=self._multi_file.camera_config,
            camera=camera,
            ignore_samples_start=self.ignore_samples_start,
            ignore_samples_end=self.ignore_samples_end,
        )
        return subarray_event

    def _generator(self):
        for count, zfits_event in enumerate(self._multi_file):
            yield self._fill_event(count, zfits_event)
