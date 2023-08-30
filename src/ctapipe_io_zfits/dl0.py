"""
DL0 Protozfits EventSource
"""
from ctapipe.io import EventSource
import logging
from contextlib import ExitStack

from typing import Tuple, Dict

from ctapipe.containers import (
    ArrayEventContainer,
    EventIndexContainer,
    ObservationBlockContainer,
    SchedulingBlockContainer,
    TriggerContainer,
)
from ctapipe.io import DataLevel
from ctapipe.instrument import SubarrayDescription

from protozfits import File

from .time import cta_high_res_to_time

__all__ = [
    "ProtozfitsDL0EventSource",
]

log = logging.getLogger(__name__)


class ProtozfitsDL0EventSource(EventSource):
    """
    DL0 Protozfits EventSource.

    The ``input_url`` must be the subarray trigger file, the source
    will then look for the other data files according to the filename and
    directory schema layed out in the draft of the ACADA - DPPS ICD.
    """
    def __init__(self, input_url, **kwargs):
        super().__init__(input_url=input_url, **kwargs)
        self._subarray_trigger_file = File(str(input_url))
        self._subarray_trigger_stream = self._subarray_trigger_file.DataStream[0]

        self._subarray = None

        obs_id = self._subarray_trigger_stream.obs_id
        sb_id = self._subarray_trigger_stream.sb_id

        self._observation_blocks = {
            obs_id: ObservationBlockContainer(obs_is=obs_id, sb_id=sb_id)
        }
        self._scheduling_blocks = {
            sb_id: SchedulingBlockContainer(sb_id=sb_id)
        }

    def close(self):
        self._subarray_trigger_file.close()

    @property
    def is_simulation(self) -> bool:
        return False

    @property
    def datalevels(self) -> Tuple[DataLevel]:
        return (DataLevel.DL0, )

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
                    obs_id=subarray_trigger.obs_id,
                    event_id=subarray_trigger.event_id
                ),
                trigger=TriggerContainer(
                    time=cta_high_res_to_time(
                        subarray_trigger.event_time_s,
                        subarray_trigger.event_time_qns
                    ),
                    tels_with_trigger=subarray_trigger.tel_ids.tolist(),
                )
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
                log.debug("FITS file does not contain a DataStream HDU, returning False")
                return False

            if "Events" not in hdul:
                log.debug("FITS file does not contain an Events HDU, returning False")
                return False

            header = hdul["Events"].header

        if header["XTENSION"] != "BINTABLE":
            log.debug(f"Events HDU is not a bintable")
            return False

        if not header.get("ZTABLE", False):
            log.debug(f"ZTABLE is not in header or False")
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
    
