"""
DL0 Protozfits EventSource
"""
from ctapipe.io import EventSource
import logging
from contextlib import ExitStack

from typing import Tuple, Dict

from ctapipe.containers import ObservationBlockContainer, SchedulingBlockContainer
from ctapipe.io import DataLevel
from ctapipe.instrument import SubarrayDescription

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
        self._subarray = None
        self._observation_blocks = {}
        self._scheduling_blocks = {}

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
        pass

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
    
