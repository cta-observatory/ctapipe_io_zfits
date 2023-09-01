import astropy.units as u
import numpy as np
from astropy.time import Time
from ctapipe.instrument import SubarrayDescription
from ctapipe.io import EventSource


def test_is_compatible(dummy_dl0):
    from ctapipe_io_zfits import ProtozfitsDL0EventSource

    assert ProtozfitsDL0EventSource.is_compatible(dummy_dl0)


def test_is_valid_eventsource(dummy_dl0):
    from ctapipe_io_zfits.dl0 import ProtozfitsDL0EventSource

    with EventSource(dummy_dl0) as source:
        assert isinstance(source, ProtozfitsDL0EventSource)


def test_subarray(dummy_dl0):
    with EventSource(dummy_dl0) as source:
        assert isinstance(source.subarray, SubarrayDescription)


def test_subarray_events(dummy_dl0):
    time = Time("2023-08-02T02:15:31")

    with EventSource(dummy_dl0) as source:
        n_read = 0
        for array_event in source:
            assert array_event.index.obs_id == 456
            assert array_event.index.event_id == n_read + 1
            dt = np.abs(array_event.trigger.time - time).to(u.ns)
            assert dt < 0.2 * u.ns
            assert array_event.trigger.tels_with_trigger == [
                1,
            ]

            n_read += 1
            time = time + 0.001 * u.s

        assert n_read == 100
