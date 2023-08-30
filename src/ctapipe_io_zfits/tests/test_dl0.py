from ctapipe.io import EventSource
from astropy.time import Time
import astropy.units as u


def test_is_compatible(dummy_dl0):
    from ctapipe_io_zfits import ProtozfitsDL0EventSource

    assert ProtozfitsDL0EventSource.is_compatible(dummy_dl0)


def test_is_valid_eventsource(dummy_dl0):
    from ctapipe_io_zfits.dl0 import ProtozfitsDL0EventSource

    assert isinstance(EventSource(dummy_dl0), ProtozfitsDL0EventSource)


def test_subarray_events(dummy_dl0):
    from ctapipe_io_zfits.dl0 import ProtozfitsDL0EventSource

    obs_start = Time("2023-08-02T02:15:31")

    with EventSource(dummy_dl0) as source:
        n_read = 0
        for array_event in source:
            assert array_event.index.obs_id == 456
            assert array_event.index.event_id == n_read + 1
            assert array_event.trigger.time == obs_start + n_read * 0.001 * u.s
            assert array_event.trigger.tels_with_trigger == [1, ]
            n_read += 1

        assert n_read == 100

    assert isinstance(EventSource(dummy_dl0), ProtozfitsDL0EventSource)
