from ctapipe.io import EventSource


def test_is_compatible(dummy_dl0):
    from ctapipe_io_zfits import ProtozfitsDL0EventSource

    assert ProtozfitsDL0EventSource.is_compatible(dummy_dl0)


def test_is_valid_eventsource(dummy_dl0):
    from ctapipe_io_zfits.dl0 import ProtozfitsDL0EventSource

    assert isinstance(EventSource(dummy_dl0), ProtozfitsDL0EventSource)
