def test_is_compatible(dummy_dl0):
    from ctapipe_io_zfits import ProtozfitsDL0EventSource

    ProtozfitsDL0EventSource.is_compatible()
