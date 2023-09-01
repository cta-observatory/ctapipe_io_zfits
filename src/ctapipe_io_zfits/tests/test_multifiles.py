from ctapipe.core import Provenance


def test_multifiles(dummy_dl0, dl0_base):
    from ctapipe_io_zfits.multifile import MultiFiles

    directory = dl0_base / "LSTN-01/ctao-acada-n/acada-adh/events/2023/08/01"
    filename = "TEL001_SDH001_20230802T021531_SBID0000000000000000123_OBSID0000000000000000456_TEL_SHOWER_CHUNK000.fits.fz"  # noqa
    path = directory / filename

    Provenance().start_activity("test_multifiles")
    with MultiFiles(path) as mf:
        assert mf.n_open_files == 4

        expected_event_id = 0
        for expected_event_id, event in enumerate(mf, start=1):
            assert event.event_id == expected_event_id

        assert expected_event_id == 100

    recorded_inputs = Provenance().current_activity.provenance["input"]
    # five chunks per stream
    assert len(recorded_inputs) == 12
