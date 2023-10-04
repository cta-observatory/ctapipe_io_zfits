from ctapipe.core import Provenance


def test_multifiles(dummy_dl0, dl0_base):
    from ctapipe_io_zfits.multifile import MultiFiles

    directory = dl0_base / "TEL001/ctao-acada-n/acada-adh/events/2023/08/01"
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



def test_acada_filename_conventions():
    from ctapipe_io_zfits.multifile import filename_conventions, get_file_info
    name = "Tel001_SDH_3001_20231003T204445_sbid2000000008_obid2000000016_9.fits.fz"

    acada = filename_conventions["acada_rel1"]

    m = acada["re"].match(name)
    assert m is not None
    groups = m.groupdict()

    assert groups["tel_id"] == "001"
    assert groups["data_source"] == "SDH_3001"
    assert groups["timestamp"] == "20231003T204445"
    assert groups["sb_id"] == "2000000008"
    assert groups["obs_id"] == "2000000016"
    assert groups["chunk"] == "9"

    path = f"foo/bar/{name}"
    file_info = get_file_info(path, convention="acada_rel1")
    assert file_info.tel_id == 1
    assert file_info.data_source == "SDH_3001"
    assert file_info.sb_id == 2000000008
    assert file_info.obs_id == 2000000016
    assert file_info.chunk == 9
