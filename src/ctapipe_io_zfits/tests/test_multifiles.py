import shutil

import pytest
from ctapipe.core import Provenance


@pytest.mark.parametrize("all_chunks", [True, False])
def test_multifiles(all_chunks, dummy_tel_file, dl0_base):
    from ctapipe_io_zfits.multifile import MultiFiles

    path = dummy_tel_file

    Provenance().start_activity("test_multifiles")
    with MultiFiles(path, all_chunks=all_chunks) as mf:
        assert mf.n_open_files == 4

        expected_event_id = 0
        for expected_event_id, event in enumerate(mf, start=1):
            assert event.event_id == expected_event_id

        if all_chunks:
            assert expected_event_id == 100
        else:
            assert expected_event_id == 40

    recorded_inputs = Provenance().current_activity.provenance["input"]
    # five chunks per stream
    if all_chunks:
        assert len(recorded_inputs) == 12
    else:
        assert len(recorded_inputs) == 4


def test_acada_rel1_filename_conventions():
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
    assert groups["extra_suffix"] == ""

    path = f"foo/bar/{name}"
    file_info = get_file_info(path, convention="acada_rel1")
    assert file_info.tel_id == 1
    assert file_info.data_source == "SDH_3001"
    assert file_info.sb_id == 2000000008
    assert file_info.obs_id == 2000000016
    assert file_info.chunk == 9
    assert file_info.extra_suffix == ""

    assert acada["template"](file_info) == name


@pytest.mark.parametrize("data_type", ["", "_TEL_SHOWER", "_MUON"])
def test_acada_dpps_icd_filename_conventions(data_type):
    from ctapipe_io_zfits.multifile import filename_conventions, get_file_info

    filename = f"TEL001_SDH001_20230802T021531_SBID0000000000000000123_OBSID0000000000000000456{data_type}_CHUNK000.fits.fz"  # noqa

    acada = filename_conventions["acada_dpps_icd"]

    m = acada["re"].match(filename)
    assert m is not None
    groups = m.groupdict()

    assert groups["tel_id"] == "001"
    assert groups["data_source"] == "SDH001"
    assert groups["timestamp"] == "20230802T021531"
    assert groups["sb_id"] == "0000000000000000123"
    assert groups["obs_id"] == "0000000000000000456"
    assert groups["chunk"] == "000"
    assert groups["extra_suffix"] == ""

    path = f"foo/bar/{filename}"
    file_info = get_file_info(path, convention="acada_dpps_icd")
    assert file_info.tel_id == 1
    assert file_info.data_source == "SDH001"
    assert file_info.sb_id == 123
    assert file_info.obs_id == 456
    assert file_info.chunk == 0
    assert file_info.sb_id_padding == 19
    assert file_info.obs_id_padding == 19
    assert file_info.chunk_padding == 3
    assert file_info.extra_suffix == ""

    assert acada["template"](file_info) == filename


@pytest.mark.parametrize("data_type", [None, "CALIB"])
def test_acada_dpps_icd_filename_conventions_missing_ids(data_type):
    from ctapipe_io_zfits.multifile import filename_conventions, get_file_info

    data_type_part = "" if data_type is None else f"_{data_type}"
    filename = f"TEL001_SDH001_20230802T021531{data_type_part}_CHUNK000.fits.fz"  # noqa

    acada = filename_conventions["acada_dpps_icd"]

    m = acada["re"].match(filename)
    assert m is not None
    groups = m.groupdict()

    assert groups["tel_id"] == "001"
    assert groups["data_source"] == "SDH001"
    assert groups["timestamp"] == "20230802T021531"
    assert groups["data_type"] == data_type
    assert groups["sb_id"] is None
    assert groups["obs_id"] is None
    assert groups["chunk"] == "000"

    path = f"foo/bar/{filename}"
    file_info = get_file_info(path, convention="acada_dpps_icd")
    assert file_info.tel_id == 1
    assert file_info.data_source == "SDH001"
    assert file_info.timestamp == "20230802T021531"
    assert file_info.sb_id is None
    assert file_info.obs_id is None
    assert file_info.chunk == 0
    assert file_info.sb_id_padding == 0
    assert file_info.obs_id_padding == 0
    assert file_info.chunk_padding == 3


@pytest.mark.parametrize(
    ("convention", "filename"),
    [
        (
            "acada_dpps_icd",
            "TEL001_SDH001_20230802T021531_SBID123_OBSID456_CHUNK000_foo_bar_baz.fits.fz",
        ),
        (
            "acada_rel1",
            "Tel001_SDH_3001_20231003T204445_sbid2000000008_obid2000000016_9_foo_bar_baz.fits.fz",
        ),
    ],
)
def test_extra_suffix(convention, filename):
    from ctapipe_io_zfits.multifile import filename_conventions, get_file_info

    info = get_file_info(filename, convention)
    assert info.extra_suffix == "_foo_bar_baz"
    assert filename_conventions[convention]["template"](info) == filename


@pytest.mark.parametrize("ignore_timestamp", [True, False])
def test_ignore_timestamps(ignore_timestamp, dummy_tel_file, dl0_base, tmp_path):
    from ctapipe_io_zfits.multifile import MultiFiles, get_file_info, get_file_name

    path = dummy_tel_file
    convention = "acada_dpps_icd"
    info = get_file_info(path, convention=convention)
    info.timestamp = "*"
    info.data_source = "*"
    pattern = get_file_name(info, convention=convention)

    # simulate files where parallel streams have slightly different timestamps
    for f in path.parent.glob(pattern):
        info = get_file_info(f, convention=convention)

        # change timestamps of two of the streams
        if info.data_source in {"SDH001", "SDH003"}:
            info.timestamp = info.timestamp[:-1] + "0"

        new_name = get_file_name(info, convention=convention)
        new_path = tmp_path / new_name
        shutil.copy2(f, new_path)

        if info.data_source == "SDH000":
            path = new_path

    Provenance().start_activity("test_ignore_timestamps")
    with MultiFiles(path, ignore_timestamp=ignore_timestamp) as mf:
        if ignore_timestamp:
            assert mf.n_open_files == 4
        else:
            assert mf.n_open_files == 2
