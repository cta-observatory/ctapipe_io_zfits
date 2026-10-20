import shutil
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from ctapipe.core import Provenance

from ctapipe_io_zfits.multifile import FileInfo


@pytest.fixture
def run_files(tmp_path, monkeypatch):
    """Create filenames on disk with lightweight, mocked zfits contents."""
    from ctapipe_io_zfits.multifile import get_file_name

    first_info = FileInfo(
        tel_id=1,
        data_source="SDH001",
        timestamp="20261019T200000",
        sb_id=123,
        obs_id=456,
        local_run_id=7,
        chunk=0,
    )
    files = {}

    def add_file(event_ids, convention="acada_dpps_icd", **fields):
        info = replace(first_info, **fields)
        path = tmp_path / get_file_name(info, convention)
        path.touch()
        file_ = MagicMock()
        file_.Events.header = {"PBFHEAD": "DL0v1"}
        file_.Events.__next__.side_effect = iter(
            SimpleNamespace(event_id=event_id) for event_id in event_ids
        )
        files[str(path)] = file_
        return path

    monkeypatch.setattr(
        "ctapipe_io_zfits.multifile.File",
        lambda path, **kwargs: files[path],
    )
    monkeypatch.setattr(Provenance, "add_input_file", MagicMock())
    return add_file, files


def test_files_indexed_once(run_files, monkeypatch):
    from ctapipe_io_zfits import multifile

    add_file, files = run_files
    for source, offset in (("SDH001", 0), ("SDH002", 1)):
        path = add_file([1 + offset], data_source=source)
        add_file([3 + offset], data_source=source, chunk=1)
        add_file(
            [5 + offset],
            data_source=source,
            local_run_id=8,
            chunk=4,
            timestamp="20261019T210000",
        )
        add_file(
            [7 + offset],
            data_source=source,
            local_run_id=8,
            chunk=5,
            timestamp="20261019T210000",
        )

    glob = type(path).glob
    patterns = []

    def record_glob(directory, pattern):
        patterns.append(pattern)
        return glob(directory, pattern)

    parse = MagicMock(wraps=multifile.get_file_info)
    monkeypatch.setattr(type(path), "glob", record_glob)
    monkeypatch.setattr(multifile, "get_file_info", parse)
    with multifile.MultiFiles(path, all_chunks=True, all_runs=True) as mf:
        assert len(patterns) == 1
        parsed_paths = [str(call.args[0]) for call in parse.call_args_list]
        assert sorted(parsed_paths) == sorted(files)

        def unexpected_discovery(*args, **kwargs):
            pytest.fail("File discovery must only happen at initialization")

        monkeypatch.setattr(type(path), "glob", unexpected_discovery)
        monkeypatch.setattr(multifile, "get_file_info", unexpected_discovery)
        assert [event.event_id for event in mf] == list(range(1, 9))
        assert mf.exhausted


@pytest.mark.parametrize(
    ("convention", "data_source"), [("acada_rel1", "SDH_001"), ("lst", "1")]
)
def test_indexed_chunks_other_conventions(run_files, convention, data_source):
    from ctapipe_io_zfits.multifile import MultiFiles

    add_file, _ = run_files
    path = add_file(
        [1], convention=convention, data_source=data_source, local_run_id=None
    )
    add_file(
        [2], convention=convention, data_source=data_source, local_run_id=None, chunk=1
    )

    with MultiFiles(path, filename_convention=convention, all_chunks=True) as mf:
        assert [event.event_id for event in mf] == [1, 2]
        assert mf.exhausted


@pytest.mark.parametrize("all_runs", [False, True])
@pytest.mark.parametrize("all_chunks", [False, True])
@pytest.mark.parametrize("next_chunk", [0, 1, 2, 9])
def test_multifiles_all_runs(run_files, all_runs, all_chunks, next_chunk):
    from ctapipe_io_zfits.multifile import MultiFiles

    add_file, files = run_files
    for source, offset in (("SDH001", 0), ("SDH002", 1)):
        path = add_file([1 + offset, 3 + offset], data_source=source)
        add_file([5 + offset], data_source=source, chunk=1)
        add_file(
            [7 + offset, 9 + offset],
            data_source=source,
            local_run_id=8,
            chunk=next_chunk,
            timestamp="20261019T210000",
        )
        add_file(
            [11 + offset],
            data_source=source,
            local_run_id=8,
            chunk=next_chunk + 1,
            timestamp="20261019T210000",
        )
        # Exercise numeric run ordering and a gap between available local runs.
        add_file([13 + offset], data_source=source, local_run_id=10, chunk=20 + offset)
        # A different observation must never be opened, even with the next run id.
        add_file([100 + offset], data_source=source, local_run_id=9, obs_id=457)

    with MultiFiles(path, all_chunks=all_chunks, all_runs=all_runs) as mf:
        expected = [1, 2, 3, 4]
        if all_chunks:
            expected += [5, 6]
        if all_runs:
            expected += [7, 8, 9, 10]
            if all_chunks:
                expected += [11, 12]
            expected += [13, 14]

        assert [event.event_id for event in mf] == expected
        assert mf.exhausted

    for file_ in files.values():
        if file_.Events.__next__.called:
            file_.close.assert_called_once()
        else:
            file_.close.assert_not_called()


@pytest.mark.parametrize("ignore_timestamp", [True, False])
def test_duplicate_chunks_use_latest_timestamp(run_files, ignore_timestamp):
    from ctapipe_io_zfits.multifile import MultiFiles

    add_file, _ = run_files
    path = add_file([1])
    add_file([100], local_run_id=8, timestamp="20261019T210000")
    add_file([2], local_run_id=8, timestamp="20261019T220000")
    add_file([200], local_run_id=8, chunk=1, timestamp="20261019T210000")
    add_file([3], local_run_id=8, chunk=1, timestamp="20261019T220000")
    add_file([4], local_run_id=8, chunk=2, timestamp="20261019T230000")

    with MultiFiles(
        path, all_chunks=True, all_runs=True, ignore_timestamp=ignore_timestamp
    ) as mf:
        expected = [1, 2, 3, 4] if ignore_timestamp else [1, 2, 3]
        assert [event.event_id for event in mf] == expected
        assert mf.exhausted


@pytest.mark.parametrize("ignore_timestamp", [True, False])
def test_next_run_matching_metadata(run_files, ignore_timestamp):
    from ctapipe_io_zfits.multifile import MultiFiles

    add_file, _ = run_files
    path = add_file([1])
    add_file([2], local_run_id=8, timestamp="20261019T210000")
    for fields in (
        {"tel_id": 2},
        {"sb_id": 124},
        {"obs_id": 457},
        {"data_type": "CALIB"},
        {"extra_suffix": "_other"},
        {"data_source": "SDH002"},
    ):
        add_file([100], local_run_id=8, chunk=1, **fields)

    # Files using a different convention are ignored during run discovery.
    path.with_name("unrelated.fits.fz").touch()
    with MultiFiles(path, all_runs=True, ignore_timestamp=ignore_timestamp) as mf:
        assert [event.event_id for event in mf] == [1, 2]
        assert mf.exhausted


@pytest.mark.parametrize("ignore_timestamp", [True, False])
def test_next_run_padded_ids(run_files, ignore_timestamp):
    from ctapipe_io_zfits.multifile import MultiFiles

    add_file, _ = run_files
    padding = {"local_run_id_padding": 6, "chunk_padding": 3}
    path = add_file([1], **padding)
    add_file([2], local_run_id=8, chunk=9, timestamp="20261019T210000", **padding)
    add_file([3], local_run_id=8, chunk=10, timestamp="20261019T210000", **padding)

    with MultiFiles(
        path, all_chunks=True, all_runs=True, ignore_timestamp=ignore_timestamp
    ) as mf:
        assert [event.event_id for event in mf] == [1, 2, 3]
        assert mf.exhausted


def test_all_runs_without_local_run_id(run_files):
    from ctapipe_io_zfits.multifile import MultiFiles

    add_file, _ = run_files
    path = add_file([1], local_run_id=None)
    add_file([2], local_run_id=8)

    with MultiFiles(path, all_chunks=True, all_runs=True) as mf:
        assert [event.event_id for event in mf] == [1]
        assert mf.exhausted


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

        assert mf.exhausted

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


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        (
            "TEL001_SDH001_20230802T021531_SBID00123_OBSID00456_CHUNK000.fits.fz",  # noqa
            FileInfo(
                tel_id=1,
                data_source="SDH001",
                timestamp="20230802T021531",
                sb_id=123,
                obs_id=456,
                chunk=0,
                obs_id_padding=5,
                sb_id_padding=5,
                chunk_padding=3,
            ),
        ),
        (
            "TEL001_SDH001_20230802T021531_SBID00123_OBSID00456_TEL_MUON_CHUNK000.fits.fz",  # noqa
            FileInfo(
                tel_id=1,
                data_source="SDH001",
                timestamp="20230802T021531",
                sb_id=123,
                obs_id=456,
                data_type="TEL_MUON",
                chunk=0,
                obs_id_padding=5,
                sb_id_padding=5,
                chunk_padding=3,
            ),
        ),
        (
            "TEL001_SDH001_20230802T021531_SBID00123_OBSID00456_RUNID000789_CHUNK000.fits.fz",  # noqa
            FileInfo(
                tel_id=1,
                data_source="SDH001",
                timestamp="20230802T021531",
                sb_id=123,
                obs_id=456,
                chunk=0,
                obs_id_padding=5,
                sb_id_padding=5,
                chunk_padding=3,
                local_run_id=789,
                local_run_id_padding=6,
            ),
        ),
    ],
)
def test_acada_dpps_icd_filename_conventions(filename, expected):
    from ctapipe_io_zfits.multifile import filename_conventions, get_file_info

    acada = filename_conventions["acada_dpps_icd"]
    path = f"foo/bar/{filename}"

    file_info = get_file_info(path, convention="acada_dpps_icd")

    for field in FileInfo.__dataclass_fields__.keys():
        assert getattr(file_info, field) == getattr(expected, field), (
            f"Field {field} does not match"
        )

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
