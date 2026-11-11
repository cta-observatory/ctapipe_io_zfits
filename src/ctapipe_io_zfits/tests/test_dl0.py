from types import SimpleNamespace

import astropy.units as u
import numpy as np
import pytest
from ctapipe.containers import EventType
from ctapipe.core.tool import run_tool
from ctapipe.instrument import SubarrayDescription
from ctapipe.io import EventSource, TableLoader
from ctapipe.tools.process import ProcessorTool
from traitlets.config import Config


@pytest.mark.parametrize("dvr", [False, True])
@pytest.mark.parametrize("has_pedestal", [False, True])
def test_dl0_pedestal_intensity(dvr, has_pedestal):
    from ctapipe_io_zfits.source import _fill_telescope_event

    pixel_ids = np.array([5, 2, 7, 0])
    status = np.array([13, 0, 13, 0] if dvr else [13] * 4, dtype=np.uint8)
    pedestal = np.array([10, 20] if dvr else [10, 20, 30, 40], dtype=np.float32)
    n_stored = 2 if dvr else 4
    tel_event = SimpleNamespace(
        event_id=1,
        num_channels=1,
        num_pixels_survived=n_stored,
        num_samples=3,
        waveform=np.ones((1, n_stored, 3), dtype=np.uint16),
        pixel_status=status,
        pixel_time_shift=None,
        pedestal_intensity=pedestal if has_pedestal else None,
        event_type=EventType.SUBARRAY.value,
        event_time_s=1700000000,
        event_time_qns=0,
        first_cell_id=None,
        calibration_monitoring_id=0,
    )
    event = _fill_telescope_event(
        tel_event,
        data_stream=SimpleNamespace(
            obs_id=789, tel_id=1, waveform_scale=1.0, waveform_offset=0.0
        ),
        camera_config=SimpleNamespace(num_pixels=4, pixel_id_map=pixel_ids),
        camera=SimpleNamespace(geometry=SimpleNamespace(n_pixels=8)),
    )
    if has_pedestal:
        expected = np.zeros(8, dtype=np.float32)
        if dvr:
            expected[5], expected[7] = 10, 20
        else:
            expected[5], expected[2], expected[7], expected[0] = 10, 20, 30, 40
        np.testing.assert_array_equal(event.dl0.camera.pedestal_intensity, expected)
        assert event.dl0.camera.pedestal_intensity.dtype == np.float32
    else:
        assert event.dl0.camera.pedestal_intensity is None


def test_is_compatible(dummy_dl0):
    from ctapipe_io_zfits import ProtozfitsEventSource

    assert ProtozfitsEventSource.is_compatible(dummy_dl0["trigger_path"])


def test_is_valid_eventsource(dummy_dl0):
    from ctapipe_io_zfits.source import ProtozfitsEventSource

    with EventSource(dummy_dl0["trigger_path"]) as source:
        assert isinstance(source, ProtozfitsEventSource)


def test_subarray(dummy_dl0):
    with EventSource(dummy_dl0["trigger_path"]) as source:
        assert isinstance(source.subarray, SubarrayDescription)


def test_subarray_events(dummy_dl0):
    time = dummy_dl0["obs_start"]
    all_chunks = dummy_dl0.get("all_chunks", False)
    config = Config({"MultiFiles": {"all_chunks": all_chunks}})

    with EventSource(dummy_dl0["trigger_path"], config=config) as source:
        n_read = 0
        for i, array_event in enumerate(source):
            assert array_event.count == i
            assert array_event.index.obs_id == dummy_dl0["obs_id"]
            assert array_event.index.event_id == n_read + 1
            dt = np.abs(array_event.dl0.trigger.time - time).to(u.ns)
            assert dt < 0.2 * u.ns
            expected_trigger = [1, 2] if dummy_dl0.get("busy_trigger") else [1]
            assert array_event.dl0.trigger.tels_with_trigger == expected_trigger
            assert array_event.dl0.trigger.tels_with_data == [1]
            assert array_event.tel.keys() == {1}

            telescope_event = array_event.tel[1]
            assert telescope_event.index.obs_id == array_event.index.obs_id
            assert telescope_event.index.event_id == array_event.index.event_id
            assert telescope_event.index.tel_id == 1
            assert (
                telescope_event.dl0.trigger.time
                == telescope_event.dl0.camera.event_time
            )
            assert telescope_event.dl0.trigger.event_type == EventType.SUBARRAY
            assert np.any(array_event.tel[1].dl0.camera.waveform != 0.0)
            assert array_event.tel[1].dl0.camera.waveform.dtype == np.float32

            n_read += 1
            time = time + 0.001 * u.s

            if dummy_dl0.get("pixel_time_shift"):
                pixel_time_shift = array_event.tel[1].dl0.camera.pixel_time_shift
                assert pixel_time_shift is not None

                if dummy_dl0.get("dvr"):
                    pixel_stored = array_event.tel[1].dl0.camera.pixel_status != 0
                    np.testing.assert_array_equal(
                        pixel_time_shift[0, ~pixel_stored], 0.0
                    )
                    assert np.all(pixel_time_shift[0, pixel_stored] > 0.0)

        assert n_read == (100 if all_chunks else 40)


def test_process(dummy_dl0, tmp_path):
    input_path = dummy_dl0["trigger_path"]
    output_path = tmp_path / "dummy.dl1.h5"
    run_tool(
        ProcessorTool(),
        [
            f"--input={input_path}",
            f"--output={output_path}",
            "--write-images",
            "--write-parameters",
            "--EventTypeFilter.allowed_types=SUBARRAY",
        ],
        raises=True,
    )


def test_telescope_event_source(dummy_tel_file):
    from ctapipe_io_zfits.source import ProtozfitsTelescopeEventSource

    assert ProtozfitsTelescopeEventSource.is_compatible(dummy_tel_file)

    with EventSource(dummy_tel_file) as source:
        assert isinstance(source, ProtozfitsTelescopeEventSource)

        for i, event in enumerate(source):
            assert event.count == i
            assert event.tel.keys() == {1}
            assert event.dl0.trigger.tels_with_data == [1]


def test_process_tel_events(dummy_dl0, tmp_path):
    input_path = dummy_dl0["first_tel_path"]
    output_path = tmp_path / "dummy.dl1.h5"

    run_tool(
        ProcessorTool(),
        [
            f"--input={input_path}",
            f"--output={output_path}",
            "--write-images",
            "--write-parameters",
            "--MultiFiles.all_chunks=True",
        ],
        raises=True,
    )

    with TableLoader(output_path) as loader:
        events = loader.read_telescope_events()
        assert len(events) == 100
        np.testing.assert_array_equal(events["obs_id"], dummy_dl0["obs_id"])
        np.testing.assert_array_equal(events["tel_id"], 1)
        np.testing.assert_array_equal(events["event_id"], np.arange(1, 101))


def test_pixel_status(dummy_tel_file):
    from ctapipe_io_zfits.source import ProtozfitsTelescopeEventSource

    with ProtozfitsTelescopeEventSource(dummy_tel_file, max_events=10) as f:
        n_read = 0
        for e in f:
            n_read += 1

            (missing_pixels_from_status,) = np.nonzero(
                e.tel[1].dl0.camera.pixel_status == 0
            )
            _, missing_pixels_from_waveform = np.nonzero(
                e.tel[1].dl0.camera.waveform.sum(axis=2) == 0
            )

            np.testing.assert_array_equal(
                missing_pixels_from_status, missing_pixels_from_waveform
            )

        assert n_read == 10


def test_telescope_event_source_missing_ids(dummy_tel_file_no_ids):
    from ctapipe_io_zfits.source import ProtozfitsTelescopeEventSource

    first_ff_file, first_ped_file = dummy_tel_file_no_ids

    assert ProtozfitsTelescopeEventSource.is_compatible(first_ff_file)
    assert ProtozfitsTelescopeEventSource.is_compatible(first_ped_file)

    config = Config({"MultiFiles": {"all_chunks": True}})
    with EventSource(first_ff_file, config=config) as source:
        assert isinstance(source, ProtozfitsTelescopeEventSource)

        n_read = 0
        for event in source:
            assert event.count == n_read
            assert event.tel.keys() == {1}
            assert event.dl0.trigger.event_type == EventType.FLATFIELD
            n_read += 1

        assert n_read == 50

    with EventSource(first_ped_file, config=config) as source:
        assert isinstance(source, ProtozfitsTelescopeEventSource)

        n_read = 0
        for event in source:
            assert event.count == n_read
            assert event.tel.keys() == {1}
            assert event.tel[1].dl0.camera.waveform is not None
            assert event.dl0.trigger.event_type == EventType.SKY_PEDESTAL

            n_read += 1

        assert n_read == 50
