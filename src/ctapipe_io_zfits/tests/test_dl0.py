import astropy.units as u
import numpy as np
from ctapipe.containers import EventType
from ctapipe.core.tool import run_tool
from ctapipe.instrument import SubarrayDescription
from ctapipe.io import EventSource, TableLoader
from ctapipe.tools.process import ProcessorTool


def test_is_compatible(dummy_dl0):
    from ctapipe_io_zfits import ProtozfitsDL0EventSource

    assert ProtozfitsDL0EventSource.is_compatible(dummy_dl0["trigger_path"])


def test_is_valid_eventsource(dummy_dl0):
    from ctapipe_io_zfits.dl0 import ProtozfitsDL0EventSource

    with EventSource(dummy_dl0["trigger_path"]) as source:
        assert isinstance(source, ProtozfitsDL0EventSource)


def test_subarray(dummy_dl0):
    with EventSource(dummy_dl0["trigger_path"]) as source:
        assert isinstance(source.subarray, SubarrayDescription)


def test_subarray_events(dummy_dl0):
    time = dummy_dl0["obs_start"]

    with EventSource(dummy_dl0["trigger_path"]) as source:
        n_read = 0
        for i, array_event in enumerate(source):
            assert array_event.count == i
            assert array_event.index.obs_id == dummy_dl0["obs_id"]
            assert array_event.index.event_id == n_read + 1
            dt = np.abs(array_event.trigger.time - time).to(u.ns)
            assert dt < 0.2 * u.ns
            assert array_event.trigger.tels_with_trigger == [
                1,
            ]

            assert np.any(array_event.dl0.tel[1].waveform != 0.0)
            assert array_event.dl0.tel[1].waveform.dtype == np.float32

            n_read += 1
            time = time + 0.001 * u.s

        assert n_read == 100


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
    from ctapipe_io_zfits.dl0 import ProtozfitsDL0TelescopeEventSource

    assert ProtozfitsDL0TelescopeEventSource.is_compatible(dummy_tel_file)

    with EventSource(dummy_tel_file) as source:
        assert isinstance(source, ProtozfitsDL0TelescopeEventSource)

        for i, event in enumerate(source):
            assert event.count == i
            assert event.dl0.tel.keys() == {1}


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
    from ctapipe_io_zfits.dl0 import ProtozfitsDL0TelescopeEventSource

    with ProtozfitsDL0TelescopeEventSource(dummy_tel_file, max_events=10) as f:
        n_read = 0
        for e in f:
            n_read += 1

            (missing_pixels_from_status,) = np.nonzero(e.dl0.tel[1].pixel_status == 0)
            _, missing_pixels_from_waveform = np.nonzero(
                e.dl0.tel[1].waveform.sum(axis=2) == 0
            )

            np.testing.assert_array_equal(
                missing_pixels_from_status, missing_pixels_from_waveform
            )

        assert n_read == 10


def test_telescope_event_source_missing_ids(dummy_tel_file_no_ids):
    from ctapipe_io_zfits.dl0 import ProtozfitsDL0TelescopeEventSource

    first_ff_file, first_ped_file = dummy_tel_file_no_ids

    assert ProtozfitsDL0TelescopeEventSource.is_compatible(first_ff_file)
    assert ProtozfitsDL0TelescopeEventSource.is_compatible(first_ped_file)

    with EventSource(first_ff_file) as source:
        assert isinstance(source, ProtozfitsDL0TelescopeEventSource)

        n_read = 0
        for event in source:
            assert event.count == n_read
            assert event.dl0.tel.keys() == {1}
            assert event.trigger.event_type == EventType.FLATFIELD
            n_read += 1

        assert n_read == 50

    with EventSource(first_ped_file) as source:
        assert isinstance(source, ProtozfitsDL0TelescopeEventSource)

        n_read = 0
        for event in source:
            assert event.count == n_read
            assert event.dl0.tel.keys() == {1}
            assert event.trigger.event_type == EventType.SKY_PEDESTAL
            n_read += 1

        assert n_read == 50
