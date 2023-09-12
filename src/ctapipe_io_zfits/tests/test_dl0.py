import astropy.units as u
import numpy as np
import pytest
from astropy.time import Time
from ctapipe.core.tool import run_tool
from ctapipe.instrument import SubarrayDescription
from ctapipe.io import EventSource, TableLoader
from ctapipe.tools.process import ProcessorTool


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


def test_process(dummy_dl0, tmp_path):
    path = tmp_path / "dummy.dl1.h5"

    with pytest.warns(UserWarning, match="Encountered an event with no R1 data"):
        run_tool(
            ProcessorTool(),
            [
                f"--input={dummy_dl0}",
                f"--output={path}",
                "--write-images",
                "--write-parameters",
            ],
            raises=True,
        )



def test_telescope_event_source(dummy_tel_file):
    from ctapipe_io_zfits.dl0 import ProtozfitsDL0TelescopeEventSource

    assert ProtozfitsDL0TelescopeEventSource.is_compatible(dummy_tel_file)

    with EventSource(dummy_tel_file) as source:
        assert isinstance(source, ProtozfitsDL0TelescopeEventSource)

        for event in source:
            assert event.dl0.tel.keys() == {1}


def test_process_tel_events(dummy_tel_file, tmp_path):
    path = tmp_path / "dummy.dl1.h5"

    with pytest.warns(UserWarning, match="Encountered an event with no R1 data"):
        run_tool(
            ProcessorTool(),
            [
                f"--input={dummy_tel_file}",
                f"--output={path}",
                "--write-images",
                "--write-parameters",
            ],
            raises=True,
        )

    with TableLoader(path) as loader:
        events = loader.read_telescope_events()
        assert len(events) == 100
        np.testing.assert_array_equal(events["obs_id"], 456)
        np.testing.assert_array_equal(events["tel_id"], 1)
        np.testing.assert_array_equal(events["event_id"], np.arange(1, 101))

