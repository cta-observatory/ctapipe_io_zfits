import astropy.units as u
import numpy as np
import pytest
from ctapipe.containers import EventType, PixelStatus
from ctapipe.io import DataLevel, EventSource
from protozfits import FlatProtobufZOFits
from protozfits import R1v1_pb2 as R1
from protozfits.anyarray import numpy_to_any_array
from traitlets.config import Config

from ctapipe_io_zfits import ProtozfitsEventSource, ProtozfitsTelescopeEventSource
from ctapipe_io_zfits.source import CTAPIPE_GE_0_31
from ctapipe_io_zfits.time import cta_high_res_to_time


@pytest.fixture(params=[(1, False), (1, True), (2, False), (2, True)])
def r1_file(tmp_path, request):
    n_channels, time_shift = request.param
    pixel_ids = np.array([5, 2, 7, 0], dtype=np.uint16)
    waveform = np.arange(n_channels * 4 * 6, dtype=np.uint16).reshape(n_channels, 4, 6)
    status = np.array(
        [4, 8, 4, 0] if n_channels == 1 else [4, 8, 12, 0], dtype=np.uint8
    )
    first_cell_id = np.array([10, 20], dtype=np.uint16)
    pedestal = np.arange(4, dtype=np.float32)
    shifts = np.arange(n_channels * 4, dtype=np.int16).reshape(n_channels, 4)

    first_path = None
    for chunk in range(2):
        path = tmp_path / (
            f"TEL001_SDH000_20260204T204531_SBID0000000000000000124_"
            f"OBSID0000000000000000789_TEL_SHOWER_CHUNK{chunk:03d}.fits.fz"
        )
        if first_path is None:
            first_path = path
        with FlatProtobufZOFits() as writer:
            writer.open(str(path))
            writer.move_to_new_table("DataStream")
            writer.write_message(
                R1.TelescopeDataStream(
                    tel_id=1,
                    sb_id=124,
                    obs_id=789,
                    waveform_scale=2.0,
                    waveform_offset=3.0,
                )
            )
            writer.move_to_new_table("CameraConfiguration")
            writer.write_message(
                R1.CameraConfiguration(
                    tel_id=1,
                    num_pixels=4,
                    num_modules=1,
                    num_channels=n_channels,
                    pixel_id_map=numpy_to_any_array(pixel_ids),
                    module_id_map=numpy_to_any_array(np.array([0], dtype=np.uint16)),
                    num_samples_nominal=6,
                )
            )
            writer.move_to_new_table("Events")
            extra = {}
            if time_shift:
                extra["pixel_time_shift"] = numpy_to_any_array(shifts)
            writer.write_message(
                R1.Event(
                    event_id=chunk + 1,
                    tel_id=1,
                    event_type=EventType.SUBARRAY.value,
                    event_time_s=1700000000,
                    event_time_qns=123,
                    num_channels=n_channels,
                    num_samples=6,
                    num_pixels=4,
                    num_modules=1,
                    waveform=numpy_to_any_array(waveform),
                    pixel_status=numpy_to_any_array(status),
                    first_cell_id=numpy_to_any_array(first_cell_id),
                    pedestal_intensity=numpy_to_any_array(pedestal),
                    calibration_monitoring_id=42,
                    **extra,
                )
            )

    return {
        "path": first_path,
        "waveform": waveform,
        "pixel_ids": pixel_ids,
        "status": status,
        "first_cell_id": first_cell_id,
        "pedestal": pedestal,
        "shifts": shifts if time_shift else None,
        "n_channels": n_channels,
    }


@pytest.mark.parametrize("fill_nan", [False, True])
def test_r1_telescope_source(r1_file, fill_nan):
    path = r1_file["path"]
    assert ProtozfitsTelescopeEventSource.is_compatible(path)
    assert not ProtozfitsEventSource.is_compatible(path)
    config = Config(
        {
            "MultiFiles": {"all_chunks": True},
            "ProtozfitsTelescopeEventSource": {
                "ignore_samples_start": 1,
                "ignore_samples_end": 2,
                "dvr_fill_nan": fill_nan,
            },
        }
    )
    with EventSource(path, config=config) as source:
        assert isinstance(source, ProtozfitsTelescopeEventSource)
        assert source.datalevels == (DataLevel.R1,)
        assert not source.is_simulation
        assert source.observation_blocks[789].sb_id == 124
        assert source.scheduling_blocks[124].sb_id == 124
        n_pixels = source.subarray.tel[1].camera.geometry.n_pixels
        n_read = 0
        for event in source:
            n_read += 1
            assert event.index.event_id == n_read
            assert event.index.obs_id == 789
            assert event.r1.tel.keys() == {1}
            assert not event.dl0.tel
            camera = event.r1.tel[1]
            ids = r1_file["pixel_ids"]
            expected = np.full(
                (r1_file["n_channels"], n_pixels, 3),
                np.nan if fill_nan else 0.0,
                dtype=np.float32,
            )
            expected[:, ids] = r1_file["waveform"][..., 1:-2] / 2.0 - 3.0
            np.testing.assert_array_equal(camera.waveform, expected)
            assert camera.waveform.dtype == np.float32
            np.testing.assert_array_equal(
                PixelStatus.get_channel_info(camera.pixel_status[ids]),
                PixelStatus.get_channel_info(r1_file["status"]),
            )
            np.testing.assert_array_equal(camera.pixel_status[ids], r1_file["status"])
            mask = event.monitoring.tel[1].camera.coefficients.outlier_mask
            np.testing.assert_array_equal(
                mask[:, ids],
                [
                    [False, True, False, True],
                    [True, False, r1_file["n_channels"] == 1, True],
                ],
            )
            missing_pixels = np.ones(n_pixels, dtype=bool)
            missing_pixels[ids] = False
            assert np.all(mask[:, missing_pixels])
            if r1_file["n_channels"] == 1:
                np.testing.assert_array_equal(
                    camera.selected_gain_channel[ids], [0, 1, 0, 1]
                )
            else:
                assert camera.selected_gain_channel is None
            assert camera.event_type == EventType.SUBARRAY
            dt = camera.event_time - cta_high_res_to_time(1700000000, 123)
            assert abs(dt.to_value(u.ns)) < 0.2
            assert event.trigger.tel[1].time == camera.event_time
            np.testing.assert_array_equal(
                camera.first_cell_id, r1_file["first_cell_id"]
            )
            np.testing.assert_array_equal(
                camera.pedestal_intensity[ids], r1_file["pedestal"]
            )
            assert camera.calibration_monitoring_id == 42
            if CTAPIPE_GE_0_31:
                if r1_file["shifts"] is None:
                    assert camera.pixel_time_shift is None
                else:
                    expected_shifts = np.zeros(
                        (r1_file["n_channels"], n_pixels), dtype=np.float32
                    )
                    expected_shifts[:, ids] = r1_file["shifts"].astype(
                        np.float32
                    ) * np.float32(0.01)
                    np.testing.assert_array_equal(
                        camera.pixel_time_shift, expected_shifts
                    )
                    assert camera.pixel_time_shift.dtype == np.float32
        assert n_read == 2
