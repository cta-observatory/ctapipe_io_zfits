import datetime
from contextlib import ExitStack
from datetime import timedelta, timezone
from zoneinfo import ZoneInfo

import astropy.units as u
import numpy as np
import pytest
from astropy.time import Time
from ctapipe.containers import EventType
from protozfits import DL0v1_Subarray_pb2 as DL0_Subarray
from protozfits import DL0v1_Telescope_pb2 as DL0_Telescope
from protozfits import ProtobufZOFits
from protozfits.anyarray import numpy_to_any_array

from ctapipe_io_zfits.time import time_to_cta_high_res

acada_user = "ctao-acada-n"
timezone_cta_n = ZoneInfo("Atlantic/Canary")


def evening_of_obs(time: Time, tz: ZoneInfo) -> datetime.date:
    """Get the evening an observation started.

    Uses noon localtime in ``tz`` as a cutoff.
    """
    dt = time.to_datetime(timezone.utc).astimezone(tz)
    if dt.hour < 12:
        return (dt - timedelta(days=1)).date()

    return dt.date()


@pytest.fixture(scope="session")
def acada_base(tmp_path_factory):
    """Base directory of acada data."""
    return tmp_path_factory.mktemp("acada_base_")


@pytest.fixture(scope="session")
def dl0_base(acada_base):
    """DL0 Directory Structure.

    See Table 6 of the ACADA-DPPS ICD.
    """
    dl0 = acada_base / "DL0"
    dl0.mkdir(exist_ok=True)

    lst_base = dl0 / "LSTN-01" / acada_user / "acada-adh"
    lst_events = lst_base / "events"
    lst_monitoring = lst_base / "monitoring"
    array_triggers = dl0 / "array" / acada_user / "acada-adh" / "triggers"
    array_monitoring = dl0 / "array" / acada_user / "acada-adh" / "monitoring"

    for directory in (lst_events, lst_monitoring, array_triggers, array_monitoring):
        directory.mkdir(exist_ok=True, parents=True)

    return dl0


def get_module_and_pixel_id_map(n_modules, n_pixels_module, missing_modules=None):
    module_id_map = np.arange(n_modules)

    if missing_modules is not None:
        module_id_map = np.delete(module_id_map, missing_modules)
        n_modules = len(module_id_map)

    module_ids = np.repeat(module_id_map, n_pixels_module)
    pixel_id_map = module_ids + np.tile(np.arange(n_pixels_module), n_modules)

    return module_id_map, pixel_id_map


# we parametrize the dummy dl0 for a couple of different scenarios.
# Tests using this fixture will be run under all scenarios automatically
@pytest.fixture(
    scope="session",
    params=[
        {
            "missing_modules": True,
            "obs_start": Time("2023-08-02T02:15:31"),
            "sb_creator_id": 2,
            "sb_id": 123,
            "obs_id": 456,
        },
        {
            "missing_modules": False,
            "obs_start": Time("2025-02-04T20:45:31"),
            "sb_creator_id": 2,
            "sb_id": 124,
            "obs_id": 789,
        },
    ],
)
def dummy_dl0(dl0_base, request):
    rng = np.random.default_rng(0)

    config = request.param
    sb_id = config["sb_id"]
    obs_id = config["obs_id"]
    sb_creator_id = config["sb_creator_id"]

    obs_start = config["obs_start"]
    date = evening_of_obs(obs_start, timezone_cta_n)

    date_path = f"{date.year}/{date.month:02d}/{date.day:02d}"
    trigger_dir = dl0_base / "ARRAY" / acada_user / "acada-adh/triggers" / date_path
    lst_event_dir = dl0_base / "LSTN-01" / acada_user / "acada-adh/events" / date_path
    for directory in (trigger_dir, lst_event_dir):
        directory.mkdir(exist_ok=True, parents=True)

    subarray_id = 1
    sdh_ids = (0, 1, 2, 3)

    obs_start_path_string = f"{obs_start.to_datetime(timezone.utc):%Y%m%dT%H%M%S}"
    filename = f"SUB{subarray_id:03d}_SWAT001_{obs_start_path_string}_SBID{sb_id:019d}_OBSID{obs_id:019d}_SUBARRAY_CHUNK000.fits.fz"  # noqa

    # sdh_id and chunk_id will be filled later -> double {{}}
    lst_event_pattern = f"TEL001_SDH{{sdh_id:03d}}_{obs_start_path_string}_SBID{sb_id:019d}_OBSID{obs_id:019d}_TEL_SHOWER_CHUNK{{chunk_id:03d}}.fits.fz"  # noqa
    trigger_path = trigger_dir / filename

    subarray_data_stream = DL0_Subarray.DataStream(
        subarray_id=subarray_id,
        sb_id=sb_id,
        obs_id=obs_id,
        producer_id=1,  # FIXME: what is correct here?,
        sb_creator_id=sb_creator_id,
    )

    lst_data_stream = DL0_Telescope.DataStream(
        tel_id=1,
        sb_id=sb_id,
        obs_id=obs_id,
        waveform_scale=80.0,
        waveform_offset=5.0,
        sb_creator_id=sb_creator_id,
    )

    missing_modules = [50, 200] if config["missing_modules"] else None
    module_id_map, pixel_id_map = get_module_and_pixel_id_map(
        n_modules=265, n_pixels_module=7, missing_modules=missing_modules
    )
    n_pixels = len(pixel_id_map)

    camera_configuration = DL0_Telescope.CameraConfiguration(
        tel_id=1,
        local_run_id=789,
        config_time_s=obs_start.unix,
        camera_config_id=47,
        num_pixels=len(pixel_id_map),
        pixel_id_map=numpy_to_any_array(pixel_id_map),
        module_id_map=numpy_to_any_array(module_id_map),
        num_channels=2,
        num_samples_nominal=40,
        num_samples_long=0,
        num_modules=len(module_id_map),
        sampling_frequency=1024,
    )

    time = obs_start

    ctx = ExitStack()
    proto_kwargs = dict(
        n_tiles=5, rows_per_tile=20, compression_block_size_kb=64 * 1024
    )

    chunksize = 10
    events_written = {sdh_id: 0 for sdh_id in sdh_ids}
    current_chunk = {sdh_id: -1 for sdh_id in sdh_ids}
    lst_event_files = {}

    def open_next_event_file(sdh_id):
        if sdh_id in lst_event_files:
            lst_event_files[sdh_id].close()

        current_chunk[sdh_id] += 1
        chunk_id = current_chunk[sdh_id]
        path = lst_event_dir / lst_event_pattern.format(
            sdh_id=sdh_id, chunk_id=chunk_id
        )
        f = ctx.enter_context(ProtobufZOFits(**proto_kwargs))
        f.open(str(path))
        f.move_to_new_table("DataStream")
        f.write_message(lst_data_stream)
        f.move_to_new_table("CameraConfiguration")
        f.write_message(camera_configuration)
        f.move_to_new_table("Events")
        lst_event_files[sdh_id] = f
        events_written[sdh_id] = 0

    def convert_waveform(waveform):
        scale = lst_data_stream.waveform_scale
        offset = lst_data_stream.waveform_offset
        return ((waveform + offset) * scale).astype(np.uint16)

    with ctx:
        trigger_file = ctx.enter_context(ProtobufZOFits(**proto_kwargs))
        trigger_file.open(str(trigger_path))
        trigger_file.move_to_new_table("DataStream")
        trigger_file.write_message(subarray_data_stream)
        trigger_file.move_to_new_table("SubarrayEvents")

        for sdh_id in sdh_ids:
            open_next_event_file(sdh_id)

        for i in range(100):
            event_id = i + 1
            time_s, time_qns = time_to_cta_high_res(time)

            trigger_file.write_message(
                DL0_Subarray.Event(
                    event_id=event_id,
                    trigger_type=1,
                    sb_id=sb_id,
                    obs_id=obs_id,
                    event_time_s=int(time_s),
                    event_time_qns=int(time_qns),
                    trigger_ids=numpy_to_any_array(np.array([event_id])),
                    tel_ids_with_trigger=numpy_to_any_array(np.array([1])),
                    tel_ids_with_data=numpy_to_any_array(np.array([1])),
                )
            )

            sdh_id = sdh_ids[i % len(sdh_ids)]
            # TODO: randomize event to test actually parsing it

            # TODO: fill actual signal into waveform, not just 0
            waveform = rng.normal(0.0, 1.0, size=(1, n_pixels, 40)).astype(np.float32)

            lst_event_files[sdh_id].write_message(
                DL0_Telescope.Event(
                    event_id=event_id,
                    tel_id=camera_configuration.tel_id,
                    event_type=EventType.SUBARRAY.value,
                    event_time_s=int(time_s),
                    event_time_qns=int(time_qns),
                    # identified as signal, low gain stored, high gain stored
                    pixel_status=numpy_to_any_array(
                        np.full(n_pixels, 0b00001101, dtype=np.uint8)
                    ),
                    waveform=numpy_to_any_array(convert_waveform(waveform)),
                    num_channels=1,
                    num_samples=40,
                    num_pixels_survived=n_pixels,
                )
            )
            events_written[sdh_id] += 1
            if events_written[sdh_id] >= chunksize:
                open_next_event_file(sdh_id)

            time = time + 0.001 * u.s

    config["trigger_path"] = trigger_path
    config["first_tel_path"] = lst_event_dir / lst_event_pattern.format(
        sdh_id=0, chunk_id=0
    )
    return config


@pytest.fixture(scope="session")
def dummy_tel_file(dummy_dl0):
    return dummy_dl0["first_tel_path"]


@pytest.fixture(scope="session")
def dummy_tel_file_no_ids(dl0_base):
    """Test for files that do not contain SBID / OBSID in their names.

    The files with calibration events (interleaved flatfield and pedestals)
    during the ACADA LST tests 2025-10-22 did not contain these fields due to a bug.
    """
    rng = np.random.default_rng(0)

    sb_creator_id = 2
    sb_id = int(sb_creator_id * 1e9) + 123
    obs_id = int(sb_creator_id * 1e9) + 234

    obs_start = Time("2025-10-22T23:50:01")
    date = evening_of_obs(obs_start, timezone_cta_n)

    date_path = f"{date.year}/{date.month:02d}/{date.day:02d}"
    directory = dl0_base / "LSTN-01" / acada_user / "acada-adh/events" / date_path
    directory.mkdir(exist_ok=True, parents=True)

    sdh_ids = (0, 1, 2, 3)

    # sdh_id and chunk_id will be filled later -> double {{}}
    obs_start_path_string = f"{obs_start.to_datetime(timezone.utc):%Y%m%dT%H%M%S}"
    # ped was having no info, ff had "CALIB" but no ids
    lst_ped_pattern = f"TEL001_SDH{{sdh_id:03d}}_{obs_start_path_string}_CHUNK{{chunk_id:03d}}.fits.fz"  # noqa
    lst_ff_pattern = f"TEL001_SDH{{sdh_id:03d}}_{obs_start_path_string}_CALIB_CHUNK{{chunk_id:03d}}.fits.fz"  # noqa

    data_stream = DL0_Telescope.DataStream(
        tel_id=1,
        sb_id=sb_id,
        obs_id=obs_id,
        waveform_scale=60.0,
        waveform_offset=5.0,
        sb_creator_id=sb_creator_id,
    )

    module_id_map, pixel_id_map = get_module_and_pixel_id_map(
        n_modules=265, n_pixels_module=7
    )
    n_pixels = len(pixel_id_map)

    camera_configuration = DL0_Telescope.CameraConfiguration(
        tel_id=1,
        local_run_id=789,
        config_time_s=obs_start.unix,
        camera_config_id=47,
        num_pixels=n_pixels,
        pixel_id_map=numpy_to_any_array(pixel_id_map),
        module_id_map=numpy_to_any_array(module_id_map),
        num_channels=2,
        num_samples_nominal=40,
        num_samples_long=0,
        num_modules=len(module_id_map),
        sampling_frequency=1024,
    )

    time = obs_start

    ctx = ExitStack()
    proto_kwargs = dict(
        n_tiles=5, rows_per_tile=20, compression_block_size_kb=64 * 1024
    )

    chunksize = 10
    data_types = ["flatfield", "pedestal"]
    events_written = {
        (sdh_id, data_type): 0 for sdh_id in sdh_ids for data_type in data_types
    }
    current_chunk = {
        (sdh_id, data_type): -1 for sdh_id in sdh_ids for data_type in data_types
    }
    mean_charge = {"flatfield": 70.0, "pedestal": 0.0}

    open_files = {}

    def open_next_event_file(sdh_id, data_type):
        key = (sdh_id, data_type)
        if key in open_files:
            open_files[key].close()

        current_chunk[key] += 1
        chunk_id = current_chunk[key]

        if data_type == "flatfield":
            pattern = lst_ff_pattern
        else:
            pattern = lst_ped_pattern

        path = directory / pattern.format(sdh_id=sdh_id, chunk_id=chunk_id)

        print(f"Opening path: {path} for {data_type=}, {chunk_id=}")
        f = ctx.enter_context(ProtobufZOFits(**proto_kwargs))
        f.open(str(path))
        f.move_to_new_table("DataStream")
        f.write_message(data_stream)
        f.move_to_new_table("CameraConfiguration")
        f.write_message(camera_configuration)
        f.move_to_new_table("Events")
        open_files[key] = f
        events_written[key] = 0

    def convert_waveform(waveform):
        scale = data_stream.waveform_scale
        offset = data_stream.waveform_offset
        return ((waveform + offset) * scale).astype(np.uint16)

    event_types = {
        "flatfield": EventType.FLATFIELD.value,
        "pedestal": EventType.SKY_PEDESTAL.value,
    }

    with ctx:
        for data_type in data_types:
            for sdh_id in sdh_ids:
                open_next_event_file(sdh_id, data_type)

        for i in range(100):
            # round robin over data types
            data_type = data_types[i % len(data_types)]

            event_id = i + 1
            time_s, time_qns = time_to_cta_high_res(time)

            sdh_id = sdh_ids[i // 2 % len(sdh_ids)]
            key = (sdh_id, data_type)

            if events_written[key] >= chunksize:
                open_next_event_file(sdh_id, data_type)

            waveform = rng.normal(
                mean_charge[data_type], 1.0, size=(1, n_pixels, 40)
            ).astype(np.float32)

            open_files[key].write_message(
                DL0_Telescope.Event(
                    event_id=event_id,
                    tel_id=camera_configuration.tel_id,
                    event_type=event_types[data_type],
                    event_time_s=int(time_s),
                    event_time_qns=int(time_qns),
                    # identified as signal, low gain stored, high gain stored
                    pixel_status=numpy_to_any_array(
                        np.full(n_pixels, 0b00001101, dtype=np.uint8)
                    ),
                    waveform=numpy_to_any_array(convert_waveform(waveform)),
                    num_channels=1,
                    num_samples=40,
                    num_pixels_survived=n_pixels,
                )
            )
            events_written[key] += 1

            time = time + 0.001 * u.s

    for f in open_files.values():
        f.close()

    first_ped = directory / lst_ped_pattern.format(sdh_id=0, chunk_id=0)
    first_ff = directory / lst_ff_pattern.format(sdh_id=0, chunk_id=0)
    return first_ff, first_ped
