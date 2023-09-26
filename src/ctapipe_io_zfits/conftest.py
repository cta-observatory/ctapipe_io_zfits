from contextlib import ExitStack
from datetime import timedelta, timezone
from zoneinfo import ZoneInfo

import astropy.units as u
import numpy as np
import pytest
from astropy.time import Time
from ctapipe.containers import EventType
from protozfits import CTA_DL0_Subarray_pb2 as DL0_Subarray
from protozfits import CTA_DL0_Telescope_pb2 as DL0_Telescope
from protozfits import ProtobufZOFits
from protozfits.CoreMessages_pb2 import AnyArray

from ctapipe_io_zfits.time import time_to_cta_high_res

ANY_ARRAY_TYPE_TO_NUMPY_TYPE = {
    AnyArray.S8: np.int8,
    AnyArray.U8: np.uint8,
    AnyArray.S16: np.int16,
    AnyArray.U16: np.uint16,
    AnyArray.S32: np.int32,
    AnyArray.U32: np.uint32,
    AnyArray.S64: np.int64,
    AnyArray.U64: np.uint64,
    AnyArray.FLOAT: np.float32,
    AnyArray.DOUBLE: np.float64,
}

DTYPE_TO_ANYARRAY_TYPE = {v: k for k, v in ANY_ARRAY_TYPE_TO_NUMPY_TYPE.items()}

acada_user = "ctao-acada-n"
obs_start = Time("2023-08-02T02:15:31")
timezone_cta_n = ZoneInfo("Atlantic/Canary")


def to_anyarray(array):
    type_ = DTYPE_TO_ANYARRAY_TYPE[array.dtype.type]
    return AnyArray(type=type_, data=array.tobytes())


def evening_of_obs(time, tz):
    dt = time.to_datetime(timezone.utc).astimezone(tz)
    if dt.hour < 12:
        return (dt - timedelta(days=1)).date()

    return dt.date()


@pytest.fixture(scope="session")
def acada_base(tmp_path_factory):
    return tmp_path_factory.mktemp("acada_base_")


@pytest.fixture(scope="session")
def dl0_base(acada_base):
    """DL0 Directory Structure.

    See Table 6 of the ACADA-DPPS ICD.
    """
    dl0 = acada_base / "DL0"
    dl0.mkdir(exist_ok=True)

    lst_base = dl0 / "TEL001" / acada_user / "acada-adh"
    lst_events = lst_base / "events"
    lst_monitoring = lst_base / "monitoring"
    array_triggers = dl0 / "array" / acada_user / "acada-adh" / "triggers"
    array_monitoring = dl0 / "array" / acada_user / "acada-adh" / "monitoring"

    evening = evening_of_obs(obs_start, timezone_cta_n)

    for directory in (lst_events, lst_monitoring, array_triggers, array_monitoring):
        date_path = directory / f"{evening:%Y/%m/%d}"
        date_path.mkdir(exist_ok=True, parents=True)

    return dl0


@pytest.fixture(scope="session")
def dummy_dl0(dl0_base):
    trigger_dir = dl0_base / "array" / acada_user / "acada-adh/triggers/2023/08/01/"
    lst_event_dir = dl0_base / "TEL001" / acada_user / "acada-adh/events/2023/08/01/"
    subarray_id = 1
    sb_id = 123
    obs_id = 456
    sb_creator_id = 1
    sdh_ids = (1, 2, 3, 4)

    obs_start_path_string = f"{obs_start.to_datetime(timezone.utc):%Y%m%dT%H%M%S}"
    filename = f"SUB{subarray_id:03d}_SWAT001_{obs_start_path_string}_SBID{sb_id:019d}_OBSID{obs_id:019d}_SUBARRAY_CHUNK000.fits.fz"  # noqa
    # sdh_id and chunk_id will be filled later -> double {{}}
    lst_event_pattern = f"TEL001_SDH{{sdh_id:03d}}_{obs_start_path_string}_SBID{sb_id:019d}_OBSID{obs_id:019d}_TEL_SHOWER_CHUNK{{chunk_id:03d}}.fits.fz"  # noqa
    trigger_path = trigger_dir / filename

    # subarray_data_stream = DL0_Subarray.DataStream(
    #     subarray_id=subarray_id,
    #     sb_id=sb_id,
    #     obs_id=obs_id,
    #     producer_id=1  # FIXME: what is correct here?,
    #     sb_creator_id=sb_creator_id,
    # )

    lst_data_stream = DL0_Telescope.DataStream(
        tel_id=1,
        sb_id=sb_id,
        obs_id=obs_id,
        waveform_scale=80.0,
        waveform_offset=5.0,
        sb_creator_id=sb_creator_id,
    )
    camera_configuration = DL0_Telescope.CameraConfiguration(
        tel_id=1,
        local_run_id=789,
        config_time_s=obs_start.unix,
        camera_config_id=47,
        pixel_id_map=to_anyarray(np.arange(1855)),
        module_id_map=to_anyarray(np.arange(265)),
        num_pixels=1855,
        num_channels=2,
        num_samples_nominal=40,
        num_samples_long=0,
        num_modules=265,
        sampling_frequncy=1024,
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
        # trigger_file.move_to_new_table("DataStream")
        # trigger_file.write_message(subarray_data_stream)
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
                    trigger_ids=to_anyarray(np.array([event_id])),
                    tel_ids=to_anyarray(np.array([1])),
                )
            )

            sdh_id = sdh_ids[i % len(sdh_ids)]
            # TODO: randomize event to test actually parsing it

            # TODO: fill actual signal into waveform, not just 0
            waveform = np.zeros((2, 1855, 40), dtype=np.float32)

            lst_event_files[sdh_id].write_message(
                DL0_Telescope.Event(
                    event_id=event_id,
                    tel_id=camera_configuration.tel_id,
                    event_type=EventType.SUBARRAY.value,
                    event_time_s=int(time_s),
                    event_time_qns=int(time_qns),
                    # identified as signal, low gain stored, high gain stored
                    pixel_status=to_anyarray(np.full(1855, 0b00001101, dtype=np.uint8)),
                    waveform=to_anyarray(convert_waveform(waveform)),
                    num_channels=2,
                    num_samples=40,
                    num_pixels_survived=1855,
                )
            )
            events_written[sdh_id] += 1
            if events_written[sdh_id] >= chunksize:
                open_next_event_file(sdh_id)

            time = time + 0.001 * u.s

    return trigger_path


@pytest.fixture(scope="session")
def dummy_tel_file(dummy_dl0, dl0_base):
    name = "TEL001_SDH001_20230802T021531_SBID0000000000000000123_OBSID0000000000000000456_TEL_SHOWER_CHUNK000.fits.fz"  # noqa
    return dl0_base / "TEL001/ctao-acada-n/acada-adh/events/2023/08/01/" / name
