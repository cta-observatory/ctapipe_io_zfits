from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from astropy.time import Time
import astropy.units as u
import numpy as np
import pytest
from protozfits import ProtobufZOFits
from protozfits.CTA_DL0_Subarray_pb2 import DataStream, Event
from protozfits.CoreMessages_pb2 import AnyArray

from ctapipe_io_zfits.time import time_to_cta_high_res

ANY_ARRAY_TYPE_TO_NUMPY_TYPE = {
    1: np.int8,
    2: np.uint8,
    3: np.int16,
    4: np.uint16,
    5: np.int32,
    6: np.uint32,
    7: np.int64,
    8: np.uint64,
    9: np.float32,
    10: np.float64,
}

DTYPE_TO_ANYARRAY_TYPE = {v: k for k, v in ANY_ARRAY_TYPE_TO_NUMPY_TYPE.items()}

acada_user = "ctao-acada-n"
obs_start = Time("2023-08-02T02:15:31")
timezone_canary = ZoneInfo("Atlantic/Canary")

array_elements = {
    1: "LSTN-01",
}


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

    lst_base = dl0 / array_elements[1] / acada_user / "acada-adh"
    lst_events = lst_base / "events"
    lst_monitoring = lst_base / "monitoring"
    array_triggers = dl0 / "array" / acada_user / "acada-adh" / "triggers"
    array_monitoring = dl0 / "array" / acada_user / "acada-adh" / "monitoring"

    evening = evening_of_obs(obs_start, timezone_canary)

    for directory in (lst_events, lst_monitoring, array_triggers, array_monitoring):
        date_path = directory / f"{evening:%Y/%m/%d}"
        date_path.mkdir(exist_ok=True, parents=True)

    return dl0


@pytest.fixture(scope="session")
def dummy_dl0(dl0_base):
    trigger_dir = dl0_base / "array" / acada_user / "acada-adh/triggers/2023/08/01/" 
    subarray_id = 1
    sb_id = 123
    obs_id = 456
    producer_id = 1 # what is this?
    sb_creator_id = 1

    filename = f"SUB{subarray_id:03d}_SWAT001_{obs_start.to_datetime(timezone.utc):%Y%m%dT%H%M%S}_SBID{sb_id:019d}_OBSID{obs_id:019d}_SUBARRAY_CHUNK000.fits.fz"
    path = trigger_dir / filename


    data_stream = DataStream(
        subarray_id=subarray_id,
        sb_id=sb_id,
        obs_id=obs_id,
        producer_id=producer_id,
        sb_creator_id=sb_creator_id,
    )

    time = obs_start

    with ProtobufZOFits(n_tiles=5, rows_per_tile=20, compression_block_size_kb=64 * 1024) as trigger_file:
        trigger_file.open(str(path))
        trigger_file.move_to_new_table("DataStream")
        trigger_file.write_message(data_stream)

        trigger_file.move_to_new_table("Events")

        for event_id in range(1, 101):
            time_s, time_qns = time_to_cta_high_res(time)
            trigger_file.write_message(Event(
                event_id=event_id,
                trigger_type=1,
                sb_id=sb_id,
                obs_id=obs_id,
                event_time_s=int(time_s),
                event_time_qns=int(time_qns),
                trigger_ids=to_anyarray(np.array([event_id])),
                tel_ids=to_anyarray(np.array([1])),
            ))

            time = time + 0.001 * u.s

    return path
