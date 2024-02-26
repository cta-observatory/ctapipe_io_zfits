from datetime import date, datetime

import pytest

from ctapipe_io_zfits.paths import FileNameInfo, parse_filename

timestamp = datetime(2023, 10, 11, 3, 1, 5)

icd_examples = [
    # 1 <sub_id><swat_id><sb_id><obs_id>SUB.fits.fz
    (
        "SUB001_SWAT001_20231011T030105_SBID0000000000000012345_OBSID0000000000000006789_SUBARRAY_CHUNK000.fits.fz",  # noqa
        FileNameInfo(
            subarray_id=1,
            data_source_id="SWAT001",
            timestamp=timestamp,
            sb_id=12345,
            obs_id=6789,
            type_="SUBARRAY",
            chunk_id=0,
            suffix=".fits.fz",
        ),
    ),
    (
        "SUB002_SWAT001_20231011T030105_SBID0000000000000012346_OBSID0000000000000006789_SUBARRAY_CHUNK001.fits.fz",  # noqa
        FileNameInfo(
            subarray_id=2,
            data_source_id="SWAT001",
            timestamp=timestamp,
            sb_id=12346,
            obs_id=6789,
            type_="SUBARRAY",
            chunk_id=1,
            suffix=".fits.fz",
        ),
    ),
    (
        "SUB003_SWAT001_20231011T030105_SBID0000000000000012347_OBSID0000000000000006789_SUBARRAY_CHUNK000.fits.fz",  # noqa
        FileNameInfo(
            subarray_id=3,
            data_source_id="SWAT001",
            timestamp=timestamp,
            sb_id=12347,
            obs_id=6789,
            type_="SUBARRAY",
            chunk_id=0,
            suffix=".fits.fz",
        ),
    ),
    # 2 <sub_id><swat_id><sb_id><obs_id>TEL.fits.fz
    (
        "SUB001_SWAT001_20231011T030105_SBID0000000000000012345_OBSID0000000000000006789_TEL_CHUNK000.fits.fz",  # noqa
        FileNameInfo(
            subarray_id=1,
            data_source_id="SWAT001",
            timestamp=timestamp,
            sb_id=12345,
            obs_id=6789,
            type_="TEL",
            chunk_id=0,
            suffix=".fits.fz",
        ),
    ),
    (
        "SUB002_SWAT001_20231011T030105_SBID0000000000000012345_OBSID0000000000000006789_TEL_CHUNK000.fits.fz",  # noqa
        FileNameInfo(
            subarray_id=2,
            data_source_id="SWAT001",
            timestamp=timestamp,
            sb_id=12345,
            obs_id=6789,
            type_="TEL",
            chunk_id=0,
            suffix=".fits.fz",
        ),
    ),
    (
        "SUB003_SWAT001_20231011T030105_SBID0000000000000012345_OBSID0000000000000006789_TEL_CHUNK000.fits.fz",  # noqa
        FileNameInfo(
            subarray_id=3,
            data_source_id="SWAT001",
            timestamp=timestamp,
            sb_id=12345,
            obs_id=6789,
            type_="TEL",
            chunk_id=0,
            suffix=".fits.fz",
        ),
    ),
    # 3 <sub_id><swat_id><sb_id><obs_id>MON.fits.fz
    (
        "SUB001_SWAT001_20231011T030105_SBID0000000000000012345_OBSID0000000000000006789_MON_CHUNK000.fits.fz",  # noqa
        FileNameInfo(
            subarray_id=1,
            data_source_id="SWAT001",
            timestamp=timestamp,
            sb_id=12345,
            obs_id=6789,
            type_="MON",
            chunk_id=0,
            suffix=".fits.fz",
        ),
    ),
    (
        "SUB002_SWAT001_20231011T030105_SBID0000000000000012346_OBSID0000000000000006790_MON_CHUNK000.fits.fz",  # noqa
        FileNameInfo(
            subarray_id=2,
            data_source_id="SWAT001",
            timestamp=timestamp,
            sb_id=12346,
            obs_id=6790,
            type_="MON",
            chunk_id=0,
            suffix=".fits.fz",
        ),
    ),
    (
        "SUB003_SWAT001_20231011T030105_SBID0000000000000012347_OBSID0000000000000006791_MON_CHUNK000.fits.fz",  # noqa
        FileNameInfo(
            subarray_id=3,
            data_source_id="SWAT001",
            timestamp=timestamp,
            sb_id=12347,
            obs_id=6791,
            type_="MON",
            chunk_id=0,
            suffix=".fits.fz",
        ),
    ),
    # 4 <tel_id><sdh_id><sb_id><obs_id>TEL_SHO.fits.fz
    (
        "TEL001_SDH001_20231011T030105_SBID0000000000000012345_OBSID0000000000000006789_TEL_SHOWER_CHUNK000.fits.fz",  # noqa
        FileNameInfo(
            ae_type="TEL",
            ae_id=1,
            data_source_id="SDH001",
            timestamp=timestamp,
            sb_id=12345,
            obs_id=6789,
            type_="TEL",
            subtype="SHOWER",
            chunk_id=0,
            suffix=".fits.fz",
        ),
    ),
    (
        "TEL001_SDH001_20231011T030105_SBID0000000000000012345_OBSID0000000000000006789_TEL_SHOWER_CHUNK001.fits.fz",  # noqa
        FileNameInfo(
            ae_type="TEL",
            ae_id=1,
            data_source_id="SDH001",
            timestamp=timestamp,
            sb_id=12345,
            obs_id=6789,
            type_="TEL",
            subtype="SHOWER",
            chunk_id=1,
            suffix=".fits.fz",
        ),
    ),
    (
        "TEL002_SDH001_20231011T030105_SBID0000000000000012345_OBSID0000000000000006789_TEL_SHOWER_CHUNK000.fits.fz",  # noqa
        FileNameInfo(
            ae_type="TEL",
            ae_id=2,
            data_source_id="SDH001",
            timestamp=timestamp,
            sb_id=12345,
            obs_id=6789,
            type_="TEL",
            subtype="SHOWER",
            chunk_id=0,
            suffix=".fits.fz",
        ),
    ),
    (
        "TEL002_SDH001_20231011T030105_SBID0000000000000012345_OBSID0000000000000006789_TEL_SHOWER_CHUNK001.fits.fz",  # noqa
        FileNameInfo(
            ae_type="TEL",
            ae_id=2,
            data_source_id="SDH001",
            timestamp=timestamp,
            sb_id=12345,
            obs_id=6789,
            type_="TEL",
            subtype="SHOWER",
            chunk_id=1,
            suffix=".fits.fz",
        ),
    ),
    # 5 <tel_id><sdh_id><sb_id><obs_id>TEL_CAL.fits.fz
    (
        "TEL001_SDH001_20231011T030105_SBID0000000000000012345_OBSID0000000000000006789_TEL_CAL_CHUNK000.fits.fz",  # noqa
        FileNameInfo(
            ae_type="TEL",
            ae_id=1,
            data_source_id="SDH001",
            timestamp=timestamp,
            sb_id=12345,
            obs_id=6789,
            type_="TEL",
            subtype="CAL",
            chunk_id=0,
            suffix=".fits.fz",
        ),
    ),
    (
        "TEL001_SDH001_20231011T030105_SBID0000000000000012345_OBSID0000000000000006789_TEL_CAL_CHUNK001.fits.fz",  # noqa
        FileNameInfo(
            ae_type="TEL",
            ae_id=1,
            data_source_id="SDH001",
            timestamp=timestamp,
            sb_id=12345,
            obs_id=6789,
            type_="TEL",
            subtype="CAL",
            chunk_id=1,
            suffix=".fits.fz",
        ),
    ),
    # 6 <tel_id><sdh_id><sb_id><obs_id>TEL_MUO.fits.fz
    (
        "TEL001_SDH001_20231011T030105_SBID0000000000000012345_OBSID0000000000000006789_TEL_MUON_CHUNK001.fits.fz",  # noqa
        FileNameInfo(
            ae_type="TEL",
            ae_id=1,
            data_source_id="SDH001",
            timestamp=timestamp,
            sb_id=12345,
            obs_id=6789,
            type_="TEL",
            subtype="MUON",
            chunk_id=1,
            suffix=".fits.fz",
        ),
    ),
    (
        "TEL002_SDH002_20231011T030105_SBID0000000000000012345_OBSID0000000000000006789_TEL_MUON_CHUNK001.fits.fz",  # noqa
        FileNameInfo(
            ae_type="TEL",
            ae_id=2,
            data_source_id="SDH002",
            timestamp=timestamp,
            sb_id=12345,
            obs_id=6789,
            type_="TEL",
            subtype="MUON",
            chunk_id=1,
            suffix=".fits.fz",
        ),
    ),
    # 7 <fram_id><sb_id><obs_id>_.fits
    (
        "AUX021_CAMERA01_20231011T030105_SBID0000000000000012345_OBSID0000000000000006789_FILE001.fits",  # noqa
        FileNameInfo(
            ae_type="AUX",
            ae_id=21,
            data_source_id="CAMERA01",
            timestamp=timestamp,
            sb_id=12345,
            obs_id=6789,
            file_id=1,
            suffix=".fits",
        ),
    ),
    (
        "AUX021_CAMERA01_20231011T030105_SBID0000000000000012345_OBSID0000000000000006789_FILE002.fits",  # noqa
        FileNameInfo(
            ae_type="AUX",
            ae_id=21,
            data_source_id="CAMERA01",
            timestamp=timestamp,
            sb_id=12345,
            obs_id=6789,
            file_id=2,
            suffix=".fits",
        ),
    ),
    (
        "AUX021_CAMERA01_20231011T030105_SBID0000000000000012345_OBSID0000000000000006789_FILE002.fits",  # noqa
        FileNameInfo(
            ae_type="AUX",
            ae_id=21,
            data_source_id="CAMERA01",
            timestamp=timestamp,
            sb_id=12345,
            obs_id=6789,
            file_id=2,
            suffix=".fits",
        ),
    ),
    # 8 <lidar_id><sb_id><obs_id>_.fits
    (
        "AUX020_LASER001_20231011T030105_SBID0000000000000012345_OBSID0000000000000006789_FILE001.fits",  # noqa
        FileNameInfo(
            ae_type="AUX",
            ae_id=20,
            data_source_id="LASER001",
            timestamp=timestamp,
            sb_id=12345,
            obs_id=6789,
            file_id=1,
            suffix=".fits",
        ),
    ),
    (
        "AUX020_LASER001_20231011T030105_SBID0000000000000012345_OBSID0000000000000006789_FILE002.fits",  # noqa
        FileNameInfo(
            ae_type="AUX",
            ae_id=20,
            data_source_id="LASER001",
            timestamp=timestamp,
            sb_id=12345,
            obs_id=6789,
            file_id=2,
            suffix=".fits",
        ),
    ),
    (
        "AUX020_LASER001_20231011T030105_SBID0000000000000012345_OBSID0000000000000006790_FILE001.fits",  # noqa
        FileNameInfo(
            ae_type="AUX",
            ae_id=20,
            data_source_id="LASER001",
            timestamp=timestamp,
            sb_id=12345,
            obs_id=6790,
            file_id=1,
            suffix=".fits",
        ),
    ),
    # 9 <ceilometer_id><sb_id><obs_id>_.nc
    (
        "AUX025_LASER001_20231011T030105_SBID0000000000000012345_OBSID0000000000000006789_FILE001.nc",  # noqa
        FileNameInfo(
            ae_type="AUX",
            ae_id=25,
            data_source_id="LASER001",
            timestamp=timestamp,
            sb_id=12345,
            obs_id=6789,
            file_id=1,
            suffix=".nc",
        ),
    ),
    (
        "AUX025_LASER001_20231011T030105_SBID0000000000000012345_OBSID0000000000000006789_FILE014.nc",  # noqa
        FileNameInfo(
            ae_type="AUX",
            ae_id=25,
            data_source_id="LASER001",
            timestamp=timestamp,
            sb_id=12345,
            obs_id=6789,
            file_id=14,
            suffix=".nc",
        ),
    ),
    # 10 <asc_id><obs_id>.fits
    (
        "AUX029_CAMERA01_20231011T030105_FILE001.fits",
        FileNameInfo(
            ae_type="AUX",
            ae_id=29,
            data_source_id="CAMERA01",
            timestamp=timestamp,
            file_id=1,
            suffix=".fits",
        ),
    ),
    (
        "AUX029_CAMERA01_20231011T030105_FILE001.fits",
        FileNameInfo(
            ae_type="AUX",
            ae_id=29,
            data_source_id="CAMERA01",
            timestamp=timestamp,
            file_id=1,
            suffix=".fits",
        ),
    ),
    # 11 <ws_id>_.fits
    (
        "AUX026_BRIDGE01_20231011_FILE001.fits",
        FileNameInfo(
            ae_type="AUX",
            ae_id=26,
            data_source_id="BRIDGE01",
            timestamp=date(2023, 10, 11),
            file_id=1,
            suffix=".fits",
        ),
    ),
    (
        "AUX026_BRIDGE01_20231012_FILE001.fits",
        FileNameInfo(
            ae_type="AUX",
            ae_id=26,
            data_source_id="BRIDGE01",
            timestamp=date(2023, 10, 12),
            file_id=1,
            suffix=".fits",
        ),
    ),
    # 12 <msp_id>_.fits
    (
        "AUX022_BRIDGE01_20231011_FILE001.fits",
        FileNameInfo(
            ae_type="AUX",
            ae_id=22,
            data_source_id="BRIDGE01",
            timestamp=date(2023, 10, 11),
            file_id=1,
            suffix=".fits",
        ),
    ),
    (
        "AUX022_BRIDGE01_20231012_FILE001.fits",
        FileNameInfo(
            ae_type="AUX",
            ae_id=22,
            data_source_id="BRIDGE01",
            timestamp=date(2023, 10, 12),
            file_id=1,
            suffix=".fits",
        ),
    ),
    # 13 <tel_id>_.fits
    (
        "TEL001_CCDCAM01_20231011T030105_FILE001.fits",
        FileNameInfo(
            ae_type="TEL",
            ae_id=1,
            data_source_id="CCDCAM01",
            timestamp=timestamp,
            file_id=1,
            suffix=".fits",
        ),
    ),
    (
        "TEL001_CCDCAM01_20231011T030105_FILE001.fits",
        FileNameInfo(
            ae_type="TEL",
            ae_id=1,
            data_source_id="CCDCAM01",
            timestamp=timestamp,
            file_id=1,
            suffix=".fits",
        ),
    ),
    # 14 <array_element_id>_.fits
    (
        "TEL001_BROKER01_20231011_FILE001.fits",
        FileNameInfo(
            ae_type="TEL",
            ae_id=1,
            data_source_id="BROKER01",
            timestamp=date(2023, 10, 11),
            file_id=1,
            suffix=".fits",
        ),
    ),
    (
        "TEL001_BROKER01_20231012_FILE001.fits",
        FileNameInfo(
            ae_type="TEL",
            ae_id=1,
            data_source_id="BROKER01",
            timestamp=date(2023, 10, 12),
            file_id=1,
            suffix=".fits",
        ),
    ),
    # 15 <array_element_id>_.fits
    (
        "AUX020_LASER001_20231011_FILE001.fits",
        FileNameInfo(
            ae_type="AUX",
            ae_id=20,
            data_source_id="LASER001",
            timestamp=date(2023, 10, 11),
            file_id=1,
            suffix=".fits",
        ),
    ),
    (
        "AUX020_LASER001_20231011_FILE002.fits",
        FileNameInfo(
            ae_type="AUX",
            ae_id=20,
            data_source_id="LASER001",
            timestamp=date(2023, 10, 11),
            file_id=2,
            suffix=".fits",
        ),
    ),
    # 16 <array_element_id>.fits
    (
        "TEL001_EVB_20231011_DRS4CORR_FILE001.fits",
        FileNameInfo(
            ae_type="TEL",
            ae_id=1,
            data_source_id="EVB",
            timestamp=date(2023, 10, 11),
            type_="DRS4CORR",
            file_id=1,
            suffix=".fits",
        ),
    ),
    (
        "TEL001_EVB_20231011_DRS4CORR_FILE002.fits",
        FileNameInfo(
            ae_type="TEL",
            ae_id=1,
            data_source_id="EVB",
            timestamp=date(2023, 10, 11),
            type_="DRS4CORR",
            file_id=2,
            suffix=".fits",
        ),
    ),
]


@pytest.mark.parametrize(("filename", "expected"), icd_examples)
def test_parse_filename(filename, expected):
    assert parse_filename(filename) == expected
