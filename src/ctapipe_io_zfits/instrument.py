import json
from functools import cache
from importlib.resources import as_file, files
from typing import Tuple

import astropy.units as u
from ctapipe.coordinates import CameraFrame
from ctapipe.core import Provenance
from ctapipe.instrument import (
    CameraDescription,
    CameraGeometry,
    CameraReadout,
    OpticsDescription,
    ReflectorShape,
    SizeType,
    SubarrayDescription,
    TelescopeDescription,
)

OPTICS = {
    "LST": OpticsDescription(
        name="LST",
        size_type=SizeType.LST,
        n_mirrors=1,
        n_mirror_tiles=198,
        reflector_shape=ReflectorShape.PARABOLIC,
        equivalent_focal_length=u.Quantity(28, u.m),
        effective_focal_length=u.Quantity(29.30565, u.m),
        mirror_area=u.Quantity(386.73, u.m**2),
    )
}


__all__ = [
    "build_subarray_description",
]

RESOURCES = files("ctapipe_io_zfits") / "resources"


def _load_json_resource(name):
    with as_file(RESOURCES / name) as path:
        with path.open("r") as f:
            return json.load(f)


@cache
def _load_subarrays():
    return _load_json_resource("subarray-ids.json")


@cache
def _load_array_elements():
    return _load_json_resource("array-element-ids.json")


@cache
def get_subarrays_by_id():
    """Get a mapping of subarray_id to subarray definition"""
    data = _load_subarrays()
    return {subarray["id"]: subarray for subarray in data["subarrays"]}


@cache
def get_array_elements_by_id():
    """Get a mapping of ae_id to array element definition"""
    data = _load_array_elements()
    return {ae["id"]: ae for ae in data["array_elements"]}


@cache
def get_array_element_ids(subarray_id: int) -> Tuple[int]:
    """Get array element ids for a given subarray_id"""
    subarray = get_subarrays_by_id().get(subarray_id)
    if subarray_id is None:
        raise ValueError(f"Unknown subarray_id: {subarray_id}")

    return tuple(subarray["array_element_ids"])


def build_subarray_description(subarray_id):
    try:
        subarray = get_subarrays_by_id()[subarray_id]
    except KeyError:
        raise ValueError(f"Unknown subarray_id: {subarray_id}")

    tel_ids = get_array_element_ids(subarray_id)
    array_elements = get_array_elements_by_id()

    telescopes = {}
    for tel_id in tel_ids:
        name = array_elements[tel_id]["name"]

        if name.startswith("LST"):
            optics = OPTICS["LST"]

            with as_file(RESOURCES / "LSTCam.camgeom.fits.gz") as path:
                Provenance().add_input_file(path, "CameraGeometry")
                geometry = CameraGeometry.from_table(path)
                geometry.frame = CameraFrame(focal_length=optics.effective_focal_length)

            with as_file(RESOURCES / "LSTCam.camreadout.fits.gz") as path:
                Provenance().add_input_file(path, "CameraReadout")
                readout = CameraReadout.from_table(path)

            camera = CameraDescription("LSTCam", geometry=geometry, readout=readout)
        else:
            raise ValueError("Only LSTs supported at the moment")

        telescopes[tel_id] = TelescopeDescription(
            name=name,
            optics=optics,
            camera=camera,
        )

    return SubarrayDescription(
        name=subarray["name"],
        tel_descriptions=telescopes,
        # FIXME: fill actual telescope positions
        tel_positions={tel_id: [0, 0, 0] * u.m for tel_id in telescopes}
        # FIXME: fill reference location
    )
