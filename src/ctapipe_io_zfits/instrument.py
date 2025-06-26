"""Definitionas of the instrument configuration."""

import json
from functools import cache
from importlib.resources import as_file, files

import astropy.units as u
import numpy as np
from astropy.coordinates import EarthLocation
from astropy.table import QTable
from ctapipe.coordinates import CameraFrame, GroundFrame
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
    """Get a mapping of subarray_id to subarray definition."""
    data = _load_subarrays()
    return {subarray["id"]: subarray for subarray in data["subarrays"]}


@cache
def get_array_elements_by_id():
    """Get a mapping of ae_id to array element definition."""
    data = _load_array_elements()
    return {ae["id"]: ae for ae in data["array_elements"]}


@cache
def get_array_element_ids(subarray_id: int) -> tuple[int]:
    """Get array element ids for a given subarray_id."""
    subarray = get_subarrays_by_id().get(subarray_id)
    if subarray_id is None:
        raise ValueError(f"Unknown subarray_id: {subarray_id}")

    return tuple(subarray["array_element_ids"])


@cache
def get_array_element_positions(site):
    with as_file(RESOURCES / f"array_element_positions_{site.lower()}.ecsv") as path:
        positions = QTable.read(path)
        Provenance().add_input_file(path, "array element positions")
    positions.add_index("ae_id")
    return positions


def get_tel_positions(tel_ids, positions, reference_location):
    """Get telescope positions in GroundFrame for given tel_ids."""
    tel_positions = positions.loc[np.array(tel_ids)]
    locations = EarthLocation(
        x=tel_positions["x"],
        y=tel_positions["y"],
        z=tel_positions["z"],
    )

    ground_frame = GroundFrame.from_earth_location(
        locations,
        reference_location=reference_location,
    )
    coords = np.atleast_2d(ground_frame.cartesian.xyz.T)
    return {tel_id: coord for tel_id, coord in zip(tel_ids, coords, strict=True)}


def get_reference_locations(positions):
    """Get the reference position as EarthLocation."""
    return EarthLocation(
        x=u.Quantity(positions.meta["reference_x"]),
        y=u.Quantity(positions.meta["reference_y"]),
        z=u.Quantity(positions.meta["reference_z"]),
    )


def build_subarray_description(subarray_id):
    """Create a SubarrayDescription from the subarray_id."""
    try:
        subarray = get_subarrays_by_id()[subarray_id]
    except KeyError:
        raise ValueError(f"Unknown subarray_id: {subarray_id}") from None

    site = subarray["site"]
    tel_ids = get_array_element_ids(subarray_id)
    array_elements = get_array_elements_by_id()
    positions = get_array_element_positions(site)
    reference_location = get_reference_locations(positions)
    tel_positions = get_tel_positions(tel_ids, positions, reference_location)

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
        tel_positions=tel_positions,
        reference_location=reference_location,
    )
