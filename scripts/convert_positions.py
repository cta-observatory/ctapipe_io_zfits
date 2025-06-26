"""
Convert EPSG coordinates in official CTAO table to geodetic and geocentric system.

Avoids runtime dependency on proj/pyproj
"""

import json

import astropy.units as u
from astropy.table import QTable, Table
from pyproj import CRS, Transformer

positions = QTable.read("./CTAN_ArrayElements_Positions.ecsv")
with open("src/ctapipe_io_zfits/resources/array-element-ids.json") as f:
    array_elements = json.load(f)
    ae_by_name = {ae["name"]: ae["id"] for ae in array_elements["array_elements"]}

crs = CRS.from_epsg(positions.meta["EPSG"])
trafo_geodetic = Transformer.from_crs(crs, crs.geodetic_crs)

positions["lat"], positions["lon"] = trafo_geodetic.transform(
    positions["utm_east"].to_value(u.m),
    positions["utm_north"].to_value(u.m),
)
positions["lat"].unit = u.deg
positions["lon"].unit = u.deg

reference_lat, reference_lon = (
    trafo_geodetic.transform(
        u.Quantity(positions.meta["center_easting"]).to_value(u.m),
        u.Quantity(positions.meta["center_northing"]).to_value(u.m),
    )
    * u.deg
)

# https://epsg.io/4978 WGS 84 geocentric CRS
geocentric_crs = CRS.from_epsg(4978)
trafo_geocentric = Transformer.from_crs(crs, geocentric_crs)

positions["x"], positions["y"], positions["z"] = trafo_geocentric.transform(
    positions["utm_east"].to_value(u.m),
    positions["utm_north"].to_value(u.m),
    positions["alt"].to_value(u.m),
)
for col in "xyz":
    positions[col].unit = u.m


reference_height = 2147 * u.m
reference_x, reference_y, reference_z = (
    trafo_geocentric.transform(
        u.Quantity(positions.meta["center_easting"]).to_value(u.m),
        u.Quantity(positions.meta["center_northing"]).to_value(u.m),
        reference_height.to_value(u.m),
    )
    * u.m
)

positions["name"] = [
    f"{row['asset_code']}-{row['sequence_number']}" for row in positions
]
positions["ae_id"] = [ae_by_name.get(row["name"], -1) for row in positions]

# reduce precision for printing the table
for col in ("lat", "lon"):
    positions[col].info.format = ".6f"

for col in ("x", "y", "z"):
    positions[col].info.format = ".3f"

cols = ["ae_id", "name", "lon", "lat", "alt", "x", "y", "z", "utm_east", "utm_north"]
# convert to plain tale to avoid "mixin column metadata in ecsv"
table = Table(positions[cols])
table.meta["reference_lon"] = f"{reference_lon:.6f}"
table.meta["reference_lat"] = f"{reference_lat:.6f}"
table.meta["reference_height"] = f"{reference_height:.3f}"
table.meta["reference_x"] = f"{reference_x:.3f}"
table.meta["reference_y"] = f"{reference_y:.3f}"
table.meta["reference_z"] = f"{reference_z:.3f}"

table.meta["comments"] = """
Converted from https://gitlab.cta-observatory.org/cta-science/array-element-positions/-/blob/main/CTAN_ArrayElements_Positions.ecsv
using ctapipe_io_zfits/scripts/convert_positions.py
"""

table.write(
    "src/ctapipe_io_zfits/resources/array_element_positions_cta_north.ecsv",
    overwrite=True,
)
