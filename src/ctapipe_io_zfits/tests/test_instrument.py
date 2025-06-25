import astropy.units as u


def test_load_subarray():
    from ctapipe_io_zfits.instrument import _load_subarrays

    subarrays = _load_subarrays()
    subarray = subarrays["subarrays"][0]
    assert subarray["id"] == 1
    assert subarray["name"] == "LST1"
    assert subarray["array_element_ids"] == [1]


def test_load_array_elements():
    from ctapipe_io_zfits.instrument import _load_array_elements

    array_elements = _load_array_elements()["array_elements"]
    lst = array_elements[0]
    assert lst["name"] == "LSTN-01"
    assert lst["id"] == 1


def test_get_array_element_ids():
    from ctapipe_io_zfits.instrument import get_array_element_ids

    assert get_array_element_ids(1) == (1,)
    assert get_array_element_ids(2) == (1, 2, 3, 4)


def test_build_subarray_description():
    from ctapipe_io_zfits.instrument import build_subarray_description

    subarray = build_subarray_description(subarray_id=1)
    assert len(subarray) == 1
    assert subarray.tel[1].name == "LSTN-01"
    assert u.isclose(subarray.reference_location.lon, -17.89 * u.deg, rtol=0.01)
    assert u.isclose(subarray.reference_location.lat, 28.76 * u.deg, rtol=0.01)

    ref_height = subarray.reference_location.height
    assert u.isclose(ref_height, 2149 * u.m, rtol=0.01)
    assert u.isclose(ref_height + subarray.positions[1][2], 2185 * u.m, rtol=0.001)
