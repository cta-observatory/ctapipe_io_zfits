def test_load_subarray():
    from ctapipe_io_zfits.instrument import load_subarrays

    subarrays = load_subarrays()
    subarray = subarrays["subarrays"][0]
    assert subarray["id"] == 1
    assert subarray["name"] == "LST1"
    assert subarray["array_element_ids"] == [1]


def test_load_array_elements():
    from ctapipe_io_zfits.instrument import load_array_elements

    array_elements = load_array_elements()["array_elements"]
    lst = array_elements[0]
    assert lst["name"] == "LSTN-01"
    assert lst["id"] == 1
