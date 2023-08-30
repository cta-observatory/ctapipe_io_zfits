from datetime import datetime

import pytest

acada_user = "ctao-acada-n"
date = datetime(2023, 8, 1)


@pytest.fixture(scope="session")
def acada_base(tmp_path_factory):
    return tmp_path_factory.mkdir("acada_base")


@pytest.fixture(scope="session")
def dl0_structure(acada_base):
    dl0 = acada_base / "DL0"
    dl0.mkdir(exist_ok=True)

    return dl0
