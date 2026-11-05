"""Requires the live windrow-haxall container (localhost:8180)."""

import pytest

from windrow.haxall_client import connect, eval_axon

HAXALL_URI = "http://localhost:8180/"
HAXALL_USER = "su"
HAXALL_PASS = "windrow-demo-pass"


@pytest.fixture
def session():
    return connect(HAXALL_URI, HAXALL_USER, HAXALL_PASS)


@pytest.mark.integration
def test_connect_authenticates(session):
    result = eval_axon(session, "today()")
    assert result[0]["val"] is not None


@pytest.mark.integration
def test_eval_reads_real_points(session):
    result = eval_axon(session, "readAll(point)")
    dis_values = {row["dis"] for row in result}
    assert {"Zone Temp", "Fan Status"}.issubset(dis_values)


@pytest.mark.integration
def test_eval_raises_on_axon_error(session):
    from pyhaystack.exception import HaystackError

    with pytest.raises(HaystackError):
        eval_axon(session, "thisFunctionDoesNotExist()")
