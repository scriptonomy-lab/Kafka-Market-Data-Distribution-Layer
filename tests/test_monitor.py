"""Unit tests for Monitoring's assignment tracking (pipeline/monitor.py):
how a group's members map to instances, and how a rebalance is described."""

from pipeline.common import GroupSample, Settings
from pipeline.monitor import (
    assignment_of,
    compact_partitions,
    data_partitions,
    diff_assignments,
    instance_of,
)

FX, CR = "marketdata.fx.quotes.v1", "marketdata.crypto.quotes.v1"


def member(client_id, parts, member_id="m"):
    return {
        "member_id": member_id,
        "client_id": client_id,
        "host": "/10.0.0.2",
        "partitions": parts,
    }


def test_instance_is_the_client_id_after_the_group_name():
    assert instance_of("storage-svc-8eb2fb65e6cb", "storage-svc") == "8eb2fb65e6cb"
    assert instance_of("someone-else", "storage-svc") == "someone-else"


def test_assignment_of_maps_instances_to_their_partitions():
    s = GroupSample(
        "storage-svc",
        "STABLE",
        2,
        {},
        {},
        [member("storage-svc-a", [[FX, 1], [FX, 2]]), member("storage-svc-b", [[CR, 0]])],
    )
    assert assignment_of(s) == {"a": ((FX, 1), (FX, 2)), "b": ((CR, 0),)}


def test_join_moves_only_the_handed_over_partitions():
    before = {"a": ((FX, 1), (FX, 2), (CR, 0)), "b": ((FX, 5),)}
    after = {"a": ((FX, 1), (FX, 2)), "b": ((FX, 5),), "c": ((CR, 0),)}
    d = diff_assignments(before, after)
    assert d["joined"] == ["c"] and d["left"] == []
    assert d["moved"] == [{"topic": CR, "partition": 0, "from": "a", "to": "c"}]


def test_leave_and_unassigned_partitions_are_reported():
    before = {"a": ((FX, 1),), "b": ((FX, 2),)}
    during = {"a": ((FX, 1),)}  # b gone, its partition not yet reassigned
    d = diff_assignments(before, during)
    assert d["left"] == ["b"]
    assert d["moved"] == [{"topic": FX, "partition": 2, "from": "b", "to": None}]


def test_identical_assignments_produce_no_change():
    a = {"a": ((FX, 1),)}
    assert diff_assignments(a, dict(a)) == {"joined": [], "left": [], "moved": []}


def test_partitions_with_data_are_starred():
    topology = Settings.from_env().topology
    with_data = data_partitions(topology)
    assert (FX, 5) in with_data and (FX, 0) not in with_data  # EURUSD -> 5
    text = compact_partitions(((FX, 0), (FX, 5), (CR, 3)), {FX: "FX", CR: "Crypto"}, with_data)
    assert text == "Crypto 3* · FX 0,5*"  # topics sorted by name
