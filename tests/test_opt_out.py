"""Opt-out list."""

from __future__ import annotations


def test_add_remove_list(app_env):
    from services import opt_out as o

    assert o.count() == 0
    o.add(111, "user_stop")
    o.add(222, "manual")
    assert o.count() == 2
    assert o.is_opted_out(111)
    rows = o.list_all()
    assert {int(r["user_id"]) for r in rows} == {111, 222}
    assert o.remove(111)
    assert not o.is_opted_out(111)
    assert o.count() == 1
