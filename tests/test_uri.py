"""Unit tests for URI parsing — no server required."""

from __future__ import annotations

import pytest

from adk_aerospike._internal.uri import parse


def test_basic():
    u = parse("aerospike://localhost:3000/adk")
    assert u.scheme == "aerospike"
    assert u.hosts == (("localhost", 3000),)
    assert u.namespace == "adk"
    assert u.set_prefix == "adk_"
    assert u.tls is False


def test_default_port():
    u = parse("aerospike://host/ns")
    assert u.hosts == (("host", 3000),)


def test_multi_host():
    u = parse("aerospike://a:3000,b:3000,c:3001/ns")
    assert u.hosts == (("a", 3000), ("b", 3000), ("c", 3001))


def test_auth_and_query():
    u = parse("aerospike://u:p@host:3000/ns?set_prefix=prod_&tls=true")
    assert u.username == "u"
    assert u.password == "p"
    assert u.set_prefix == "prod_"
    assert u.tls is True


def test_missing_namespace():
    with pytest.raises(ValueError, match="namespace"):
        parse("aerospike://host:3000/")


def test_bad_scheme():
    with pytest.raises(ValueError, match="scheme"):
        parse("redis://host:3000/ns")
