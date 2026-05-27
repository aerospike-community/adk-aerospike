"""Unit tests for the schema registry — no Aerospike server required."""

from __future__ import annotations

from adk_aerospike._internal.schema import (
    BIN_REGISTRY,
    Bins,
    EVENT_FIELD_REGISTRY,
    SET_REGISTRY,
    BinName,
    EventFieldName,
    StorageSet,
)


def test_bin_registry_covers_every_bin_name():
    assert set(BIN_REGISTRY) == set(BinName)


def test_event_field_registry_covers_every_event_field():
    assert set(EVENT_FIELD_REGISTRY) == set(EventFieldName)


def test_set_registry_covers_every_storage_set():
    assert set(SET_REGISTRY) == set(StorageSet)


def test_bins_class_matches_bin_name_wire_values():
    for name in BinName:
        assert getattr(Bins, name.name) == name
        assert BIN_REGISTRY[name].name == name


def test_bins_last_update_aliases_timestamp():
    assert Bins.LAST_UPDATE == BinName.TIMESTAMP


def test_bin_registry_wire_names_are_unique():
    wire = [spec.name for spec in BIN_REGISTRY.values()]
    assert len(wire) == len(set(wire))


def test_event_field_wire_names_are_unique():
    wire = [spec.name for spec in EVENT_FIELD_REGISTRY.values()]
    assert len(wire) == len(set(wire))
