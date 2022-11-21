# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import unittest
from abc import ABC, abstractmethod
from typing import Tuple
from unittest.mock import Mock, patch

from charms.data_platform_libs.v0.database_provides import (
    DatabaseProvides,
    DatabaseRequestedEvent,
    Diff,
    KafkaProvides,
)
from charms.harness_extensions.v0.capture_events import capture
from ops.charm import CharmBase
from ops.testing import Harness

DATABASE = "data_platform"
EXTRA_USER_ROLES = "CREATEDB,CREATEROLE"
DATABASE_RELATION_INTERFACE = "database_client"
DATABASE_RELATION_NAME = "database"
DATABASE_METADATA = f"""
name: database
provides:
  {DATABASE_RELATION_NAME}:
    interface: {DATABASE_RELATION_INTERFACE}
"""
TOPIC = ""
KAFKA_RELATION_INTERFACE = "kafka_client"
KAFKA_RELATION_NAME = "kafka"
KAFKA_METADATA = f"""
name: kafka
provides:
  {KAFKA_RELATION_NAME}:
    interface: {KAFKA_RELATION_INTERFACE}
"""


class DatabaseCharm(CharmBase):
    """Mock database charm to use in units tests."""

    def __init__(self, *args):
        super().__init__(*args)
        self.provider = DatabaseProvides(
            self,
            DATABASE_RELATION_NAME,
        )
        self.framework.observe(self.provider.on.database_requested, self._on_database_requested)

    def _on_database_requested(self, _) -> None:
        pass


class KafkaCharm(CharmBase):
    """Mock Kafka charm to use in units tests."""

    def __init__(self, *args):
        super().__init__(*args)
        self.provider = KafkaProvides(
            self,
            KAFKA_RELATION_NAME,
        )
        self.framework.observe(self.provider.on.topic_requested, self._on_topic_requested)

    def _on_topic_requested(self, _) -> None:
        pass


class DataProvidesBaseTests(ABC):

    @abstractmethod
    def get_harness(self) -> Tuple[Harness, int]:
        pass

    def setUp(self):
        self.harness, self.rel_id = self.get_harness()

    def tearDown(self) -> None:
        self.harness.cleanup()

    def test_diff(self):
        """Asserts that the charm library correctly returns a diff of the relation data."""
        # Define a mock relation changed event to be used in the subsequent diff calls.
        mock_event = Mock()
        # Set the app, id and the initial data for the relation.
        mock_event.app = self.harness.charm.model.get_app(self.app_name)
        mock_event.relation.id = self.rel_id
        mock_event.relation.data = {
            mock_event.app: {"username": "test-username", "password": "test-password"}
        }
        # Use a variable to easily update the relation changed event data during the test.
        data = mock_event.relation.data[mock_event.app]

        # Test with new data added to the relation databag.
        result = self.harness.charm.provider._diff(mock_event)
        assert result == Diff({"username", "password"}, set(), set())

        # Test with the same data.
        result = self.harness.charm.provider._diff(mock_event)
        assert result == Diff(set(), set(), set())

        # Test with changed data.
        data["username"] = "test-username-1"
        result = self.harness.charm.provider._diff(mock_event)
        assert result == Diff(set(), {"username"}, set())

        # Test with deleted data.
        del data["username"]
        del data["password"]
        result = self.harness.charm.provider._diff(mock_event)
        assert result == Diff(set(), set(), {"username", "password"})

    def test_set_credentials(self):
        """Asserts that the database name is in the relation databag when it's requested."""
        # Set the credentials in the relation using the provides charm library.
        self.harness.charm.provider.set_credentials(self.rel_id, "test-username", "test-password")

        # Check that the credentials are present in the relation.
        assert self.harness.get_relation_data(self.rel_id, self.app_name) == {
            "data": "{}",  # Data is the diff stored between multiple relation changed events.
            "username": "test-username",
            "password": "test-password",
        }


class TestDatabaseProvides(DataProvidesBaseTests, unittest.TestCase):
    charm = DatabaseCharm
    metadata = DATABASE_METADATA
    relation_name = DATABASE_RELATION_NAME
    app_name = "database"

    def get_harness(self) -> Tuple[Harness, int]:
        harness = Harness(self.charm, meta=self.metadata)
        # Set up the initial relation and hooks.
        rel_id = harness.add_relation(self.relation_name, "application")
        harness.add_relation_unit(rel_id, "application/0")
        harness.set_leader(True)
        harness.begin_with_initial_hooks()
        return harness, rel_id

    @patch.object(DatabaseCharm, "_on_database_requested")
    def emit_database_requested_event(self, _on_database_requested):
        # Emit the database requested event.
        relation = self.harness.charm.model.get_relation(DATABASE_RELATION_NAME, self.rel_id)
        application = self.harness.charm.model.get_app("database")
        self.harness.charm.provider.on.database_requested.emit(relation, application)
        return _on_database_requested.call_args[0][0]

    @patch.object(DatabaseCharm, "_on_database_requested")
    def test_on_database_requested(self, _on_database_requested):
        """Asserts that the correct hook is called when a new database is requested."""
        # Simulate the request of a new database plus extra user roles.
        self.harness.update_relation_data(
            self.rel_id,
            "application",
            {"database": DATABASE, "extra-user-roles": EXTRA_USER_ROLES},
        )

        # Assert the correct hook is called.
        _on_database_requested.assert_called_once()

        # Assert the database name and the extra user roles
        # are accessible in the providers charm library event.
        event = _on_database_requested.call_args[0][0]
        assert event.database == DATABASE
        assert event.extra_user_roles == EXTRA_USER_ROLES

    def test_set_endpoints(self):
        """Asserts that the endpoints are in the relation databag when they change."""
        # Set the endpoints in the relation using the provides charm library.
        self.harness.charm.provider.set_endpoints(self.rel_id, "host1:port,host2:port")

        # Check that the endpoints are present in the relation.
        assert (
                self.harness.get_relation_data(self.rel_id, "database")["endpoints"]
                == "host1:port,host2:port"
        )

    def test_set_read_only_endpoints(self):
        """Asserts that the read only endpoints are in the relation databag when they change."""
        # Set the endpoints in the relation using the provides charm library.
        self.harness.charm.provider.set_read_only_endpoints(self.rel_id, "host1:port,host2:port")

        # Check that the endpoints are present in the relation.
        assert (
                self.harness.get_relation_data(self.rel_id, "database")["read-only-endpoints"]
                == "host1:port,host2:port"
        )

    def test_set_additional_fields(self):
        """Asserts that the additional fields are in the relation databag when they are set."""
        # Set the additional fields in the relation using the provides charm library.
        self.harness.charm.provider.set_replset(self.rel_id, "rs0")
        self.harness.charm.provider.set_tls(self.rel_id, "True")
        self.harness.charm.provider.set_tls_ca(self.rel_id, "Canonical")
        self.harness.charm.provider.set_uris(self.rel_id, "host1:port,host2:port")
        self.harness.charm.provider.set_version(self.rel_id, "1.0")

        # Check that the additional fields are present in the relation.
        assert self.harness.get_relation_data(self.rel_id, "database") == {
            "data": "{}",  # Data is the diff stored between multiple relation changed events.
            "replset": "rs0",
            "tls": "True",
            "tls_ca": "Canonical",
            "uris": "host1:port,host2:port",
            "version": "1.0",
        }

    def test_fetch_relation_data(self):
        # Set some data in the relation.
        self.harness.update_relation_data(self.rel_id, "application", {"database": DATABASE})

        # Check the data using the charm library function
        # (the diff/data key should not be present).
        data = self.harness.charm.provider.fetch_relation_data()
        assert data == {self.rel_id: {"database": DATABASE}}

    def test_database_requested_event(self):
        # Test custom event creation

        # Test the event being emitted by the application.
        with capture(self.harness.charm, DatabaseRequestedEvent) as captured:
            self.harness.update_relation_data(self.rel_id, "application", {"database": DATABASE})
        assert captured.event.app.name == "application"

        # Reset the diff data to trigger the event again later.
        self.harness.update_relation_data(self.rel_id, "database", {"data": "{}"})

        # Test the event being emitted by the unit.
        with capture(self.harness.charm, DatabaseRequestedEvent) as captured:
            self.harness.update_relation_data(self.rel_id, "application/0", {"database": DATABASE})
        assert captured.event.unit.name == "application/0"
