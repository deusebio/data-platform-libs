# Copyright 2022 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Relation provider side abstraction for database relation.

This library is a uniform interface to a selection of common database
metadata, with added custom events that add convenience to database management,
and methods to set the application related data.

It can be used as the main library in a database charm to handle relations with
application charms or be extended/used as a template when creating a more complete library
(like one that also handles the database and user creation using database specific APIs).

Following an example of using the DatabaseRequestedEvent, in the context of the
database charm code:

```python
from charms.data_platform_libs.v0.database_provides import DatabaseProvides

class SampleCharm(CharmBase):

    def __init__(self, *args):
        super().__init__(*args)

        # Charm events defined in the database provides charm library.
        self.provided_database = DatabaseProvides(self, relation_name="database")
        self.framework.observe(self.provided_database.on.database_requested,
            self._on_database_requested)

        # Database generic helper
        self.database = DatabaseHelper()

    def _on_database_requested(self, event: DatabaseRequestedEvent) -> None:
        # Handle the event triggered by a new database requested in the relation

        # Retrieve the database name using the charm library.
        db_name = event.database

        # generate a new user credential
        username = self.database.generate_user()
        password = self.database.generate_password()

        # set the credentials for the relation
        self.provided_database.set_credentials(event.relation.id, username, password)

        # set other variables for the relation event.set_tls("False")
```

As shown above, the library provides a custom event (database_requested) to handle
the situation when an application charm requests a new database to be created.
It's preferred to subscribe to this event instead of relation changed event to avoid
creating a new database when other information other than a database name is
exchanged in the relation databag.
"""
import logging
from typing import Optional

from ops.charm import CharmEvents, RelationChangedEvent, RelationEvent
from ops.framework import EventSource

from lib.charms.data_platform_libs.v0.resource_provides import BaseResourceAccessEvent, BaseResourceProvides, \
    TlsEnabledRelation

# The unique Charmhub library identifier, never change it
LIBID = "8eea9ca584d84c7bb357f1946b6f34ce"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 2

logger = logging.getLogger(__name__)


class DatabaseEvent(RelationEvent):

    @property
    def database(self) -> Optional[str]:
        """Returns the database that was requested."""
        return self.relation.data[self.relation.app].get("database")


class DatabaseRequestedEvent(DatabaseEvent, BaseResourceAccessEvent):
    """Event emitted when a new database is requested for use on this relation."""


class DatabaseEvents(CharmEvents):
    """Database events.

    This class defines the events that the database can emit.
    """

    database_requested = EventSource(DatabaseRequestedEvent)


class DatabaseProvides(BaseResourceProvides, TlsEnabledRelation):
    """Provides-side of the database relation."""

    on = DatabaseEvents()

    def _on_relation_changed(self, event: RelationChangedEvent) -> None:
        """Event emitted when the database relation has changed."""
        # Only the leader should handle this event.
        if not self.local_unit.is_leader():
            return

        # Check which data has changed to emit customs events.
        diff = self._diff(event)

        # Emit a database requested event if the setup key (database name and optional
        # extra user roles) was added to the relation databag by the application.
        if "database" in diff.added:
            self.on.database_requested.emit(event.relation, app=event.app, unit=event.unit)

    def set_endpoints(self, relation_id: int, connection_strings: str) -> None:
        """Set database primary connections.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            relation_id: the identifier for a particular relation.
            connection_strings: database hosts and ports comma separated list.
        """
        self._update_relation_data(relation_id, {"endpoints": connection_strings})

    def set_read_only_endpoints(self, relation_id: int, connection_strings: str) -> None:
        """Set database replicas connection strings.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            relation_id: the identifier for a particular relation.
            connection_strings: database hosts and ports comma separated list.
        """
        self._update_relation_data(relation_id, {"read-only-endpoints": connection_strings})

    def set_replset(self, relation_id: int, replset: str) -> None:
        """Set replica set name in the application relation databag.

        MongoDB only.

        Args:
            relation_id: the identifier for a particular relation.
            replset: replica set name.
        """
        self._update_relation_data(relation_id, {"replset": replset})

    def set_uris(self, relation_id: int, uris: str) -> None:
        """Set the database connection URIs in the application relation databag.

        MongoDB, Redis, OpenSearch and Kafka only.

        Args:
            relation_id: the identifier for a particular relation.
            uris: connection URIs.
        """
        self._update_relation_data(relation_id, {"uris": uris})
