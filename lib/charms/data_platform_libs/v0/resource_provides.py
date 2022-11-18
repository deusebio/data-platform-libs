import json
from abc import ABC, abstractmethod
from collections import namedtuple
from typing import List, Optional

from ops.charm import CharmBase, RelationChangedEvent, RelationEvent, CharmEvents
from ops.framework import Object
from ops.model import Relation


########
### GENERAL



class BaseResourceAccessEvent(RelationEvent):
    """Base class for database events."""

    @property
    def extra_user_roles(self) -> Optional[str]:
        """Returns the extra user roles that were requested."""
        return self.relation.data[self.relation.app].get("extra-user-roles")


Diff = namedtuple("Diff", "added changed deleted")
Diff.__doc__ = """
A tuple for storing the diff between two data mappings.

added - keys that were added
changed - keys that still exist but have new values
deleted - key that were deleted"""


class BaseResourceProvides(Object, ABC):

    def __init__(self, charm: CharmBase, relation_name: str, events: CharmEvents) -> None:
        super().__init__(charm, relation_name)
        self.charm = charm
        self.local_app = self.charm.model.app
        self.local_unit = self.charm.unit
        self.relation_name = relation_name
        self.framework.observe(
            charm.on[relation_name].relation_changed,
            self._on_relation_changed,
        )
        self.events = events

    @property
    def on(self):
        return self.events

    @abstractmethod
    def _on_relation_changed(self, _):
        raise NotImplementedError

    def _diff(self, event: RelationChangedEvent) -> Diff:
        """Retrieves the diff of the data in the relation changed databag.

        Args:
            event: relation changed event.

        Returns:
            a Diff instance containing the added, deleted and changed
                keys from the event relation databag.
        """
        # Retrieve the old data from the data key in the application relation databag.
        old_data = json.loads(event.relation.data[self.local_app].get("data", "{}"))
        # Retrieve the new data from the event relation databag.
        new_data = {
            key: value for key, value in event.relation.data[event.app].items() if key != "data"
        }

        # These are the keys that were added to the databag and triggered this event.
        added = new_data.keys() - old_data.keys()
        # These are the keys that were removed from the databag and triggered this event.
        deleted = old_data.keys() - new_data.keys()
        # These are the keys that already existed in the databag,
        # but had their values changed.
        changed = {
            key for key in old_data.keys() & new_data.keys() if old_data[key] != new_data[key]
        }

        # TODO: evaluate the possibility of losing the diff if some error
        # happens in the charm before the diff is completely checked (DPE-412).
        # Convert the new_data to a serializable format and save it for a next diff check.
        event.relation.data[self.local_app].update({"data": json.dumps(new_data)})

        # Return the diff with all possible changes.
        return Diff(added, changed, deleted)

    def fetch_relation_data(self) -> dict:
        """Retrieves data from relation.

        This function can be used to retrieve data from a relation
        in the charm code when outside an event callback.

        Returns:
            a dict of the values stored in the relation data bag
                for all relation instances (indexed by the relation id).
        """
        data = {}
        for relation in self.relations:
            data[relation.id] = {
                key: value for key, value in relation.data[relation.app].items() if key != "data"
            }
        return data

    def _update_relation_data(self, relation_id: int, data: dict) -> None:
        """Updates a set of key-value pairs in the relation.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            relation_id: the identifier for a particular relation.
            data: dict containing the key-value pairs
                that should be updated in the relation.
        """
        if self.local_unit.is_leader():
            relation = self.charm.model.get_relation(self.relation_name, relation_id)
            relation.data[self.local_app].update(data)

    @property
    def relations(self) -> List[Relation]:
        """The list of Relation instances associated with this relation_name."""
        return list(self.charm.model.relations[self.relation_name])

    def set_version(self, relation_id: int, version: str) -> None:
        """Set the database version in the application relation databag.

        Args:
            relation_id: the identifier for a particular relation.
            version: database version.
        """
        self._update_relation_data(relation_id, {"version": version})

    def set_credentials(self, relation_id: int, username: str, password: str) -> None:
        """Set database primary connections.

        This function writes in the application data bag, therefore,
        only the leader unit can call it.

        Args:
            relation_id: the identifier for a particular relation.
            username: user that was created.
            password: password of the created user.
        """
        self._update_relation_data(
            relation_id,
            {
                "username": username,
                "password": password,
            },
        )


class TlsEnabledRelation(BaseResourceProvides, ABC):

    def set_tls(self, relation_id: int, tls: str) -> None:
        """Set whether TLS is enabled.

        Args:
            relation_id: the identifier for a particular relation.
            tls: whether tls is enabled (True or False).
        """
        self._update_relation_data(relation_id, {"tls": tls})

    def set_tls_ca(self, relation_id: int, tls_ca: str) -> None:
        """Set the TLS CA in the application relation databag.

        Args:
            relation_id: the identifier for a particular relation.
            tls_ca: TLS certification authority.
        """
        self._update_relation_data(relation_id, {"tls_ca": tls_ca})
