from typing import Optional

from ops.charm import CharmEvents, RelationChangedEvent, RelationEvent
from ops.framework import EventSource

from lib.charms.data_platform_libs.v0.resource_provides import BaseResourceAccessEvent, TlsEnabledRelation, \
    BaseResourceProvides


class KafkaEvent(RelationEvent):
    @property
    def topic(self) -> Optional[str]:
        """Returns the topic that was requested."""
        return self.relation.data[self.relation.app].get("topic")


class TopicCreatedEvent(KafkaEvent, BaseResourceAccessEvent):
    """Event generated when a kafka topic is created"""


class KafkaEvents(CharmEvents):
    topic_created = EventSource(TopicCreatedEvent)


class KafkaProvides(BaseResourceProvides, TlsEnabledRelation):
    """Provides-side of the database relation."""

    on = KafkaEvents()

    def _on_relation_changed(self, event: RelationChangedEvent) -> None:
        """Event emitted when the database relation has changed."""
        # Only the leader should handle this event.
        if not self.local_unit.is_leader():
            return

        # Check which data has changed to emit customs events.
        diff = self._diff(event)

        # Emit a database requested event if the setup key (database name and optional
        # extra user roles) was added to the relation databag by the application.
        if "topic" in diff.added:
            self.topic_created.emit(event.relation, app=event.app, unit=event.unit)
