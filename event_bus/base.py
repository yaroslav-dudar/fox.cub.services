"""Defines event bus base class.

EventBus represents generic publish subscribe pattern
"""
from typing import Any, Callable

import abc

class EventBus(metaclass=abc.ABCMeta):
    """Provides interface for EventBus classes only."""
    def register(self, event_type: Any, handler: Callable):
        """Registers handler for a given type of events.

        Args:
            handler: callable object to trigger on given event
            event_type: represents class to which a handler will be assigned
        """
        raise NotImplementedError()

    def publish_one(self, event: Any):
        """Trigger the handler(s) previously registered for this event.

        Args:
            event: input event for which handler(s) will be called
        """
        raise NotImplementedError()

    def publish_batch(self, event_type: Any, events: list):
        """Trigger the handler(s) for provided event type only.

        Args:
            event_type: call handlers with this type
            events: input events for which handler(s) will be called
        """
        raise NotImplementedError()
