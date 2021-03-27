"""Local In-memory event bus implementation."""
from typing import Any, Callable
from collections import defaultdict

from .base import EventBus

class MemoryEventBus(EventBus):
    """In-memory message bus implementation."""
    def __init__(self) -> None:
        super().__init__()
        self.handlers = defaultdict(list)

    def register(self, event_type: Any, handler: Callable):
        self.handlers[event_type].append(handler)

    def publish_one(self, event: Any):
        [h(event) for h in self.handlers[event.__class__]]

    def publish_batch(self, event_type: Any, events: list):
        [h(events) for h in self.handlers[event_type]]
