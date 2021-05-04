"""Local In-memory event bus implementation."""
import inspect
from typing import Any, Callable
from collections import defaultdict
import asyncio

from .base import EventBus

class MemoryEventBus(EventBus):
    """In-memory message bus implementation."""
    def __init__(self) -> None:
        super().__init__()
        self.handlers = defaultdict(list)

    def register(self, event_type: Any, handler: Callable):
        """Registers handler for a particular event type."""
        self.handlers[event_type].append(handler)

    async def publish_one_async(self, event: Any):
        """Sends single event to all registered handlers."""
        for h in self.handlers[event.__class__]:
            if inspect.iscoroutinefunction(h):
                await h(event)
            else:
                h(event)


    async def publish_batch_async(self, event_type: Any, events: list):
        """Sends batch of events to all registered handlers."""
        for h in self.handlers[event_type]:
            if inspect.iscoroutinefunction(h):
                await h(events)
            else:
                h(events)
