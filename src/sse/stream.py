"""Thread-safe SSE broker with pub/sub and heartbeat."""

import json
import logging
import queue
import threading
import time

logger = logging.getLogger(__name__)


class SSEBroker:
    """Thread-safe publish/subscribe broker for Server-Sent Events.

    Each subscriber gets their own queue. publish() fans out to all.
    stream() yields SSE-formatted data with periodic heartbeats.
    """

    def __init__(self, heartbeat_interval: float = 30.0) -> None:
        self._subscribers: dict[int, queue.Queue] = {}
        self._lock = threading.Lock()
        self._counter = 0
        self.heartbeat_interval = heartbeat_interval

    def subscribe(self) -> int:
        """Register a new client. Returns subscriber ID."""
        with self._lock:
            self._counter += 1
            sid = self._counter
            self._subscribers[sid] = queue.Queue(maxsize=256)
            logger.debug(f"SSE client {sid} subscribed ({len(self._subscribers)} total)")
            return sid

    def unsubscribe(self, sid: int) -> None:
        """Remove a client."""
        with self._lock:
            self._subscribers.pop(sid, None)
            logger.debug(f"SSE client {sid} unsubscribed ({len(self._subscribers)} total)")

    def publish(self, event: str, data: dict | str) -> None:
        """Fan-out an event to all subscribers."""
        if isinstance(data, dict):
            data = json.dumps(data)
        message = f"event: {event}\ndata: {data}\n\n"

        with self._lock:
            dead = []
            for sid, q in self._subscribers.items():
                try:
                    q.put_nowait(message)
                except queue.Full:
                    dead.append(sid)
            for sid in dead:
                self._subscribers.pop(sid, None)
                logger.debug(f"SSE client {sid} dropped (queue full)")

    @property
    def client_count(self) -> int:
        with self._lock:
            return len(self._subscribers)

    def stream(self, sid: int):
        """Generator that yields SSE-formatted messages for a subscriber.

        Includes periodic heartbeat comments to keep the connection alive.
        """
        try:
            while True:
                try:
                    msg = self._subscribers[sid].get(timeout=self.heartbeat_interval)
                    yield msg
                except queue.Empty:
                    # Heartbeat — SSE comment to keep connection alive
                    yield f": heartbeat {int(time.time())}\n\n"
                except KeyError:
                    # Subscriber was removed
                    break
        finally:
            self.unsubscribe(sid)


# Global broker instance
sse_broker = SSEBroker()
