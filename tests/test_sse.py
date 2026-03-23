"""Tests for SSE broker pub/sub, heartbeat, and client cleanup."""


from src.sse.stream import SSEBroker


class TestSSEBroker:
    def test_subscribe_returns_unique_ids(self):
        broker = SSEBroker()
        sid1 = broker.subscribe()
        sid2 = broker.subscribe()
        assert sid1 != sid2
        assert broker.client_count == 2

    def test_unsubscribe_removes_client(self):
        broker = SSEBroker()
        sid = broker.subscribe()
        assert broker.client_count == 1
        broker.unsubscribe(sid)
        assert broker.client_count == 0

    def test_unsubscribe_nonexistent_is_noop(self):
        broker = SSEBroker()
        broker.unsubscribe(999)  # Should not raise
        assert broker.client_count == 0

    def test_publish_dict_sends_json(self):
        broker = SSEBroker()
        sid = broker.subscribe()
        broker.publish("test:event", {"key": "value"})

        msg = broker._subscribers[sid].get_nowait()
        assert "event: test:event" in msg
        assert '"key": "value"' in msg
        assert msg.endswith("\n\n")

    def test_publish_string_sends_as_is(self):
        broker = SSEBroker()
        sid = broker.subscribe()
        broker.publish("ping", "hello")

        msg = broker._subscribers[sid].get_nowait()
        assert "event: ping" in msg
        assert "data: hello" in msg

    def test_publish_fans_out_to_all_subscribers(self):
        broker = SSEBroker()
        sid1 = broker.subscribe()
        sid2 = broker.subscribe()
        broker.publish("evt", {"x": 1})

        msg1 = broker._subscribers[sid1].get_nowait()
        msg2 = broker._subscribers[sid2].get_nowait()
        assert msg1 == msg2

    def test_publish_drops_full_queue(self):
        broker = SSEBroker()
        broker.subscribe()  # subscribe but don't need the ID
        # Fill the queue
        for i in range(256):
            broker.publish("fill", f"msg-{i}")
        assert broker.client_count == 1

        # Next publish should drop the client
        broker.publish("overflow", "too-much")
        assert broker.client_count == 0

    def test_stream_yields_messages(self):
        broker = SSEBroker(heartbeat_interval=0.1)
        sid = broker.subscribe()
        broker.publish("test", {"data": 1})

        gen = broker.stream(sid)
        msg = next(gen)
        assert "event: test" in msg
        assert '"data": 1' in msg

    def test_stream_yields_heartbeat_on_timeout(self):
        broker = SSEBroker(heartbeat_interval=0.05)
        sid = broker.subscribe()

        gen = broker.stream(sid)
        # No messages published — should get heartbeat
        msg = next(gen)
        assert msg.startswith(": heartbeat")

    def test_stream_stops_when_unsubscribed(self):
        broker = SSEBroker(heartbeat_interval=60)
        sid = broker.subscribe()
        broker.publish("msg", "data")

        gen = broker.stream(sid)
        next(gen)  # consume the message

        # Unsubscribe externally
        broker.unsubscribe(sid)

        # Next iteration should detect KeyError and stop
        msgs = list(gen)
        assert len(msgs) == 0

    def test_client_count_property(self):
        broker = SSEBroker()
        assert broker.client_count == 0
        s1 = broker.subscribe()
        assert broker.client_count == 1
        _s2 = broker.subscribe()  # noqa: F841
        assert broker.client_count == 2
        broker.unsubscribe(s1)
        assert broker.client_count == 1
