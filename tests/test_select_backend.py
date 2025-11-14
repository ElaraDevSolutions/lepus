import time
import threading
import pytest

from lepus import configure, get_instance, publish


class DummyIOLoop:
    def __init__(self):
        self._stopped = threading.Event()
    def start(self):
        # Simulate short-lived loop processing then exit
        time.sleep(0.01)
    def stop(self):
        self._stopped.set()
    def call_later(self, delay, cb):
        # Immediately invoke for test predictability
        cb()


class DummyChannel:
    def __init__(self, state):
        self.state = state
        self.published = []
    def queue_declare(self, **kwargs):
        pass
    def exchange_declare(self, **kwargs):
        pass
    def basic_consume(self, queue, on_message_callback, auto_ack):
        # Immediately simulate one message arrival if queued
        if self.state.get('inject_message'):
            body = b'{"hello":"world"}'
            on_message_callback(self, type('M', (), {'delivery_tag':1}), None, body)
    def basic_ack(self, tag):
        pass
    def basic_publish(self, exchange, routing_key, body):
        self.published.append((exchange, routing_key, body))
        return True


class DummySelectConnection:
    def __init__(self, params, on_open_callback, on_open_error_callback, on_close_callback):
        self.ioloop = DummyIOLoop()
        self._on_open_callback = on_open_callback
        self._closed = False
        self.state = {'inject_message': False}
        # Immediately invoke connection opened callback to simulate async success
        on_open_callback(self)
    def channel(self, on_open_callback):
        ch = DummyChannel(self.state)
        on_open_callback(ch)


def test_select_backend_initialization(monkeypatch):
    import lepus.connection as lc
    monkeypatch.setattr(lc, '_PikaSelectConnection', DummySelectConnection)
    # Configure with select backend
    configure(None, host='memory', queues=[{"name":"sq"}], backend='select', eager=True)
    rabbit = get_instance()
    # In memory mode select backend bypasses network; publish should deliver synchronously
    received = []

    @rabbit.listener('sq')
    def h(msg):
        received.append(msg)

    publish({"x":1}, queue='sq')
    assert received == [{"x":1}]


def test_select_backend_publish_queue(monkeypatch):
    import lepus.connection as lc
    # Force non-memory to exercise publish queue draining
    monkeypatch.setattr(lc, '_PikaSelectConnection', DummySelectConnection)
    # Use dummy host; we won't open real sockets
    rabbit = lc.Rabbit(None, host='dummy', queues=[{"name":"qsel"}], backend='select', eager=True)
    # Wait small time for thread start
    time.sleep(0.02)
    rabbit.publish({"n":2}, routing_key='qsel')
    # Allow drain
    time.sleep(0.05)
    # Access internal channel published list
    ch = getattr(rabbit, '_select_channel', None)
    assert ch is not None
    assert any(rk=='qsel' for (_, rk, _) in ch.published)
