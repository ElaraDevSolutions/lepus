import time
import pytest

from lepus import configure, publish, get_instance
from lepus.connection import AMQPConnectionError


def test_retry_success_memory():
    received = []
    attempts = {"count": 0}
    configure(None, host="memory", queues=[{"name": "r1", "max_retries": 3}])
    rabbit = get_instance()

    @rabbit.listener("r1")
    def handle(msg):
        attempts["count"] += 1
        # Fail first two attempts
        if attempts["count"] < 3:
            raise RuntimeError("fail")
        received.append(msg)

    publish({"ok": True}, queue="r1")
    assert received == [{"ok": True}]
    # Poison queue should be empty
    assert rabbit.get_memory_messages("r1.poison") == []
    assert attempts["count"] == 3  # 2 failures + 1 success


def test_retry_poison_memory():
    received = []
    configure(None, host="memory", queues=[{"name": "always-fail", "max_retries": 2}])
    rabbit = get_instance()

    @rabbit.listener("always-fail")
    def handle(msg):  # always fails
        raise RuntimeError("boom")

    publish({"msg": 1}, queue="always-fail")
    # No successful receives
    assert received == []
    # Poison queue should have the original message
    poison = rabbit.get_memory_messages("always-fail.poison")
    assert poison == [{"msg": 1}]


def test_quorum_queue_config_memory():
    configure(None, host="memory", queues=[{"name": "quorum-q", "queue_type": "quorum"}])
    rabbit = get_instance()
    qdef = next(q for q in rabbit._queue_defs if q.name == "quorum-q")
    assert qdef.durable is True
    assert qdef.arguments.get("x-queue-type") == "quorum"


def test_publisher_confirms_flag(monkeypatch):
    calls = {"n": 0, "fail": 2}

    class DummyChannel:
        def __init__(self):
            self.confirmed = False
        def confirm_delivery(self):
            self.confirmed = True
        def basic_publish(self, **kwargs):
            return True
        def add_on_return_callback(self, cb):
            pass
        def queue_declare(self, **kwargs):
            pass
        def exchange_declare(self, **kwargs):
            pass
        def basic_consume(self, **kwargs):
            pass
        def start_consuming(self):
            pass
        def stop_consuming(self):
            pass

    class DummyConn:
        def __init__(self, params):
            pass
        def channel(self):
            return DummyChannel()
        def close(self):
            pass

    def fake_blocking_connection(params):
        calls["n"] += 1
        if calls["n"] <= calls["fail"]:
            raise AMQPConnectionError("simulated")
        return DummyConn(params)

    # Patch BlockingConnection used in lepus.connection
    import lepus.connection as lc
    monkeypatch.setattr(lc, "BlockingConnection", fake_blocking_connection)

    # Small delays for speed
    rabbit = lc.Rabbit(None, host="dummy-host", reconnect={"enabled": True, "max_attempts": 5, "base_delay": 0.001, "max_delay": 0.002, "jitter": 0}, publisher_confirms=True, mandatory_publish=True, eager=True)
    # Connection should have retried 3 times (2 failures + success)
    assert calls["n"] == 3
    assert rabbit._publisher_confirms is True


def test_reconnect_backoff(monkeypatch):
    attempts = {"n": 0}

    class DummyChannel:
        def confirm_delivery(self):
            pass
        def basic_publish(self, **kwargs):
            return True
        def add_on_return_callback(self, cb):
            pass
        def queue_declare(self, **kwargs):
            pass
        def exchange_declare(self, **kwargs):
            pass
        def basic_consume(self, **kwargs):
            pass
        def start_consuming(self):
            pass
        def stop_consuming(self):
            pass

    class DummyConn:
        def __init__(self, params):
            pass
        def channel(self):
            return DummyChannel()
        def close(self):
            pass

    def fake_blocking_connection(params):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise AMQPConnectionError("temp fail")
        return DummyConn(params)

    import lepus.connection as lc
    monkeypatch.setattr(lc, "BlockingConnection", fake_blocking_connection)

    start = time.time()
    rabbit = lc.Rabbit(None, host="dummy-host", reconnect={"enabled": True, "max_attempts": 5, "base_delay": 0.001, "max_delay": 0.002, "jitter": 0}, publisher_confirms=False, eager=True)
    elapsed = time.time() - start
    assert attempts["n"] == 3  # 2 failures + 1 success
    # Backoff should keep overall time modest (< 0.1s)
    assert elapsed < 0.1
