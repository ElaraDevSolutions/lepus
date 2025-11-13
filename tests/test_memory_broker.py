import pytest
from lepus import configure, publish, listener, start_consuming


def test_publish_consume_memory_broker():
    received = []
    configure(None, host="memory", queues=[{"name": "q1"}])

    @listener("q1")
    def handle(msg):
        received.append(msg)

    publish({"hello": "world"}, queue="q1")
    # Memory broker delivers synchronously
    assert received == [{"hello": "world"}]


def test_string_publish():
    received = []
    configure(None, host="memory", queues=[{"name": "q2"}])

    @listener("q2")
    def handle(msg):
        received.append(msg)

    publish("plain text", queue="q2")
    assert received == ["plain text"]
