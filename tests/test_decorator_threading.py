import time
from lepus import configure, publish, listener, start_consuming


def test_threaded_start_consuming_no_block():
    # Use memory broker; start_consuming should be a no-op (non-blocking)
    received = []
    configure(None, host="memory", queues=[{"name": "q3"}])

    @listener("q3")
    def handle(msg):
        received.append(msg)

    start_consuming()  # memory mode => immediate return
    publish({"n": 1}, queue="q3")
    publish({"n": 2}, queue="q3")
    assert received == [{"n": 1}, {"n": 2}]


def test_multiple_listeners_fanout():
    configure(None, host="memory", queues=[{"name": "q4"}])
    a = []
    b = []

    @listener("q4")
    def handle_a(msg):
        a.append(msg)

    @listener("q4")
    def handle_b(msg):
        b.append(msg)

    publish({"x": 10}, queue="q4")
    assert a == [{"x": 10}]
    assert b == [{"x": 10}]
