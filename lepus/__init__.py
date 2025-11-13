"""Public interface for Lepus.

Exposes a simplified publish/subscribe API built on top of the internal
`Rabbit` class. You can either instantiate `Rabbit` explicitly or call
`configure` once and then use module-level helpers `publish` and `listener`.

Test mode: if host is set to "memory" (or configure(test_mode=True)), Lepus
uses an in-memory broker so unit tests can run without a real RabbitMQ
instance or Docker.
"""

from .connection import Rabbit

_default_instance: Rabbit | None = None

def configure(config_path: str | None = None, **overrides):
	"""Configure a global Rabbit instance.

	Parameters
	----------
	config_path: Optional path to JSON config file.
	overrides:   Keyword overrides (e.g. host="memory", queues=[...]).
	"""
	global _default_instance
	_default_instance = Rabbit(config_path, **overrides)
	return _default_instance

def get_instance() -> Rabbit:
	if _default_instance is None:
		raise RuntimeError("Lepus not configured. Call lepus.configure(path) first or instantiate Rabbit().")
	return _default_instance

def publish(body: bytes | str | dict, queue: str | None = None, *, exchange: str = '', routing_key: str | None = None):
	"""Publish a message using the configured instance.

	If `body` is a dict it will be JSON-serialized.
	If `queue` is provided and `routing_key` not specified, routing_key=queue.
	"""
	inst = get_instance()
	inst.publish(body, exchange=exchange, routing_key=routing_key or queue or '')

def listener(queue: str, auto_ack: bool = True):
	"""Decorator shortcut using the default instance."""
	inst = get_instance()
	return inst.listener(queue, auto_ack=auto_ack)

def start_consuming(in_thread: bool = True):
	inst = get_instance()
	inst.start_consuming(in_thread=in_thread)

def close():
	inst = get_instance()
	inst.close()

__all__ = [
	"Rabbit",
	"configure",
	"get_instance",
	"publish",
	"listener",
	"start_consuming",
	"close",
]