import os, json, threading, time, queue as _queue
from typing import Callable, Any, Dict, List, Optional

from pika import BlockingConnection, ConnectionParameters
from pika.connection import Parameters
from pika.credentials import PlainCredentials
from pika.exceptions import AMQPConnectionError

def _load_env(key, default):
    try:
        return os.environ[key]
    except KeyError:
        return default

def _load_json(data, key, default):
    try:
        return data[key]
    except KeyError:
        return default

class Queue:
    def __init__(self, value):
        # Basic queue properties
        self.name = value['name']
        self.passive = _load_json(value, 'passive', False)
        self.durable = _load_json(value, 'durable', False)
        self.exclusive = _load_json(value, 'exclusive', False)
        self.auto_delete = _load_json(value, 'auto_delete', False)
        self.arguments = _load_json(value, 'arguments', {}) or {}

        # Reliability / DLX / retry pattern
        self.dead_letter_exchange = _load_json(value, 'dead_letter_exchange', None)
        self.dead_letter_routing_key = _load_json(value, 'dead_letter_routing_key', None)
        self.max_retries = _load_json(value, 'max_retries', 0)  # 0 => no retry handling
        self.retry_delay_ms = _load_json(value, 'retry_delay_ms', 0)  # For future delayed patterns
        self.poison_queue = _load_json(value, 'poison_queue', f"{self.name}.poison") if self.max_retries > 0 else None
        self.queue_type = _load_json(value, 'queue_type', None)  # 'quorum' or None

        # Inject DLX arguments if provided
        if self.dead_letter_exchange:
            # Only add if user hasn't supplied manually
            self.arguments.setdefault('x-dead-letter-exchange', self.dead_letter_exchange)
            if self.dead_letter_routing_key:
                self.arguments.setdefault('x-dead-letter-routing-key', self.dead_letter_routing_key)

        # Quorum queue support
        if self.queue_type == 'quorum':
            # Force durable for quorum semantics
            self.durable = True
            self.arguments.setdefault('x-queue-type', 'quorum')

class Exchange:
    def __init__(self, value):
        self.name = value['name']
        self.type = _load_json(value, 'type', 'fanout')
        self.passive = _load_json(value, 'passive', False)
        self.durable = _load_json(value, 'durable', False)
        self.auto_delete = _load_json(value, 'auto_delete', False)
        self.internal = _load_json(value, 'internal', False)
        self.arguments = _load_json(value, 'arguments', None)

class Rabbit:
    """High-level RabbitMQ client with optional in-memory test broker.

    Usage:
        rabbit = Rabbit('config.json')
        @rabbit.listener('queue-name')
        def on_msg(msg): ...
        rabbit.publish({'k': 'v'}, routing_key='queue-name')
        rabbit.start_consuming()  # launches a thread by default
    """

    def __init__(self, json_filename: Optional[str] = None, *, test_mode: bool = False, **overrides):
        # Load configuration JSON if provided
        data: Dict[str, Any] = {}
        if json_filename:
            try:
                with open(json_filename, 'r') as file:
                    data = json.load(file)
            except FileNotFoundError as e:
                raise FileNotFoundError(f"Config file '{json_filename}' not found") from e
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in config file '{json_filename}': {e}") from e

        # Apply overrides after file values
        data.update(overrides)

        # Core connection parameters
        self.host = data.get('host', Parameters.DEFAULT_HOST)
        self.port = data.get('port', Parameters.DEFAULT_PORT)
        self.virtual_host = data.get('virtual_host', Parameters.DEFAULT_VIRTUAL_HOST)
        self.blocked_connection_timeout = data.get('blocked_connection_timeout', Parameters.DEFAULT_BLOCKED_CONNECTION_TIMEOUT)
        self.channel_max = data.get('channel_max', Parameters.DEFAULT_CHANNEL_MAX)
        self.client_properties = data.get('client_properties', Parameters.DEFAULT_CLIENT_PROPERTIES)
        self.connection_attempts = data.get('connection_attempts', Parameters.DEFAULT_CONNECTION_ATTEMPTS)
        self.frame_max = data.get('frame_max', Parameters.DEFAULT_FRAME_MAX)
        self.heartbeat = data.get('heartbeat', Parameters.DEFAULT_HEARTBEAT_TIMEOUT)
        self.locale = data.get('locale', Parameters.DEFAULT_LOCALE)
        self.retry_delay = data.get('retry_delay', Parameters.DEFAULT_RETRY_DELAY)
        self.socket_timeout = data.get('socket_timeout', Parameters.DEFAULT_SOCKET_TIMEOUT)
        self.stack_timeout = data.get('stack_timeout', Parameters.DEFAULT_STACK_TIMEOUT)

        username = _load_env('RABBIT_USERNAME', Parameters.DEFAULT_USERNAME)
        password = _load_env('RABBIT_PASSWORD', Parameters.DEFAULT_PASSWORD)
        self.credentials = PlainCredentials(username, password)

        # Queues/exchanges definitions
        self._queue_defs = [Queue(q) for q in data.get('queues', [])]
        self._exchange_defs = [Exchange(e) for e in data.get('exchanges', [])]

        # Subscribers registry
        self._listeners = {}  # queue -> [callables]
        self._consuming_thread = None
        self._stop_event = threading.Event()

        # Reliability / publisher confirms / return handling configuration
        reconnect_cfg = data.get('reconnect', {}) or {}
        self._reconnect_enabled = reconnect_cfg.get('enabled', True)
        self._reconnect_max_attempts = reconnect_cfg.get('max_attempts', 5)
        self._reconnect_base_delay = reconnect_cfg.get('base_delay', 0.5)
        self._reconnect_max_delay = reconnect_cfg.get('max_delay', 10.0)
        self._reconnect_jitter = reconnect_cfg.get('jitter', 0.2)  # fraction of base

        self._publisher_confirms = data.get('publisher_confirms', False)
        self._mandatory_publish = data.get('mandatory_publish', False)
        self._raise_on_nack = data.get('raise_on_nack', True)
        self._raise_on_return = data.get('raise_on_return', False)

        # Internal state for reliability features
        self._returned_messages = []  # list of raw bodies returned by broker
        self._retry_counts = {}

        # Test mode decision: explicit flag or host == 'memory'
        self._test_mode = test_mode or self.host == 'memory'
        self._memory_queues = {}
        self.channel = None
        self.connection = None

        # Lazy connection until first publish/consume unless eager requested
        if data.get('eager', False):
            self._ensure_connection()

    # ---------------------- Internal setup ----------------------
    def _ensure_connection(self):
        if self._test_mode:
            # Initialize memory queues (including poison queues if needed)
            for q in self._queue_defs:
                self._memory_queues.setdefault(q.name, _queue.Queue())
                if q.poison_queue:
                    self._memory_queues.setdefault(q.poison_queue, _queue.Queue())
            return
        if self.connection and self.channel:
            return
        attempt = 0
        last_err = None
        while True:
            attempt += 1
            params = ConnectionParameters(
                host=self.host,
                port=self.port,
                virtual_host=self.virtual_host,
                blocked_connection_timeout=self.blocked_connection_timeout,
                channel_max=self.channel_max,
                client_properties=self.client_properties,
                connection_attempts=self.connection_attempts,
                frame_max=self.frame_max,
                heartbeat=self.heartbeat,
                locale=self.locale,
                retry_delay=self.retry_delay,
                socket_timeout=self.socket_timeout,
                stack_timeout=self.stack_timeout,
                credentials=self.credentials,
            )
            try:
                self.connection = BlockingConnection(params)
                break
            except AMQPConnectionError as e:
                last_err = e
                if not self._reconnect_enabled or attempt >= self._reconnect_max_attempts:
                    raise ConnectionError(f"Failed to connect to RabbitMQ at {self.host}:{self.port} after {attempt} attempts - {e}") from e
                # Exponential backoff with jitter
                delay = min(self._reconnect_base_delay * (2 ** (attempt - 1)), self._reconnect_max_delay)
                jitter = delay * self._reconnect_jitter * (0.5 - os.urandom(1)[0]/255)  # random small jitter
                time.sleep(max(0.0, delay + jitter))
        self.channel = self.connection.channel()
        if self._publisher_confirms:
            try:
                self.channel.confirm_delivery()
            except Exception:
                # Non-fatal; leave channel without confirms if unsupported
                self._publisher_confirms = False
        if self._mandatory_publish:
            # Track returns
            self.channel.add_on_return_callback(self._on_return)
        # Declare queues
        for q in self._queue_defs:
            self.channel.queue_declare(
                queue=q.name,
                passive=q.passive,
                durable=q.durable,
                exclusive=q.exclusive,
                auto_delete=q.auto_delete,
                arguments=q.arguments,
            )
            if q.poison_queue:
                # Declare poison queue (durable by default for reliability)
                self.channel.queue_declare(queue=q.poison_queue, durable=True)
        # Declare exchanges
        for ex in self._exchange_defs:
            self.channel.exchange_declare(
                exchange=ex.name,
                exchange_type=ex.type,
                passive=ex.passive,
                durable=ex.durable,
                auto_delete=ex.auto_delete,
                internal=ex.internal,
                arguments=ex.arguments,
            )

    # --------------- Publisher Returns Callback ---------------
    def _on_return(self, channel, method, properties, body):  # pragma: no cover (network dependent)
        self._returned_messages.append(body)
        if self._raise_on_return:
            raise RuntimeError(f"Message returned by broker (unroutable): {body!r}")

    # ---------------------- Decorator ----------------------
    def listener(self, queue: str, auto_ack: bool = True):
        def decorator(fn: Callable[[Any], None]):
            self._listeners.setdefault(queue, []).append(fn)
            # If real broker, register consumer now (lazy connection ok)
            def _pika_wrapper(ch, method, properties, body):
                msg = self._deserialize(body)
                queue_def = next((qd for qd in self._queue_defs if qd.name == queue), None)
                try:
                    fn(msg)
                except Exception as e:
                    if queue_def and queue_def.max_retries > 0:
                        self._handle_retry(queue_def, body)
                    # Always ack to avoid redelivery storm when we manage retries ourselves
                finally:
                    if not auto_ack:
                        try:
                            ch.basic_ack(method.delivery_tag)
                        except Exception:
                            pass
            if not self._test_mode:
                self._ensure_connection()
                self.channel.basic_consume(queue=queue, on_message_callback=_pika_wrapper, auto_ack=auto_ack)
            return fn
        return decorator

    # ---------------------- Publish ----------------------
    def publish(self, body: Any, *, exchange: str = '', routing_key: str = ''):
        self._ensure_connection()
        payload = self._serialize(body)
        if self._test_mode:
            # Memory broker: put into queue and invoke listeners immediately
            qname = routing_key
            if qname not in self._memory_queues:
                # Auto-create for convenience
                self._memory_queues[qname] = _queue.Queue()
            self._memory_queues[qname].put(payload)
            # Fan out to listeners
            if qname in self._listeners:
                # Process messages one by one allowing retry logic to republish
                while not self._memory_queues[qname].empty():
                    raw = self._memory_queues[qname].get()
                    for fn in self._listeners[qname]:
                        queue_def = next((qd for qd in self._queue_defs if qd.name == qname), None)
                        try:
                            fn(self._deserialize(raw))
                        except Exception:
                            if queue_def and queue_def.max_retries > 0:
                                self._handle_retry(queue_def, raw)
            return
        # Real broker publish
        if self._publisher_confirms:
            ok = self.channel.basic_publish(exchange=exchange, routing_key=routing_key, body=payload, mandatory=self._mandatory_publish)
            if not ok and self._raise_on_nack:
                raise RuntimeError("Publish NACK by broker (publisher confirms enabled)")
        else:
            self.channel.basic_publish(exchange=exchange, routing_key=routing_key, body=payload, mandatory=self._mandatory_publish)

    # ---------------------- Consuming ----------------------
    def start_consuming(self, in_thread: bool = True):
        if self._test_mode:
            # Nothing blocking needed; messages delivered synchronously
            return
        self._ensure_connection()
        if not in_thread:
            self.channel.start_consuming()
            return
        if self._consuming_thread and self._consuming_thread.is_alive():
            return
        def _run():
            try:
                self.channel.start_consuming()
            except Exception:
                pass
        self._consuming_thread = threading.Thread(target=_run, name="lepus-consumer", daemon=True)
        self._consuming_thread.start()

    def stop_consuming(self):
        self._stop_event.set()
        if self.channel and not self._test_mode:
            try:
                self.channel.stop_consuming()
            except Exception:
                pass

    # ---------------------- Serialization helpers ----------------------
    def _serialize(self, body: Any) -> bytes:
        if isinstance(body, bytes):
            return body
        if isinstance(body, str):
            return body.encode('utf-8')
        # Fallback to JSON
        return json.dumps(body).encode('utf-8')

    def _deserialize(self, raw: bytes) -> Any:
        try:
            text = raw.decode('utf-8')
        except UnicodeDecodeError:
            return raw
        # Try JSON, fall back to plain text
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text

    # ---------------------- Close ----------------------
    def close(self):
        if self._test_mode:
            return
        if self.connection:
            try:
                self.connection.close()
            except Exception:
                pass

    # Convenience: consume one message with timeout (test helper)
    def consume_once(self, timeout: float = 1.0):
        if self._test_mode:
            # Not applicable; messages immediate
            return None
        self._ensure_connection()
        start = time.time()
        while time.time() - start < timeout:
            self.connection.process_data_events(time_limit=0.1)
        return None

    # ---------------------- Retry helper ----------------------
    def _handle_retry(self, queue_def: Queue, raw_payload: bytes):
        key = (queue_def.name, raw_payload)
        count = self._retry_counts.get(key, 0) + 1
        self._retry_counts[key] = count
        if count > queue_def.max_retries:
            # Poison
            if self._test_mode:
                self._memory_queues[queue_def.poison_queue].put(raw_payload)
            else:
                # Publish to poison queue
                self.channel.basic_publish(exchange='', routing_key=queue_def.poison_queue, body=raw_payload)
            return
        # Requeue for another attempt
        if self._test_mode:
            self._memory_queues[queue_def.name].put(raw_payload)
        else:
            self.channel.basic_publish(exchange='', routing_key=queue_def.name, body=raw_payload)

    # ---------------------- Memory inspection (tests) ----------------------
    def get_memory_messages(self, queue_name: str) -> List[Any]:
        if not self._test_mode:
            raise RuntimeError("Only available in memory test mode")
        q = self._memory_queues.get(queue_name)
        if not q:
            return []
        # Non destructive copy
        items = list(q.queue)
        return [self._deserialize(x) for x in items]
