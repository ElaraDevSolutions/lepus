import os, json

from pika import BlockingConnection, ConnectionParameters
from pika.connection import Parameters
from pika.credentials import PlainCredentials

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
        self.name = value['name']
        self.passive = _load_json(value, 'passive', False)
        self.durable = _load_json(value, 'durable', False)
        self.exclusive = _load_json(value, 'exclusive', False)
        self.auto_delete = _load_json(value, 'auto_delete', False)
        self.arguments = _load_json(value, 'arguments', None)

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
    def __init__(self, json_filename):
        try:
            with open(json_filename, 'r') as file:
                data = json.load(file)
                
                self.host = data.get('host', Parameters.DEFAULT_HOST)
                self.port = data.get('port', Parameters.DEFAULT_PORT)
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
                self.virtual_host = data.get('virtual_host', Parameters.DEFAULT_VIRTUAL_HOST)
                
                username = _load_env('RABBIT_USERNAME', Parameters.DEFAULT_USERNAME)
                password = _load_env('RABBIT_PASSWORD', Parameters.DEFAULT_PASSWORD)
                self.credentials = PlainCredentials(username, password)
                
                self.connection = BlockingConnection(
                    ConnectionParameters(
                        host = self.host, 
                        port = self.port, 
                        blocked_connection_timeout = self.blocked_connection_timeout, 
                        channel_max = self.channel_max, 
                        client_properties = self.client_properties, 
                        connection_attempts = self.connection_attempts, 
                        frame_max = self.frame_max, 
                        heartbeat = self.heartbeat, 
                        locale = self.locale, 
                        retry_delay = self.retry_delay, 
                        socket_timeout = self.socket_timeout, 
                        stack_timeout = self.stack_timeout, 
                        virtual_host = self.virtual_host
                    )
                )
                
                self.channel = self.connection.channel()
                
                queues = data.get('queues', [])
                for item in queues:
                    queue = Queue(item)
                    self.channel.queue_declare(
                        queue=queue.name, 
                        passive=queue.passive, 
                        durable=queue.durable, 
                        exclusive=queue.exclusive, 
                        auto_delete=queue.auto_delete, 
                        arguments=queue.arguments
                    )
                
                exchanges = data.get('exchanges', [])
                for item in exchanges:
                    exchange = Exchange(item)
                    self.channel.exchange_declare(
                        name=exchange.name, 
                        passive=self.passive, 
                        durable=self.durable, 
                        exclusive=self.exclusive, 
                        auto_delete=self.auto_delete, 
                        arguments=self.arguments
                    )
        except FileNotFoundError as e:
            print(f"File '{json_filename}' not found.")
            raise e
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            raise e
    
    def listener(self, queue, auto_ack=True):
        def listener(callback):
            def wrapper(ch, method, properties, body):
                callback(ch, method, properties, body)

            self.channel.basic_consume(
                queue = queue,
                on_message_callback = wrapper,
                auto_ack = auto_ack
            )

        return listener

    def start_consuming(self):
        self.channel.start_consuming()

    def publish(self, body, exchange='', routing_key=''):
        self.channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body)
    
    def close(self):
        self.connection.close()
