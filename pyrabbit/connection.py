from pika import BlockingConnection
from pika.connection import Parameters
from pika.credentials import PlainCredentials

class Queue:
    def __init__(self, name, passive=None, durable=None, exclusive=None, auto_delete=None, arguments=None):
        self.name = name
        self.passive = passive if passive else False
        self.durable = durable if durable else False
        self.exclusive = exclusive if exclusive else False
        self.auto_delete = auto_delete if auto_delete else False
        self.arguments = arguments

class Exchange:
    def __init__(self, name, type='fanout', passive=False, durable=False, auto_delete=False, internal=False, arguments=None):
        self.name = name
        self.type = type
        self.passive = passive
        self.durable = durable
        self.auto_delete = auto_delete
        self.internal = internal
        self.arguments = arguments

class Connection:
    def __init__(self, host=None, port=None, blocked_connection_timeout=None, channel_max=None, client_properties=None, connection_attempts=None, credentials=None, frame_max=None, heartbeat=None, locale=None, retry_delay=None, socket_timeout=None, stack_timeout=None, virtual_host=None, username=None, password=None, auto_ack=None, queues=[], exchanges=[]):
        self.host = host if host else Parameters.DEFAULT_HOST
        self.port = port if port else Parameters.DEFAULT_PORT
        self.blocked_connection_timeout = blocked_connection_timeout if blocked_connection_timeout else Parameters.DEFAULT_BLOCKED_CONNECTION_TIMEOUT
        self.channel_max = channel_max if channel_max else Parameters.DEFAULT_CHANNEL_MAX
        self.client_properties = client_properties if client_properties else Parameters.DEFAULT_CLIENT_PROPERTIES
        self.connection_attempts = connection_attempts if connection_attempts else Parameters.DEFAULT_CONNECTION_ATTEMPTS
        self.frame_max = frame_max if frame_max else Parameters.DEFAULT_FRAME_MAX
        self.heartbeat = heartbeat if heartbeat else Parameters.DEFAULT_HEARTBEAT_TIMEOUT
        self.locale = locale if locale else Parameters.DEFAULT_LOCALE
        self.retry_delay = retry_delay if retry_delay else Parameters.DEFAULT_RETRY_DELAY
        self.socket_timeout = socket_timeout if socket_timeout else Parameters.DEFAULT_SOCKET_TIMEOUT
        self.stack_timeout = stack_timeout if stack_timeout else Parameters.DEFAULT_STACK_TIMEOUT
        self.virtual_host = virtual_host if virtual_host else Parameters.DEFAULT_VIRTUAL_HOST
        
        if credentials:
            self.credentials = credentials
        elif not username:
            self.credentials = Parameters.DEFAULT_CREDENTIALS
        else:
            usr = username if username else Parameters.DEFAULT_USERNAME
            pwd = password if password else Parameters.DEFAULT_PASSWORD
            self.credentials = PlainCredentials(usr, pwd)
            
        self.auto_ack = auto_ack if auto_ack else True
        
        self.connection = BlockingConnection(
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
        
        self.channel = self.connection.channel()
        
        for queue in queues:
            self.channel.queue_declare(queue=queue.name, passive=queue.passive, durable=queue.durable, exclusive=queue.exclusive, auto_delete=queue.auto_delete, arguments=queue.arguments)
        
        for exchange in exchanges:
            self.channel.exchange_declare(name=exchange.name, passive=self.passive, durable=self.durable, exclusive=self.exclusive, auto_delete=self.auto_delete, arguments=self.arguments)
    
    def listener(self, queue):
        def decorator(callback):
            def wrapper(ch, method, properties, body):
                kwargs = {}
                if 'ch' in callback.__code__.co_varnames:
                    kwargs['ch'] = ch
                if 'method' in callback.__code__.co_varnames:
                    kwargs['method'] = method
                if 'properties' in callback.__code__.co_varnames:
                    kwargs['properties'] = properties

                callback(body, **kwargs)

            self.channel.basic_consume(
                queue = queue,
                on_message_callback = wrapper,
                auto_ack = self.auto_ack
            )

        return decorator
    
    def publish(self, body, exchange='', routing_key=''):
        self.channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body)
    
    def close(self):
        self.connection.close()

