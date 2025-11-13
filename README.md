# Lepus

Using RabbitMQ with Python in a simplified way.

Lepus is a Python library designed to streamline integration with RabbitMQ, a robust and widely-used messaging system. The name "Lepus" pays homage to the constellation of the hare (Lepus), which is one of the many constellations that dot the night sky. Similarly, Lepus simplifies communication between your application's components, allowing them to efficiently and reliably exchange information without the complexity of managing RabbitMQ's low-level details.

## Why Lepus?

RabbitMQ is a popular choice for implementing message systems due to its reliability, scalability, and support for various communication protocols. However, dealing directly with RabbitMQ using Pika, the official Python library for RabbitMQ interaction, can be a challenging task. Lepus was created with the aim of simplifying this process, making it more accessible for developers who want to focus on their application's business logic rather than worrying about low-level details.

## Getting Started

To start using Lepus in your project, follow these simple steps:

1. Install Lepus using pip:

   ```
   pip install lepus
   ```
2. Import the library into your Python code:

   ```python
   from lepus import Rabbit
   ```
3. Declare queues and exchanges, configure message handling, and start efficiently exchanging information with RabbitMQ.

   ```python
   from lepus import configure, publish, listener, start_consuming

   # Configure once (global singleton). You can pass a path to a JSON file
   # or override values directly. If host == "memory" an in-memory broker is used.
   configure('config.json')  # or configure(None, host="memory", queues=[{"name": "my-queue"}])

   @listener('my-queue')
   def callback(message):  # message is auto JSON-decoded if possible
      print(f" [x] Received {message}")

   publish({"hello": "world"}, queue='my-queue')  # dicts auto serialize to JSON
   start_consuming()  # runs consumer loop in a background thread by default
   ```

### Direct Class Usage

If you prefer explicit instances over the global helpers:

```python
from lepus import Rabbit
rabbit = Rabbit('config.json')

@rabbit.listener('my-queue')
def on_msg(msg):
   print(msg)

rabbit.publish('Hello!', routing_key='my-queue')
rabbit.start_consuming()  # thread by default
```

Lepus provides a smooth and effective development experience for RabbitMQ integration, enabling you to make the most of the power of this powerful messaging tool.

## Contribution

Lepus is an open-source project, and we encourage contributions from the community. Feel free to open issues, submit pull requests, or help improve the documentation. Together, we can make Lepus even better.

## Documentation

As mentioned above, almost all configuration must be in a JSON file. This configuration will be used when instantiating the `Rabbit` object in the example above (in our example, `config.json`). Here is the list of settings:
Certainly, here is the first table in English:

### Root properties

| Property                       | Description                                                            |
| ------------------------------ | ---------------------------------------------------------------------- |
| `host`                       | The host address for the RabbitMQ connection.                          |
| `port`                       | The RabbitMQ host port for the connection.                             |
| `blocked_connection_timeout` | The timeout for blocked connections.                                   |
| `channel_max`                | The maximum number of allowed communication channels.                  |
| `client_properties`          | RabbitMQ client properties.                                            |
| `connection_attempts`        | The number of connection attempts allowed.                             |
| `frame_max`                  | The maximum frame size for communication.                              |
| `heartbeat`                  | The timeout for maintaining the heartbeat connection.                  |
| `locale`                     | The locale for communication with RabbitMQ.                            |
| `retry_delay`                | The delay between connection retry attempts.                           |
| `socket_timeout`             | The timeout for socket operations.                                     |
| `stack_timeout`              | The timeout for communication stack operations.                        |
| `virtual_host`               | The virtual host for the RabbitMQ connection.                          |
| `queues`                     | List of queues (See details in the lists below) |
| `exchanges`                   | List of exchanges (See details in the lists below) |

### Queue Properties

| Property         | Description                                            |
|------------------|--------------------------------------------------------|
| `name`           | The name of the queue.                                 |
| `passive`        | Whether the queue is passive (default: False).        |
| `durable`        | Whether the queue is durable (default: False).        |
| `exclusive`      | Whether the queue is exclusive (default: False).      |
| `auto_delete`    | Whether the queue is auto-deleted (default: False).  |
| `arguments`      | Additional arguments for the queue (default: None).  |

These properties define the characteristics and behavior of a RabbitMQ queue.

### Exchange Properties

| Property         | Description                                            |
|------------------|--------------------------------------------------------|
| `name`           | The name of the exchange.                              |
| `type`           | The type of the exchange (default: 'fanout').         |
| `passive`        | Whether the exchange is passive (default: False).    |
| `durable`        | Whether the exchange is durable (default: False).    |
| `auto_delete`    | Whether the exchange is auto-deleted (default: False).|
| `internal`       | Whether the exchange is internal (default: False).   |
| `arguments`      | Additional arguments for the exchange (default: None).|

### Credentials Variables

We have two crucial properties, username and password, are sourced from environment variables. These environment variables play a pivotal role in establishing secure authentication with RabbitMQ. Here is a brief explanation of each, along with a list:

| Environment Variable | Description                                                            |
| -------------------- | ---------------------------------------------------------------------- |
| `RABBIT_USERNAME`  | The user identifier for authentication with RabbitMQ.                  |
| `RABBIT_PASSWORD`  | The secret passphrase associated with `username` for authentication. |

By default: guest / guest

### Test Mode (In-Memory Broker)

For unit tests you can avoid a real RabbitMQ instance (and Docker) by configuring Lepus with `host="memory"`:

```python
from lepus import configure, publish, listener

configure(None, host="memory", queues=[{"name": "q"}])

@listener('q')
def handle(msg):
   assert isinstance(msg, dict)

publish({"x": 1}, queue='q')  # delivered synchronously
```

This uses an in-memory queue simulation sufficient for typical unit tests (publish / fan-out / JSON encoding). Integration tests can still target a real RabbitMQ server by pointing `host` at your broker.

### CI

GitHub Actions workflow `.github/workflows/tests.yml` runs the test suite (`pytest`) on pull requests and pushes to `main`.

## License

Lepus is distributed under the [GNU General Public Licience](https://www.gnu.org/licenses/gpl-3.0.html). Please read the LICENSE file for details on the license terms.

## Contact

If you have any questions, suggestions, or need assistance, don't hesitate to reach out to us at [Marcos Stefani Rosa](mailto:elaradevsolutions@gmail.com) or visit our [GitHub page](https://github.com/ElaraDevSolutions) for more information.

If you want to collaborate so that we can continue to have innovative ideas and more time to invest in these projects, contribute to our [Patreon](https://www.patreon.com/ElaraSolutions).
