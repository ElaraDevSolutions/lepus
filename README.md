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
   rabbit = Rabbit(host='localhost')

   @rabbit.listener(queue='my-queue')
   def callback(ch, method, properties, body):
       print(f" [x] Received {body}")

   rabbit.publish("Hello World!", routing_key='my-queue')
   ```

Lepus provides a smooth and effective development experience for RabbitMQ integration, enabling you to make the most of the power of this powerful messaging tool.

## Contribution

Lepus is an open-source project, and we encourage contributions from the community. Feel free to open issues, submit pull requests, or help improve the documentation. Together, we can make Lepus even better.

## Documentation

Comprehensive documentation for Lepus can be found at [documentation_link](documentation_link). Be sure to check the documentation for detailed information on how to use all of Lepus's features.

## License

Lepus is distributed under the [GNU General Public Licience](https://www.gnu.org/licenses/gpl-3.0.html). Please read the LICENSE file for details on the license terms.

## Contact

If you have any questions, suggestions, or need assistance, don't hesitate to reach out to us at [Marcos Stefani Rosa](mailto:elaradevsolutions@gmail.com) or visit our [GitHub page](https://github.com/ElaraDevSolutions) for more information.

If you want to collaborate so that we can continue to have innovative ideas and more time to invest in these projects, contribute to our [Patreon](https://www.patreon.com/ElaraSolutions).
