import json
from pyrabbit import Rabbit

if __name__ == "__main__":
    json_filename = 'config.json'
    rabbit = Rabbit(json_filename)

    @rabbit.listener('my-queue')
    def callback(ch, method, properties, body):
        print(f" [x] Received {body}")

rabbit.publish(json.dumps({"message": "Hello World!"}), routing_key='my-queue')
