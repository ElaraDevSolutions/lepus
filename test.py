import json
import threading
from pyrabbit import Rabbit

json_filename = 'config.json'
rabbit = Rabbit(json_filename)

cv = threading.Condition()

@rabbit.listener('my-queue')
def callback(ch, method, properties, body):
    print(f" [x] Received {body}")
    with cv:
        print("[INFO] notify_all")
        cv.notify_all()

msg = json.dumps({"message": "Hello World!"})
rabbit.publish(msg, routing_key='my-queue')
rabbit.start_consuming()
with cv:
    print("[INFO] waiting to consume a message...")
    cv.wait()
    print("[INFO] consumed message, exiting")
