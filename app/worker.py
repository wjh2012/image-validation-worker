import time

import pika

from app.service.blank_detector import BlankDetector

RABBITMQ_URL = "amqp://admin:admin@localhost:5672"


def callback(ch, method, properties, body, blank_detector):

    time.sleep(10)
    blank_detector.is_blank_image(body)

    ch.basic_publish(exchange="", routing_key="result_queue", body="complete".encode())
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(parameters=params)
    channel = connection.channel()

    queue_name = "test"
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_qos(prefetch_count=1)

    blank_detector = BlankDetector()

    channel.basic_consume(
        queue=queue_name,
        on_message_callback=lambda ch, method, properties, body: callback(
            ch, method, properties, body, blank_detector
        ),
    )

    try:
        channel.start_consuming()
        print("consuming start...")
    except KeyboardInterrupt:
        channel.stop_consuming()
        print("consuming stop...")
    connection.close()


if __name__ == "__main__":
    main()
