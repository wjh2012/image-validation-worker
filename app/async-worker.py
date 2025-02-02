import pika

RABBITMQ_URL = "amqp://admin:admin@192.168.45.131"


def handle_delivery(channel, method, header, body):
    print(body)
    channel.basic_ack(delivery_tag=method.delivery_tag)


def on_queue_declared(channel, method_frame):
    channel.basic_consume(queue="test", on_message_callback=handle_delivery)


def on_channel_open(channel):
    channel.queue_declare(
        queue="test",
        durable=True,
        exclusive=False,
        auto_delete=False,
        callback=lambda method_frame: on_queue_declared(channel, method_frame),
    )


def on_open(connection):
    print("RabbitMQ 연결")
    connection.channel(on_open_callback=on_channel_open)


def on_close(connection, exception):
    print("RabbitMQ 연결 끊김")
    connection.ioloop.stop()


def main():

    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.SelectConnection(
        parameters=params, on_open_callback=on_open, on_close_callback=on_close
    )

    try:
        connection.ioloop.start()
    except KeyboardInterrupt:
        connection.close()
        connection.ioloop.start()


if __name__ == "__main__":
    main()
