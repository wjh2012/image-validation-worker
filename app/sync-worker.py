import pika

RABBITMQ_URL = "amqp://admin:admin@192.168.45.131"


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)

    ch.basic_publish(exchange="", routing_key="result_queue", body="complete".encode())
    # 메시지 처리 완료 후 Ack 전송하여 큐에서 제거
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():

    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(parameters=params)
    channel = connection.channel()

    queue_name = "test"
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue="task_queue", on_message_callback=callback)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    connection.close()


if __name__ == "__main__":
    main()
