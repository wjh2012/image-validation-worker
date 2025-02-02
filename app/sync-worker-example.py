import pika
import time

# RabbitMQ 연결 URL (기본값: localhost의 guest 계정)
RABBITMQ_URL = "amqp://guest:guest@localhost/"


def process_image(zip_path: str) -> str:
    """
    CPU 집약적인 이미지 처리 작업을 수행하는 함수입니다.
    실제 로직으로 대체할 수 있으며, 여기서는 시간 지연을 통해 처리 과정을 시뮬레이션합니다.

    :param zip_path: 이미지 zip 파일의 경로
    :return: 처리 결과 문자열
    """
    print(f"[Processing] 이미지 파일 처리 시작: {zip_path}")
    # 실제 이미지 처리 로직 대신 2초간 대기하여 처리 시간 시뮬레이션
    time.sleep(2)
    result = f"Processed: {zip_path}"
    print(f"[Processing] 이미지 파일 처리 완료: {zip_path}")
    return result


def callback(ch, method, properties, body):
    """
    task_queue에서 메시지를 소비할 때 호출되는 콜백 함수입니다.
    메시지를 받고 이미지 처리 작업을 수행한 후, 결과를 result_queue로 전송합니다.

    :param ch: 채널 객체
    :param method: 메시지 전달 정보
    :param properties: 메시지 속성
    :param body: 메시지 본문 (zip 파일 경로)
    """
    zip_path = body.decode()
    print(f"[Received] 작업 요청 받음: {zip_path}")

    # CPU 집약적인 이미지 처리 함수 호출
    result = process_image(zip_path)

    # 결과를 result_queue에 발행 (기본 익스체인지 사용)
    ch.basic_publish(exchange="", routing_key="result_queue", body=result.encode())
    print(f"[Sent] 처리 결과 전송: {result}")

    # 메시지 처리 완료 후 Ack 전송하여 큐에서 제거
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    """
    RabbitMQ에 연결하고, task_queue에서 메시지를 소비하여 처리하는 메인 함수입니다.
    """
    # RabbitMQ 연결 설정
    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # 작업 큐와 결과 큐 선언 (존재하지 않으면 생성, durable 옵션으로 재시작 후에도 유지)
    channel.queue_declare(queue="task_queue", durable=True)
    channel.queue_declare(queue="result_queue", durable=True)

    # 한 번에 하나의 메시지만 처리하도록 QoS 설정 (동시 처리 제한)
    channel.basic_qos(prefetch_count=1)

    # task_queue에서 메시지 소비 시작 (콜백 함수 지정)
    channel.basic_consume(queue="task_queue", on_message_callback=callback)

    print("[*] 메시지 대기 중. 종료하려면 CTRL+C를 누르세요.")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("종료 신호 감지됨. 소비 중지...")
        channel.stop_consuming()
    connection.close()


if __name__ == "__main__":
    main()
