import asyncio


async def producer(queue):
    for i in range(3):
        print(f"🚀 [Producer] {i}을(를) 생성 중...")
        await queue.put(i)  # 큐에 데이터 추가
        print(f"✅ [Producer] {i} 추가 완료")
        await asyncio.sleep(1)  # 다음 데이터 생성까지 대기


async def consumer(queue):
    while True:
        print("⏳ [Consumer] 데이터 대기 중...")
        item = await queue.get()  # 큐에서 데이터 가져오기 (비어있으면 대기)
        print(f"🔄 [Consumer] {item} 처리 중...")
        await asyncio.sleep(2)  # 데이터 처리 시간
        queue.task_done()
        print(f"✅ [Consumer] {item} 처리 완료")


async def main():
    queue = asyncio.Queue(maxsize=2)  # 큐 크기 제한

    producer_task = asyncio.create_task(producer(queue))
    consumer_task = asyncio.create_task(consumer(queue))

    await producer_task  # 생산자 종료 대기
    await queue.join()  # 모든 데이터가 처리될 때까지 대기

    consumer_task.cancel()  # 소비자 종료


if __name__ == "__main__":
    asyncio.run(main())
