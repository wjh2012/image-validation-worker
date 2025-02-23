import asyncio


async def worker(total_steps, progress_queue):
    """작업을 실행하며 현재 진행 상태를 progress_queue에 전달"""
    for i in range(total_steps):
        await asyncio.sleep(1)  # 작업 시뮬레이션
        await progress_queue.put(i + 1)  # 진행 상태 전송
    await progress_queue.put("완료")  # 작업 완료 신호


async def main():
    total_steps = 10  # 전체 작업 개수
    progress_queue = asyncio.Queue()

    # 워커 실행 (싱글 워커)
    task = asyncio.create_task(worker(total_steps, progress_queue))

    while True:
        progress = await progress_queue.get()  # 진행 상태 가져오기
        if progress == "완료":
            print("✅ 작업 완료!")
            break
        print(f"🛠️ 작업 진행 중... {progress}/{total_steps} 단계")

    await task  # Task 완료 대기


if __name__ == "__main__":
    asyncio.run(main())
