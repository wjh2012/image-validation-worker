import asyncio
import threading


async def my_coroutine():
    print(f"코루틴 시작 - 스레드: {threading.current_thread().name}")
    await asyncio.sleep(1)
    print(f"코루틴 종료 - 스레드: {threading.current_thread().name}")


async def main():
    task1 = asyncio.create_task(my_coroutine())
    task2 = asyncio.create_task(my_coroutine())

    print(f"메인 함수 실행 - 스레드: {threading.current_thread().name}")

    await task1
    await task2


if __name__ == "__main__":
    asyncio.run(main())
