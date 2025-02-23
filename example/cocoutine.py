import asyncio


async def my_coroutine():
    print("코루틴 시작")
    await asyncio.sleep(1)
    print("코루틴 종료")


async def main():
    await my_coroutine()  # 단순 코루틴 실행


asyncio.run(main())
