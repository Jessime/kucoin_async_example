import asyncio


async def bug():
    raise Exception("not consumed")


async def main():
    asyncio.create_task(bug())
    print(2)

asyncio.run(main())
print(1)