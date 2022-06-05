import json
import time
import asyncio
import httpx
import random
from dataclasses import dataclass
from datetime import datetime, timedelta

SYMBOLS = ['Monarch', 'Flagship', 'Russian', 'Thee', 'Species', 'Policies', 'Unique', 'Deutsch', 'Agree', 'Enable']


@dataclass
class TradeMessage:
    symbol: str
    stamp: datetime
    flipped: bool
    size: float


async def scan_for_favorable_market_conditions(queue):
    for i in range(3):
        print(f"Loop: {i}")
        r = random.randint(0, 10)
        if r >= 5:
            stamp = datetime.now()
            print(f"found favorable condition. Send for processing.")
            for i in range(3):
                symbol = f"{random.choice(SYMBOLS)}-{random.choice(SYMBOLS)}"
                trade_msg = TradeMessage(symbol, stamp, True, r)
                queue.put_nowait(trade_msg)
        await asyncio.sleep(.1)


def sign(secret, endpoint="orders", full_path="api/v2/orders/"):
    return secret+endpoint+full_path


def compact_json_dict(data):
    return json.dumps(data, separators=(',', ':'), ensure_ascii=False)


def kucoin_post(client: httpx.AsyncClient, symbol, side, size):
    url = "http://www.kucoin.com/orders"
    data = {
        'side': side,
        'symbol': symbol,
        'type': 'market',
        'size': size
    }
    headers = {
            "KC-API-TIMESTAMP": int(time.time() * 1000),
            "KC-API-SIGN": sign("asdf")
        }
    client.post(url, data=data, headers=headers, timeout=10)


async def generic_post_trade_request(q: asyncio.Queue):
    async with httpx.AsyncClient() as client:
        while True:
            msg = await q.get()
            print(f"Got {msg}. Posting {msg.symbol=}")
            r = await client.post("http://www.example.com/", data={"data": msg})
            time_to_post = (datetime.now() - msg.stamp) / timedelta(milliseconds=1)
            print(f"{msg.symbol=}: {r.status_code}. Took {time_to_post=}ms to process {msg=}")
            q.task_done()

async def main():
    queue = asyncio.Queue()
    scanning_task = asyncio.create_task(scan_for_favorable_market_conditions(queue))
    n_posting_worker = 6
    posting_tasks = [asyncio.create_task(generic_post_trade_request(queue)) for i in range(n_posting_worker)]
    await scanning_task
    await queue.join()
    for task in posting_tasks:
        task.cancel()
    await asyncio.gather(*posting_tasks, return_exceptions=True)
    print("The queues are now empty, and posting tasks have been shutdown.")


if __name__ == '__main__':
    asyncio.run(main(), debug=True)
    print("Exiting gracefully.")



