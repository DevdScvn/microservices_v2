import asyncio
import websockets


async def test_ws(run_id=1):
    uri = f"ws://localhost:8000/runs/{run_id}/logs/ws"
    async with websockets.connect(uri) as ws:
        async for msg in ws:
            print(msg)


asyncio.run(test_ws())
