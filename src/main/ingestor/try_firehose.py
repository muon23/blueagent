
import asyncio

from .PostIngestor import PostIngestor
from .StreamClient import StreamClient


async def consume_forever():
    client = StreamClient([PostIngestor()])
    await client.run_forever()


if __name__ == "__main__":
    asyncio.run(consume_forever())
