"""Small Redis client factory used by the FastAPI lifespan."""

from redis.asyncio import Redis


def create_redis_client(redis_url: str) -> Redis:
    """Create a text-decoding async Redis client without opening a socket yet."""

    return Redis.from_url(redis_url, decode_responses=True)


async def close_redis_client(client: Redis) -> None:
    """Close the client and its connection pool."""

    await client.aclose()
