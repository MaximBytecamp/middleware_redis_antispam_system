"""Worker Redis Pub/Sub для обработки событий о новых заявках."""

import asyncio
import json
import logging
import os
from typing import Any

import redis.asyncio as redis
from redis.exceptions import RedisError


CHANNEL = "application.created"
RECONNECT_DELAY_SECONDS = 3


def process_message(data: Any) -> None:
    """Проверить событие и вывести сообщение о принятой заявке."""
    try:
        payload = json.loads(data)
    except (TypeError, json.JSONDecodeError):
        logging.warning("Пропущено некорректное событие из канала %s", CHANNEL)
        return

    if not isinstance(payload, dict):
        logging.warning("Пропущено некорректное событие из канала %s", CHANNEL)
        return

    application_id = payload.get("application_id")
    if not application_id:
        logging.warning("В событии %s отсутствует application_id", CHANNEL)
        return

    logging.info("Новая заявка %s принята в обработку", application_id)


async def run() -> None:
    """Подписываться на канал, переподключаясь после сбоев Redis."""
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    while True:
        client = redis.from_url(redis_url, decode_responses=True)
        try:
            await client.ping()
            async with client.pubsub(ignore_subscribe_messages=True) as pubsub:
                await pubsub.subscribe(CHANNEL)
                logging.info("Worker подписан на канал %s", CHANNEL)

                async for message in pubsub.listen():
                    if message.get("type") == "message":
                        process_message(message.get("data"))
        except asyncio.CancelledError:
            raise
        except RedisError as exc:
            logging.error(
                "Соединение с Redis потеряно: %s; повтор через %s с",
                exc,
                RECONNECT_DELAY_SECONDS,
            )
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)
        finally:
            await client.aclose()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logging.info("Worker остановлен")
