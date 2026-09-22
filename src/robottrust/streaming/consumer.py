"""At-least-once delivery plus idempotent durable ingestion; manual commits."""
from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import json
import time
from typing import Any

from confluent_kafka import Consumer, KafkaError, TopicPartition

from robottrust.streaming.config import ConsumerConfig, TransportPosition
from robottrust.streaming.events import EventEnvelope, decode_event_json
from robottrust.streaming.producer import source_key
from robottrust.streaming.store import IngestResult, IngestionStore


class ConsumerError(RuntimeError):
    """Transport failure; stop this consumer instance before further processing."""


@dataclass(frozen=True)
class Consumed:
    position: TransportPosition
    ingestion: IngestResult
    committed_next_offset: int


class EpisodeConsumer:
    def __init__(self, config: ConsumerConfig, store: IngestionStore, *, client: Any | None = None,
                 subscribe: bool = True):
        self.config, self.store = config, store
        self._client = client if client is not None else Consumer(config.client_settings())
        self._failed = False
        self._closed = False
        if subscribe:
            try:
                self._client.subscribe([config.topic])
            except BaseException:
                self._client.close()
                raise

    def __enter__(self) -> EpisodeConsumer:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def close(self) -> None:
        if not self._closed:
            self._client.close()  # auto commit is disabled
            self._closed = True

    def _ready(self) -> None:
        if self._closed or self._failed:
            raise ConsumerError("consumer is closed or failed; create a new instance")

    def poll_once(self, timeout_s: float | None = None) -> Consumed | None:
        self._ready()
        try:
            message = self._client.poll(self.config.poll_timeout_s if timeout_s is None else timeout_s)
            return self.handle_message(message) if message is not None else None
        except BaseException:
            self._failed = True
            raise

    def handle_message(self, message: Any) -> Consumed | None:
        self._ready()
        try:
            error = message.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    return None
                raise ConsumerError(f"broker message error: {error}")
            position = TransportPosition(topic=message.topic(), partition=message.partition(), offset=message.offset())
            if position.topic != self.config.topic:
                raise ConsumerError("received a message from an unexpected topic")
            previous = self.store.latest_checkpoint(position.topic, position.partition)
            next_offset = position.offset + 1
            # Redelivery can be behind a durable checkpoint after a failed broker
            # commit. Do not rewind SQLite; still record the duplicate receipt.
            checkpoint = next_offset if previous is None or previous <= next_offset else None
            value = message.value()
            rejection = None
            if value is None:
                value, rejection = b"", "tombstone/null payload (no value bytes)"
            else:
                try:
                    event = EventEnvelope.model_validate(decode_event_json(value))
                except (ValueError, TypeError):
                    event = None  # B3.1 records the precise durable rejection.
                if event is not None and message.key() != source_key(event):
                    rejection = "message key mismatch with stable run/source identity"
            if rejection is not None:
                result = self.store.reject(value, rejection, position, checkpoint_next_offset=checkpoint)
            else:
                result = self.store.ingest(value, position, checkpoint_next_offset=checkpoint)
            # The ledger call has committed before this synchronous Kafka commit.
            committed = self._client.commit(offsets=[TopicPartition(position.topic, position.partition, next_offset)], asynchronous=False)
            if (not committed or len(committed) != 1 or committed[0].topic != position.topic
                    or committed[0].partition != position.partition or committed[0].offset != next_offset
                    or committed[0].error is not None):
                raise ConsumerError(f"offset commit not confirmed after durable receipt {result.receipt_id}")
            return Consumed(position, result, next_offset)
        except BaseException:
            self._failed = True
            raise

    def consume(self, count: int, timeout_s: float = 60) -> list[Consumed]:
        if count < 1 or timeout_s <= 0:
            raise ValueError("positive count and timeout are required")
        deadline = time.monotonic() + timeout_s
        results = []
        while len(results) < count:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"received {len(results)} of {count} requested messages")
            result = self.poll_once(min(self.config.poll_timeout_s, remaining))
            if result is not None:
                results.append(result)
        return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:19092")
    parser.add_argument("--group-id", required=True)
    parser.add_argument("--ledger", required=True)
    parser.add_argument("--messages", type=int, required=True)
    parser.add_argument("--timeout", type=float, default=60)
    args = parser.parse_args()
    config = ConsumerConfig(bootstrap_servers=args.bootstrap_servers, group_id=args.group_id)
    with IngestionStore(args.ledger) as store, EpisodeConsumer(config, store) as consumer:
        for result in consumer.consume(args.messages, args.timeout):
            print(json.dumps({"position": result.position.model_dump(), "ingestion": asdict(result.ingestion),
                              "committed_next_offset": result.committed_next_offset}))


if __name__ == "__main__":
    main()
