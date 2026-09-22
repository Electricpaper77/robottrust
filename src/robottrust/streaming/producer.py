"""Acknowledged publishing of B3.1 envelopes through one Kafka client."""
from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import json
from typing import Any

from confluent_kafka import Producer

from robottrust.generator import generate_episodes
from robottrust.streaming.config import ProducerConfig
from robottrust.streaming.events import EventEnvelope, canonical_payload_bytes, create_event


class DeliveryError(RuntimeError):
    """Delivery failed or remains uncertain; callers must retain the event ID."""


@dataclass(frozen=True)
class Delivery:
    topic: str
    partition: int
    offset: int
    event_id: str


def source_key(event: EventEnvelope) -> bytes:
    """Stable JSON tuple of run and source; never regenerate event identity."""
    return json.dumps([event.run_id, event.source_id], ensure_ascii=True, separators=(",", ":")).encode("utf-8")


class EpisodeProducer:
    def __init__(self, config: ProducerConfig, *, client: Any | None = None):
        self.config = config
        self._client = client if client is not None else Producer(config.client_settings())

    def publish(self, event: EventEnvelope) -> Delivery:
        payload = canonical_payload_bytes(event)  # B3.1 revalidation and encoding
        outcomes: list[tuple[Any, Any]] = []

        def delivered(error: Any, message: Any) -> None:
            outcomes.append((error, message))

        try:
            self._client.produce(self.config.topic, value=payload, key=source_key(event), on_delivery=delivered)
            remaining = self._client.flush(self.config.flush_timeout_s)
        except Exception as exc:
            raise DeliveryError(f"publish failed for {event.event_id}: {exc}") from exc
        if remaining:
            raise DeliveryError(f"delivery uncertain for {event.event_id}: {remaining} message(s) unflushed")
        if len(outcomes) != 1:
            raise DeliveryError(f"delivery callback missing or ambiguous for {event.event_id}")
        error, message = outcomes[0]
        if error is not None:
            raise DeliveryError(f"delivery failed for {event.event_id}: {error}")
        result = Delivery(message.topic(), message.partition(), message.offset(), event.event_id)
        if result.topic != self.config.topic or result.partition < 0 or result.offset < 0:
            raise DeliveryError("delivery callback returned invalid transport coordinates")
        return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:19092")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--source-id", default="synthetic")
    parser.add_argument("--episodes", type=int, default=10)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    producer = EpisodeProducer(ProducerConfig(bootstrap_servers=args.bootstrap_servers))
    for sequence, episode in enumerate(generate_episodes(args.episodes, args.seed)):
        delivery = producer.publish(create_event(episode, args.run_id, args.source_id, sequence))
        print(json.dumps(asdict(delivery)))


if __name__ == "__main__":
    main()
