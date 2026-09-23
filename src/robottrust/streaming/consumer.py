"""At-least-once delivery plus idempotent durable ingestion; manual commits."""
from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass, replace
import json
import time
from typing import Any, Callable

from confluent_kafka import Consumer, KafkaError, TopicPartition, TopicCollection, OFFSET_INVALID
from confluent_kafka.admin import AdminClient

from robottrust.streaming.config import ConsumerConfig, TransportPosition
from robottrust.streaming.events import EventEnvelope, decode_event_json
from robottrust.streaming.producer import source_key
from robottrust.streaming.store import IngestResult, IngestionStore
from robottrust.streaming.reconciliation import ConsumerError, Action, Provenance, RecoveryDecision, RecoveryError, reconcile


@dataclass(frozen=True)
class Consumed:
    position: TransportPosition
    ingestion: IngestResult
    committed_next_offset: int


class EpisodeConsumer:
    def __init__(self, config: ConsumerConfig, store: IngestionStore, *, client: Any | None = None,
                 subscribe: bool = True,
                 metadata_provider: Callable[[], tuple[str, str, int]] | None = None):
        self.config, self.store = config, store
        self._client = client if client is not None else Consumer(config.client_settings())
        self._metadata_provider = metadata_provider or self._broker_identity
        self._reconciled: dict[int, RecoveryDecision] = {}
        self._broker_offsets: dict[int, int | None] = {}
        self._startup_error: RecoveryError | None = None
        self._failed = False
        self._closed = False
        if subscribe:
            try:
                self._client.subscribe([config.topic], on_assign=self._on_assign, on_revoke=self._on_revoke, on_lost=self._on_revoke)
            except BaseException:
                self._client.close()
                raise

    def _broker_identity(self) -> tuple[str, str, int]:
        admin = AdminClient({"bootstrap.servers": self.config.bootstrap_servers})
        timeout = self.config.startup_timeout_s
        cluster = admin.describe_cluster(request_timeout=timeout).result(timeout)
        topic = admin.describe_topics(TopicCollection([self.config.topic]), request_timeout=timeout)[self.config.topic].result(timeout)
        topic_id = str(topic.topic_id)
        if not cluster.cluster_id or not topic_id or topic_id == "AAAAAAAAAAAAAAAAAAAAAA":
            raise ValueError("broker does not expose a usable cluster/topic incarnation identity")
        return cluster.cluster_id, topic_id, len(topic.partitions)

    def _on_assign(self, client: Any, partitions: list[Any]) -> None:
        try:
            self.reconcile_assignment(partitions)
        except RecoveryError as exc:
            self._startup_error = exc
            self._failed = True

    def _on_revoke(self, client: Any, partitions: list[Any]) -> None:
        for partition in partitions:
            self._reconciled.pop(partition.partition, None)
            self._broker_offsets.pop(partition.partition, None)

    def _checked_commit(self, partition: int, offset: int) -> None:
        committed = self._client.commit(offsets=[TopicPartition(self.config.topic, partition, offset)], asynchronous=False)
        if (not committed or len(committed) != 1 or committed[0].topic != self.config.topic
                or committed[0].partition != partition or committed[0].offset != offset
                or committed[0].error is not None):
            raise ConsumerError("broker did not confirm requested next offset")
        self._broker_offsets[partition] = offset

    def reconcile_assignment(self, partitions: list[Any], *, explicit_starts: dict[int, int] | None = None) -> list[RecoveryDecision]:
        """Assign, reconcile, explicitly seek, and only then enable processing.

        Public for controlled manual assignment; subscribed consumers invoke it
        from on_assign. Injected clients follow the identical startup gate.
        """
        self._ready()
        self._reconciled.clear()
        results = []
        context = RecoveryDecision(self.config.topic, -1, None, None, None, None, Action.TRANSPORT, "assignment startup")
        try:
            broker_id, topic_id, count = self._metadata_provider()
            for partition in partitions:
                p = partition.partition
                durable = self.store.live_checkpoint(self.config.topic, p)
                legacy = self.store.latest_checkpoint(self.config.topic, p)
                context = RecoveryDecision(self.config.topic, p, durable.next_offset if durable else legacy,
                                           None, None, None, Action.TRANSPORT, "fetching broker startup state")
                if partition.topic != self.config.topic:
                    raise RecoveryError(replace(context, decision=Action.IDENTITY, reason="unexpected assigned topic"))
                committed = self._client.committed([TopicPartition(self.config.topic, p)], timeout=self.config.startup_timeout_s)
                if len(committed) != 1 or committed[0].error is not None:
                    raise ValueError("broker committed-offset lookup failed")
                k = committed[0].offset
                if k == OFFSET_INVALID:
                    k = None
                elif k < 0:
                    raise ValueError("unexpected negative committed offset")
                context = replace(context, K=k)
                low, high = self._client.get_watermark_offsets(TopicPartition(self.config.topic, p), timeout=self.config.startup_timeout_s, cached=False)
                context = replace(context, earliest=low, end=high)
                provenance = Provenance(self.config.topic, p, broker_id, topic_id, count)
                policy = self.config.bootstrap_policy
                start = (low if policy == "earliest" else high if policy == "latest" else
                         (explicit_starts or {}).get(p) if policy == "explicit" else None)
                decision = reconcile(durable, provenance, k, low, high,
                                     expected_partitions=self.config.expected_partitions,
                                     coverage_verified=durable is not None and self.store.verify_live_coverage(durable),
                                     explicit_start=start, new_stream=policy is not None and self.store.can_bootstrap(self.config.topic, p))
                if durable is None and legacy is not None:
                    decision = replace(decision, D=legacy, decision=Action.BOOTSTRAP_REQUIRED,
                                       reason="legacy checkpoint lacks starting boundary/provenance; explicit migration required")
                context = decision
                self.store.record_startup(decision, "FAILED" if decision.failed else "PLANNED")
                if decision.failed:
                    raise RecoveryError(decision)
                if decision.decision == Action.BOOTSTRAP:
                    self.store.initialize_live(provenance, decision.seek_offset)
                results.append(decision)
            self._client.assign([TopicPartition(self.config.topic, r.partition, r.seek_offset) for r in results])
            for decision in results:
                context = decision
                p = decision.partition
                self._client.seek(TopicPartition(self.config.topic, p, decision.seek_offset))
                self._broker_offsets[p] = decision.K
                if decision.decision == Action.REPAIR:
                    self._checked_commit(p, decision.seek_offset)
                self.store.record_startup(decision, "REPAIRED" if decision.decision == Action.REPAIR else "READY")
            self._reconciled = {result.partition: result for result in results}
            return results
        except BaseException as exc:
            self._reconciled.clear()
            self._failed = True
            if isinstance(exc, RecoveryError):
                raise
            failure = replace(context, decision=Action.TRANSPORT, reason=f"startup failed: {exc}")
            try:
                self.store.record_startup(failure, "FAILED")
            except Exception as audit_error:
                failure = replace(failure, reason=f"{failure.reason}; startup audit also failed: {audit_error}")
            raise RecoveryError(failure) from exc

    def __enter__(self) -> EpisodeConsumer:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def close(self) -> None:
        if not self._closed:
            self._client.close()  # auto commit is disabled
            self._closed = True

    def _ready(self) -> None:
        if self._startup_error is not None:
            raise self._startup_error
        if self._closed or self._failed:
            raise ConsumerError("consumer is closed or failed; create a new instance")

    def poll_once(self, timeout_s: float | None = None) -> Consumed | None:
        self._ready()
        try:
            message = self._client.poll(self.config.poll_timeout_s if timeout_s is None else timeout_s)
            self._ready()
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
                context = self._reconciled.get(message.partition()) or RecoveryDecision(
                    message.topic() or self.config.topic, message.partition(), None, None, None, None,
                    Action.TRANSPORT, "broker message error before reconciliation")
                raise RecoveryError(replace(context, decision=Action.TRANSPORT,
                                            reason=f"broker message error (no automatic reset): {error}"))
            position = TransportPosition(topic=message.topic(), partition=message.partition(), offset=message.offset())
            if position.topic != self.config.topic:
                raise ConsumerError("received a message from an unexpected topic")
            context = self._reconciled.get(position.partition)
            if context is None:
                raise RecoveryError(RecoveryDecision(position.topic, position.partition, None, None, None, None,
                                                     Action.NOT_READY, "partition must be reconciled before processing"))
            live = self.store.live_checkpoint(position.topic, position.partition)
            previous = live.next_offset
            if position.offset > previous or position.offset < live.start_offset:
                raise RecoveryError(replace(context, D=previous, decision=Action.INCONSISTENT,
                                             reason="delivery would skip durable work or precedes starting boundary"))
            next_offset = position.offset + 1
            # Redelivery can be behind a durable checkpoint after a failed broker
            # commit. Do not rewind SQLite; still record the duplicate receipt.
            checkpoint = next_offset if position.offset == previous else None
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
            durable_next = self.store.live_checkpoint(position.topic, position.partition).next_offset
            broker_next = self._broker_offsets.get(position.partition)
            # An ahead broker commit is not trusted for processing, nor rewritten
            # backwards while local durable work catches up. Old delivery commits D.
            if broker_next is None or durable_next >= broker_next:
                try:
                    self._checked_commit(position.partition, durable_next)
                except Exception as exc:
                    raise RecoveryError(replace(context, D=durable_next, K=broker_next, decision=Action.TRANSPORT,
                                                reason=f"commit failed after durable receipt {result.receipt_id}: {exc}")) from exc
            return Consumed(position, result, self._broker_offsets[position.partition])
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
    parser.add_argument("--bootstrap-policy", choices=["earliest", "latest"], help="explicitly authorize a new stream/group boundary")
    args = parser.parse_args()
    config = ConsumerConfig(bootstrap_servers=args.bootstrap_servers, group_id=args.group_id, bootstrap_policy=args.bootstrap_policy)
    with IngestionStore(args.ledger) as store, EpisodeConsumer(config, store) as consumer:
        for result in consumer.consume(args.messages, args.timeout):
            print(json.dumps({"position": result.position.model_dump(), "ingestion": asdict(result.ingestion),
                              "committed_next_offset": result.committed_next_offset}))


if __name__ == "__main__":
    main()
