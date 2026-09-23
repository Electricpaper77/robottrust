"""Transport boundary unit tests; real broker coverage lives in scripts/."""
from unittest.mock import Mock
import pytest
from confluent_kafka import TopicPartition, OFFSET_INVALID
from robottrust.generator import generate_episodes
from robottrust.streaming.config import ProducerConfig, ConsumerConfig
from robottrust.streaming.events import create_event, canonical_payload_bytes
from robottrust.streaming.producer import EpisodeProducer, DeliveryError, source_key
from robottrust.streaming.consumer import EpisodeConsumer, ConsumerError
from robottrust.streaming.store import IngestionStore

@pytest.fixture
def event():
    return create_event(next(generate_episodes(1, 42)), "test", "robot", 0)

@pytest.fixture
def store(tmp_path):
    with IngestionStore(tmp_path / "ledger.db") as ledger:
        yield ledger

def message(event, value="default", key="default", offset=0):
    msg = Mock()
    msg.error.return_value = None
    msg.topic.return_value = "robottrust.episodes.v1"
    msg.partition.return_value = 0
    msg.offset.return_value = offset
    msg.value.return_value = canonical_payload_bytes(event) if value == "default" else value
    msg.key.return_value = source_key(event) if key == "default" else key
    return msg

def consumer(store):
    client = Mock()
    client.commit.side_effect = lambda *, offsets, asynchronous: offsets
    client.committed.return_value = [TopicPartition("robottrust.episodes.v1", 0, OFFSET_INVALID)]
    client.get_watermark_offsets.return_value = (0, 1000)
    worker = EpisodeConsumer(ConsumerConfig(group_id="unit", bootstrap_policy="earliest"), store,
                             client=client, subscribe=False, metadata_provider=lambda: ("cluster-unit", "topic-unit", 3))
    worker.reconcile_assignment([TopicPartition("robottrust.episodes.v1", 0)])
    return worker, client

def test_producer_configuration():
    settings = ProducerConfig().client_settings()
    assert settings["enable.idempotence"] is True
    assert settings["acks"] == "all"
    assert settings["delivery.timeout.ms"] == 10000

@pytest.mark.parametrize("setting", ["enable.auto.commit", "enable.auto.offset.store"])
def test_consumer_manual_offsets(setting):
    assert ConsumerConfig(group_id="unit").client_settings()[setting] is False

@pytest.mark.parametrize("group", ["", " "])
def test_explicit_nonblank_group(group):
    with pytest.raises(ValueError):
        ConsumerConfig(group_id=group)

def producer_client(event, error=None, remaining=0, callback=True):
    client = Mock()
    def flush(timeout):
        if callback:
            client.produce.call_args.kwargs["on_delivery"](error, message(event))
        return remaining
    client.flush.side_effect = flush
    return client

def test_acknowledged_delivery(event):
    client = producer_client(event)
    result = EpisodeProducer(ProducerConfig(), client=client).publish(event)
    assert (result.partition, result.offset, result.event_id) == (0, 0, event.event_id)
    assert client.produce.call_args.kwargs["key"] == source_key(event)
    assert client.produce.call_args.kwargs["value"] == canonical_payload_bytes(event)

@pytest.mark.parametrize("options", [{"error": "broker failure"}, {"remaining": 1}, {"callback": False}])
def test_delivery_failure(event, options):
    with pytest.raises(DeliveryError):
        EpisodeProducer(ProducerConfig(), client=producer_client(event, **options)).publish(event)

def test_enqueue_failure(event):
    client = Mock()
    client.produce.side_effect = BufferError("queue full")
    with pytest.raises(DeliveryError):
        EpisodeProducer(ProducerConfig(), client=client).publish(event)

def test_durable_before_commit(store, event):
    worker, client = consumer(store)
    def commit(*, offsets, asynchronous):
        assert not asynchronous
        assert store.lookup_event(event.event_id) == event
        assert store.latest_checkpoint(offsets[0].topic, 0) == 1
        return offsets
    client.commit.side_effect = commit
    result = worker.handle_message(message(event))
    assert result.ingestion.disposition == "ACCEPTED"
    assert store.lookup_position(result.position) == result.ingestion

def test_ledger_failure_prevents_commit(store, event):
    worker, client = consumer(store)
    store.close()
    with pytest.raises(RuntimeError):
        worker.handle_message(message(event))
    client.commit.assert_not_called()

@pytest.mark.parametrize("value", [b"\xff", b"{", b'{"schema_version":2}', b'{}', None])
def test_invalid_records_durably_rejected(store, event, value):
    worker, client = consumer(store)
    result = worker.handle_message(message(event, value=value))
    assert result.ingestion.disposition == "REJECTED"
    assert result.ingestion.rejection_reason
    assert store.lookup_position(result.position) == result.ingestion
    client.commit.assert_called_once()

@pytest.mark.parametrize("key", [None, b"wrong", b"\xff"])
def test_key_mismatch(store, event, key):
    worker, _ = consumer(store)
    assert worker.handle_message(message(event, key=key)).ingestion.disposition == "REJECTED"
    assert store.lookup_event(event.event_id) is None

@pytest.mark.parametrize("offset", [0, 1])
def test_duplicate_redelivery(store, event, offset):
    worker, _ = consumer(store)
    worker.handle_message(message(event))
    assert worker.handle_message(message(event, offset=offset)).ingestion.disposition == "DUPLICATE"
    assert len(store.accepted_events(event.run_id)) == 1

def test_conflicting_identity(store, event):
    worker, _ = consumer(store)
    worker.handle_message(message(event))
    changed = event.model_copy(update={"sequence": 1})
    assert worker.handle_message(message(changed, offset=1)).ingestion.disposition == "CONFLICT"
    assert store.lookup_event(event.event_id) == event

def test_commit_failure_restart_is_duplicate(store, event):
    worker, client = consumer(store)
    client.commit.side_effect = RuntimeError("disconnected")
    with pytest.raises(RuntimeError):
        worker.handle_message(message(event))
    assert store.lookup_event(event.event_id) == event
    with pytest.raises(ConsumerError):
        worker.handle_message(message(event))
    restarted, _ = consumer(store)
    assert restarted.handle_message(message(event)).ingestion.disposition == "DUPLICATE"

def test_unconfirmed_commit_fails_closed(store, event):
    worker, client = consumer(store)
    client.commit.side_effect = None
    client.commit.return_value = []
    with pytest.raises(ConsumerError):
        worker.handle_message(message(event))
    assert store.lookup_event(event.event_id) == event

def test_reject_reason_required(store, event):
    with pytest.raises(ValueError):
        store.reject(event, " ")
