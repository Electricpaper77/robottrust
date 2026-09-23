"""Finite retained-history re-ingestion, isolated from live checkpoint/commit state."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time
from typing import Any, Callable
import uuid
from confluent_kafka import Consumer, KafkaError, TopicPartition
from robottrust.streaming.config import ReplayConfig, TransportPosition
from robottrust.streaming.replay_models import Boundary, ReplayError, ReplayFailure, ReplayManifest
from robottrust.streaming.store import IngestionStore
from robottrust.streaming.transport import broker_identity, ingest_transport


class ReplaySession:
    def __init__(self, config: ReplayConfig, store: IngestionStore, *, client: Any | None = None,
                 metadata_provider: Callable[[], tuple[str,str,int]] | None = None):
        self.config, self.store = config, store
        self.session_id = uuid.uuid4().hex
        self._client = client if client is not None else Consumer(config.client_settings(self.session_id))
        self._metadata_provider = metadata_provider or self._identity
        self.manifest: ReplayManifest | None = None
        self._deadline = time.monotonic() + config.timeout_s
        self._closed = False
        self.persisted = False

    def _remaining(self) -> float:
        remaining = self._deadline - time.monotonic()
        if remaining <= 0:
            raise ReplayError(ReplayFailure.DEADLINE,"finite replay deadline reached; idle polls do not prove completion")
        return remaining

    def _identity(self) -> tuple[str,str,int]:
        return broker_identity(self.config.bootstrap_servers,self.config.topic,min(self.config.request_timeout_s,self._remaining()))

    def _watermarks(self, partition: int) -> tuple[int,int]:
        return self._client.get_watermark_offsets(TopicPartition(self.config.topic,partition),
            timeout=min(self.config.request_timeout_s,self._remaining()),cached=False)

    def capture(self, *, starts: dict[int,int] | None = None, partitions: tuple[int,...] | None = None) -> ReplayManifest:
        if self.manifest is not None or self._closed:
            raise ValueError("session already captured or closed")
        try:
            broker,topic,count = self._metadata_provider()
            if count != self.config.expected_partitions:
                raise ReplayError(ReplayFailure.TOPOLOGY,"partition count differs from configured expectation")
            selected = tuple(range(count)) if partitions is None else tuple(sorted(partitions))
            if not selected or len(set(selected)) != len(selected) or any(type(p) is not int or p<0 or p>=count for p in selected):
                raise ValueError("select unique partitions within broker topology")
            if starts is not None and not set(starts).issubset(selected):
                raise ValueError("requested start outside selected partitions")
            boundaries = []
            for p in selected:
                live = self.store.live_checkpoint(self.config.topic,p)
                if live is None:
                    raise ReplayError(ReplayFailure.HISTORY,"existing live provenance is required; reconstruction is not supported")
                if (live.provenance.broker_id,live.provenance.topic_id) != (broker,topic):
                    raise ReplayError(ReplayFailure.IDENTITY,"ledger and broker identity differ")
                if live.provenance.partition_count != count:
                    raise ReplayError(ReplayFailure.TOPOLOGY,"ledger partition topology differs")
                low,high = self._watermarks(p)
                start = (starts or {}).get(p,low)
                if start < low:
                    raise ReplayError(ReplayFailure.EXPIRED,f"partition {p}: requested {start}, earliest {low}")
                if not 0 <= low <= high or start > high:
                    raise ReplayError(ReplayFailure.HISTORY,f"partition {p}: requested {start}, broker interval [{low},{high})")
                boundaries.append(Boundary(partition=p,start_offset=start,end_offset=high))
            self.manifest = ReplayManifest(session_id=self.session_id,topic=self.config.topic,broker_id=broker,topic_id=topic,
                partition_count=count,boundaries=tuple(boundaries),created_at=datetime.now(timezone.utc).isoformat(timespec="microseconds"))
            self.store.create_replay(self.manifest)  # durable vector BEFORE assignment or polling
            self.persisted = True
            self._validate_history()
            targets = [TopicPartition(self.config.topic,b.partition,b.start_offset) for b in boundaries]
            self._client.assign(targets)
            for target in targets:
                self._client.seek(target)
            return self.manifest
        except Exception as exc:
            self._fail(exc)
            raise

    def _validate_history(self) -> None:
        self._remaining()
        broker,topic,count = self._metadata_provider()
        manifest = self.manifest
        if (broker,topic) != (manifest.broker_id,manifest.topic_id):
            raise ReplayError(ReplayFailure.IDENTITY,"broker/topic incarnation changed during replay")
        if count != manifest.partition_count:
            raise ReplayError(ReplayFailure.TOPOLOGY,"partition topology changed during replay")
        for b in manifest.boundaries:
            low,high = self._watermarks(b.partition)
            if low > b.start_offset:
                raise ReplayError(ReplayFailure.EXPIRED,f"partition {b.partition}: frozen start {b.start_offset}, earliest {low}")
            if high < b.end_offset or high < low:
                raise ReplayError(ReplayFailure.HISTORY,f"partition {b.partition}: frozen end {b.end_offset}, broker end {high}")

    def _fail(self, exc: BaseException) -> None:
        if not self.persisted:
            return
        if self.store.replay_state(self.session_id)["status"] == "ACTIVE":
            code = exc.code if isinstance(exc,ReplayError) else ReplayFailure.TRANSPORT
            self.store.finish_replay(self.session_id,f"{code.value}: {exc}")

    def step(self) -> bool:
        """Process at most one broker message; return whether all partitions finished."""
        if self.manifest is None or self._closed:
            raise ValueError("capture an open session first")
        state = self.store.replay_state(self.session_id)
        if state["status"] == "COMPLETE":
            return True
        if state["status"] != "ACTIVE":
            raise ReplayError(ReplayFailure.INTERRUPTED,state["failure_reason"] or "session not active")
        try:
            self._validate_history()
            if all(p["complete"] for p in state["partitions"]):
                return self._finish()
            message = self._client.poll(min(self.config.poll_timeout_s,self._remaining()))
            self._validate_history()  # recheck after poll before accepting traversal
            if message is None:
                return False
            p = message.partition()
            progress = next((row for row in state["partitions"] if row["partition"]==p),None)
            if message.topic() != self.config.topic or progress is None:
                raise ReplayError(ReplayFailure.HISTORY,"message outside captured topic/partitions")
            error = message.error()
            if error is not None and error.code() != KafkaError._PARTITION_EOF:
                raise ReplayError(ReplayFailure.TRANSPORT,str(error))
            if progress["complete"]:
                return False
            offset = message.offset()
            if error is not None:
                if offset >= progress["end_offset"]:
                    self.store.complete_replay_partition(self.session_id,p,offset,"PARTITION_EOF")
            elif offset >= progress["end_offset"]:
                self.store.complete_replay_partition(self.session_id,p,offset,"BOUNDARY_RECORD")
            else:
                if offset < progress["next_offset"]:
                    raise ReplayError(ReplayFailure.HISTORY,"broker delivered before durable replay traversal")
                ingest_transport(self.store,message.value(),message.key(),
                    TransportPosition(topic=self.config.topic,partition=p,offset=offset),replay_id=self.session_id)
            latest = self.store.replay_state(self.session_id)
            if next(row for row in latest["partitions"] if row["partition"]==p)["complete"]:
                self._client.pause([TopicPartition(self.config.topic,p)])  # finite partition finished, not queue backpressure
            if all(row["complete"] for row in latest["partitions"]):
                self._validate_history()
                return self._finish()
            return False
        except Exception as exc:
            self._fail(exc)
            raise

    def _finish(self) -> bool:
        self.store.finish_replay(self.session_id)
        state = self.store.replay_state(self.session_id)
        if state["status"] != "COMPLETE":
            raise ReplayError(ReplayFailure.HISTORY,state["failure_reason"])
        return True

    def run(self) -> dict[str,Any]:
        while not self.step():
            pass
        return self.store.replay_state(self.session_id)

    def close(self) -> None:
        if not self._closed:
            try:
                self._fail(ReplayError(ReplayFailure.INTERRUPTED,"session closed before finite completion"))
            finally:
                self._client.close()  # auto commit disabled; no commit API is called by replay
                self._closed = True

    def __enter__(self) -> ReplaySession:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


def write_replay_evidence(store: IngestionStore, session_id: str, output: Path, *, page_size: int = 100) -> None:
    """Stream observations to JSONL using a bounded keyset cursor."""
    output.mkdir(parents=True,exist_ok=True)
    state = store.replay_state(session_id)
    state["counts"] = store.replay_counts(session_id)
    state["accepted_delta"] = state["accepted_after"]-state["accepted_before"] if state["accepted_after"] is not None else None
    (output/"session.json").write_text(json.dumps(state,indent=2)+"\n",encoding="utf-8",newline="\n")
    cursor = 0
    with (output/"observations.jsonl").open("w",encoding="utf-8",newline="\n") as stream:
        while True:
            page = store.replay_page(session_id,after_receipt=cursor,limit=page_size)
            if not page:
                break
            for item in page:
                stream.write(json.dumps(item,sort_keys=True)+"\n")
            cursor = page[-1]["receipt_id"]
    checksums = {}
    for name in ("session.json","observations.jsonl"):
        with (output/name).open("rb") as stream:
            checksums[name] = hashlib.file_digest(stream,"sha256").hexdigest()
    (output/"checksums.json").write_text(json.dumps(checksums,indent=2)+"\n",encoding="utf-8",newline="\n")


def main() -> None:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger",type=Path,required=True)
    parser.add_argument("--output-dir",type=Path,required=True)
    parser.add_argument("--bootstrap-servers",default="127.0.0.1:19092")
    parser.add_argument("--topic",default="robottrust.episodes.v1")
    parser.add_argument("--timeout",type=float,default=60)
    args=parser.parse_args()
    if not args.ledger.is_file():
        parser.error("an existing ledger is required")
    with IngestionStore(args.ledger) as store, ReplaySession(ReplayConfig(bootstrap_servers=args.bootstrap_servers,topic=args.topic,timeout_s=args.timeout),store) as session:
        try:
            session.capture()
            session.run()
        finally:
            if session.persisted:
                session.close()
                write_replay_evidence(store,session.session_id,args.output_dir)


if __name__ == "__main__":
    main()
