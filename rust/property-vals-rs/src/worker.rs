use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common_kafka::kafka_consumer::{Offset, OffsetErr, RecvErr, SingleTopicConsumer};
use tracing::{error, info, warn};

use crate::aggregator::Aggregator;
use crate::app_context::AppContext;
use crate::fan_out::fan_out;
use crate::metrics_consts::*;
use crate::producer::Producer;
use crate::types::Event;

/// Abstracts the offset-commit step so flush() can be tested without a
/// real Kafka consumer (the real `Offset` type can only be constructed by
/// common-kafka internals).
pub trait CommittableOffset {
    fn partition(&self) -> i32;
    fn commit(self) -> Result<(), OffsetErr>;
}

impl CommittableOffset for Offset {
    fn partition(&self) -> i32 {
        self.partition()
    }
    fn commit(self) -> Result<(), OffsetErr> {
        self.store()
    }
}

/// One worker loop: consumes events from Kafka, fans them out into tuples,
/// accumulates per-tuple counts in an in-memory buffer, and on each flush
/// timer drains the buffer to the output topic and stores input offsets.
///
/// Multiple workers can run concurrently against the same shared
/// `SingleTopicConsumer`; rdkafka multiplexes partition assignments
/// across them and each holds its own independent buffer.
pub async fn worker_loop(
    ctx: Arc<AppContext>,
    consumer: SingleTopicConsumer,
    handle: lifecycle::Handle,
) {
    let _guard = handle.process_scope();

    let mut aggregator = Aggregator::new();
    // Latest seen offset per partition; replaced as newer offsets arrive,
    // committed at flush time so commits trail durable produce.
    let mut pending_offsets: HashMap<i32, Offset> = HashMap::new();

    let mut flush_timer = tokio::time::interval(ctx.flush_interval);
    flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    flush_timer.reset();

    loop {
        tokio::select! {
            _ = handle.shutdown_recv() => {
                info!("worker received shutdown; draining final flush");
                flush(
                    &mut aggregator,
                    &mut pending_offsets,
                    ctx.producer.as_ref(),
                    ctx.producer_flush_timeout,
                    FLUSH_REASON_SHUTDOWN,
                ).await;
                return;
            }
            _ = flush_timer.tick() => {
                flush(
                    &mut aggregator,
                    &mut pending_offsets,
                    ctx.producer.as_ref(),
                    ctx.producer_flush_timeout,
                    FLUSH_REASON_TIMER,
                ).await;
            }
            recv = consumer.json_recv::<Event>() => {
                match recv {
                    Ok((event, offset)) => {
                        handle.report_healthy();
                        metrics::counter!(EVENTS_RECEIVED).increment(1);

                        if ctx.should_process(event.team_id) {
                            let tuples = fan_out(&event);
                            metrics::counter!(TUPLES_AGGREGATED).increment(tuples.len() as u64);
                            aggregator.record_many(tuples);
                        } else {
                            metrics::counter!(EVENTS_FILTERED).increment(1);
                        }

                        pending_offsets.insert(offset.partition(), offset);

                        if aggregator.len() >= ctx.max_entries_per_partition {
                            flush(
                                &mut aggregator,
                                &mut pending_offsets,
                                ctx.producer.as_ref(),
                                ctx.producer_flush_timeout,
                                FLUSH_REASON_BACKPRESSURE,
                            ).await;
                        }
                    }
                    Err(RecvErr::Empty) | Err(RecvErr::Serde(_)) => {
                        // SingleTopicConsumer auto-stores poison-pill offsets.
                    }
                    Err(RecvErr::Kafka(e)) => {
                        metrics::counter!(KAFKA_RECV_ERRORS).increment(1);
                        warn!(error = %e, "kafka recv error");
                    }
                }
            }
        }
    }
}

/// Drain the aggregator, produce all tuples, wait for broker acks, then
/// commit input offsets. Three correctness invariants this code holds:
///
/// 1. Offsets are committed only after produce returns Ok. A failed produce
///    leaves `pending_offsets` intact so events replay on consumer
///    rebalance or restart.
/// 2. On produce failure the drained snapshot is merged back into the
///    aggregator. The previous design's "retry on next flush" comment was
///    misleading; only offset retention is automatic. Counts need an
///    explicit restore or they go out of scope and are lost.
/// 3. When the aggregator is empty but `pending_offsets` is non-empty (all
///    events filtered), `produce_batch` is still called with an empty
///    batch and short-circuits to Ok; offsets then commit. This is safe
///    because there are no records in flight at this point: any prior
///    failed flush already restored its counts into the aggregator, so an
///    empty aggregator here means there is nothing un-acked.
pub(crate) async fn flush<P, O>(
    aggregator: &mut Aggregator,
    pending_offsets: &mut HashMap<i32, O>,
    producer: &P,
    timeout: Duration,
    reason: &'static str,
) where
    P: Producer + ?Sized,
    O: CommittableOffset,
{
    if aggregator.is_empty() && pending_offsets.is_empty() {
        return;
    }

    let snapshot: Vec<(crate::types::TupleKey, u64)> = aggregator.drain().into_iter().collect();

    metrics::counter!(FLUSH_TOTAL, "reason" => reason).increment(1);
    metrics::histogram!(FLUSH_TUPLES).record(snapshot.len() as f64);

    if let Err(e) = producer.produce_batch(snapshot.clone(), timeout).await {
        metrics::counter!(PRODUCER_FLUSH_FAILED).increment(1);
        error!(error = %e, "producer flush failed; restoring counts, deferring offsets");
        // Merge the drained snapshot back into the aggregator. Using add()
        // means any tuples that arrived between drain and restore keep
        // their counts.
        for (tuple, count) in snapshot {
            aggregator.add(tuple, count);
        }
        return;
    }

    // Produce confirmed; safe to advance our position.
    let to_store = std::mem::take(pending_offsets);
    for (partition, offset) in to_store {
        if let Err(e) = offset.commit() {
            metrics::counter!(OFFSET_STORE_FAILED).increment(1);
            warn!(partition, error = %e, "offset store failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::producer::ProduceError;
    use crate::types::{PropertyType, TupleKey};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;

    /// Mock producer that records each batch it sees and can be configured
    /// to fail on specific call indices.
    struct MockProducer {
        calls: AtomicUsize,
        fail_on: Mutex<Vec<usize>>,
        seen: Mutex<Vec<Vec<(TupleKey, u64)>>>,
    }

    impl MockProducer {
        fn new() -> Self {
            Self {
                calls: AtomicUsize::new(0),
                fail_on: Mutex::new(Vec::new()),
                seen: Mutex::new(Vec::new()),
            }
        }
        fn fail_on(self, call_index: usize) -> Self {
            self.fail_on.lock().unwrap().push(call_index);
            self
        }
        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
        fn seen_total_records(&self) -> usize {
            self.seen.lock().unwrap().iter().map(|b| b.len()).sum()
        }
        fn last_batch(&self) -> Vec<(TupleKey, u64)> {
            self.seen
                .lock()
                .unwrap()
                .last()
                .cloned()
                .unwrap_or_default()
        }
    }

    #[async_trait::async_trait]
    impl Producer for MockProducer {
        async fn produce_batch(
            &self,
            items: Vec<(TupleKey, u64)>,
            _timeout: Duration,
        ) -> Result<(), ProduceError> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            self.seen.lock().unwrap().push(items.clone());
            if self.fail_on.lock().unwrap().contains(&n) {
                let total = items.len().max(1);
                return Err(ProduceError::PartialFailure {
                    failed: total,
                    total,
                });
            }
            Ok(())
        }
    }

    struct TestOffset {
        partition: i32,
        committed: Arc<AtomicBool>,
    }

    impl TestOffset {
        fn new(partition: i32) -> (Self, Arc<AtomicBool>) {
            let committed = Arc::new(AtomicBool::new(false));
            (
                Self {
                    partition,
                    committed: committed.clone(),
                },
                committed,
            )
        }
    }

    impl CommittableOffset for TestOffset {
        fn partition(&self) -> i32 {
            self.partition
        }
        fn commit(self) -> Result<(), OffsetErr> {
            self.committed.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    fn tuple(team: i64, key: &str, value: &str) -> TupleKey {
        TupleKey {
            team_id: team,
            property_type: PropertyType::Event,
            property_key: key.to_string(),
            property_value: value.to_string(),
        }
    }

    fn populate(agg: &mut Aggregator, count: u64) {
        for i in 0..count {
            agg.record(tuple(2, "k", &format!("v{i}")));
        }
    }

    #[tokio::test]
    async fn successful_flush_drains_aggregator_and_commits_offsets() {
        let mut agg = Aggregator::new();
        populate(&mut agg, 5);

        let (offset_p0, committed_p0) = TestOffset::new(0);
        let (offset_p1, committed_p1) = TestOffset::new(1);
        let mut pending: HashMap<i32, TestOffset> = HashMap::new();
        pending.insert(0, offset_p0);
        pending.insert(1, offset_p1);

        let producer = MockProducer::new();
        flush(
            &mut agg,
            &mut pending,
            &producer,
            Duration::from_secs(1),
            FLUSH_REASON_TIMER,
        )
        .await;

        assert!(agg.is_empty(), "aggregator should be drained after success");
        assert!(pending.is_empty(), "pending_offsets should be drained");
        assert!(committed_p0.load(Ordering::SeqCst));
        assert!(committed_p1.load(Ordering::SeqCst));
        assert_eq!(producer.call_count(), 1);
        assert_eq!(producer.seen_total_records(), 5);
    }

    #[tokio::test]
    async fn failed_flush_restores_counts_and_keeps_pending_offsets() {
        let mut agg = Aggregator::new();
        populate(&mut agg, 3);

        let (offset, committed) = TestOffset::new(0);
        let mut pending: HashMap<i32, TestOffset> = HashMap::new();
        pending.insert(0, offset);

        let producer = MockProducer::new().fail_on(1);
        flush(
            &mut agg,
            &mut pending,
            &producer,
            Duration::from_secs(1),
            FLUSH_REASON_TIMER,
        )
        .await;

        assert_eq!(
            agg.len(),
            3,
            "aggregator must restore drained counts when produce fails"
        );
        assert_eq!(
            pending.len(),
            1,
            "pending_offsets must NOT be drained when produce fails"
        );
        assert!(
            !committed.load(Ordering::SeqCst),
            "offsets must NOT commit when produce fails"
        );
    }

    #[tokio::test]
    async fn failed_then_successful_flush_eventually_commits_offsets() {
        let mut agg = Aggregator::new();
        populate(&mut agg, 4);

        let (offset, committed) = TestOffset::new(0);
        let mut pending: HashMap<i32, TestOffset> = HashMap::new();
        pending.insert(0, offset);

        let producer = MockProducer::new().fail_on(1);

        flush(
            &mut agg,
            &mut pending,
            &producer,
            Duration::from_secs(1),
            FLUSH_REASON_TIMER,
        )
        .await;
        assert!(!agg.is_empty());
        assert!(!committed.load(Ordering::SeqCst));

        flush(
            &mut agg,
            &mut pending,
            &producer,
            Duration::from_secs(1),
            FLUSH_REASON_TIMER,
        )
        .await;
        assert!(agg.is_empty());
        assert!(pending.is_empty());
        assert!(committed.load(Ordering::SeqCst));
        assert_eq!(producer.call_count(), 2);
    }

    #[tokio::test]
    async fn empty_aggregator_with_pending_offsets_commits_offsets() {
        // Pure filter-only window: no aggregator counts, only offsets from
        // filtered events. With no prior failed flush there are no records
        // in flight, so it is safe to commit offsets without producing.
        let mut agg = Aggregator::new();
        let (offset, committed) = TestOffset::new(0);
        let mut pending: HashMap<i32, TestOffset> = HashMap::new();
        pending.insert(0, offset);

        let producer = MockProducer::new();
        flush(
            &mut agg,
            &mut pending,
            &producer,
            Duration::from_secs(1),
            FLUSH_REASON_TIMER,
        )
        .await;

        assert!(pending.is_empty());
        assert!(committed.load(Ordering::SeqCst));
        assert_eq!(producer.call_count(), 1);
        assert_eq!(producer.seen_total_records(), 0);
    }

    #[tokio::test]
    async fn empty_aggregator_empty_offsets_is_noop() {
        let mut agg = Aggregator::new();
        let mut pending: HashMap<i32, TestOffset> = HashMap::new();
        let producer = MockProducer::new();
        flush(
            &mut agg,
            &mut pending,
            &producer,
            Duration::from_secs(1),
            FLUSH_REASON_TIMER,
        )
        .await;
        assert_eq!(producer.call_count(), 0);
    }

    #[tokio::test]
    async fn restored_counts_are_emitted_on_next_successful_flush() {
        // Window N: counts captured, produce fails, counts restored.
        // Window N+1: produce succeeds and emits the restored counts.
        let mut agg = Aggregator::new();
        agg.record(tuple(2, "k1", "v1"));
        agg.record(tuple(2, "k1", "v1"));

        let (offset, _committed) = TestOffset::new(0);
        let mut pending: HashMap<i32, TestOffset> = HashMap::new();
        pending.insert(0, offset);

        let producer = MockProducer::new().fail_on(1);

        flush(
            &mut agg,
            &mut pending,
            &producer,
            Duration::from_secs(1),
            FLUSH_REASON_TIMER,
        )
        .await;
        flush(
            &mut agg,
            &mut pending,
            &producer,
            Duration::from_secs(1),
            FLUSH_REASON_TIMER,
        )
        .await;

        let batch = producer.last_batch();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].1, 2, "restored count must equal original count");
    }

    #[tokio::test]
    async fn restored_counts_merge_with_new_counts_in_next_window() {
        // After failure, a new event landing on the SAME tuple as a
        // restored one must merge counts, not overwrite.
        let mut agg = Aggregator::new();
        agg.record(tuple(2, "k1", "v1"));

        let (offset, _committed) = TestOffset::new(0);
        let mut pending: HashMap<i32, TestOffset> = HashMap::new();
        pending.insert(0, offset);

        let producer = MockProducer::new().fail_on(1);
        flush(
            &mut agg,
            &mut pending,
            &producer,
            Duration::from_secs(1),
            FLUSH_REASON_TIMER,
        )
        .await;

        agg.record(tuple(2, "k1", "v1"));

        flush(
            &mut agg,
            &mut pending,
            &producer,
            Duration::from_secs(1),
            FLUSH_REASON_TIMER,
        )
        .await;

        let batch = producer.last_batch();
        assert_eq!(batch.len(), 1);
        assert_eq!(
            batch[0].1, 2,
            "restored count + post-restore event should merge into one tuple"
        );
    }

    #[tokio::test]
    async fn stale_offsets_only_commit_once_produce_succeeds() {
        // Direct test of the bug the code review surfaced. Window N fails
        // (counts restored, offsets retained). Window N+1 has all filtered
        // events (no new counts) but adds a newer offset on top of the
        // retained one. Until the restored counts are durably produced,
        // offsets MUST NOT commit. After produce succeeds, they advance to
        // the newer offset.
        let mut agg = Aggregator::new();
        agg.record(tuple(2, "k1", "v1"));

        let (window_n_offset, committed_n) = TestOffset::new(0);
        let mut pending: HashMap<i32, TestOffset> = HashMap::new();
        pending.insert(0, window_n_offset);

        let producer = MockProducer::new().fail_on(1);

        // Window N: produce fails. Counts restored. Offset retained.
        flush(
            &mut agg,
            &mut pending,
            &producer,
            Duration::from_secs(1),
            FLUSH_REASON_TIMER,
        )
        .await;
        assert_eq!(agg.len(), 1);
        assert_eq!(pending.len(), 1);
        assert!(!committed_n.load(Ordering::SeqCst));

        // Simulate Window N+1's filter-only traffic: a NEWER offset
        // replaces the retained one on the same partition. No new counts.
        let (window_n_plus_1_offset, committed_n_plus_1) = TestOffset::new(0);
        pending.insert(0, window_n_plus_1_offset);

        // Window N+1: aggregator still has restored counts. Produce
        // succeeds this time and advances offsets.
        flush(
            &mut agg,
            &mut pending,
            &producer,
            Duration::from_secs(1),
            FLUSH_REASON_TIMER,
        )
        .await;
        assert!(agg.is_empty());
        assert!(pending.is_empty());
        // The OLD offset object was dropped when we overwrote it. The NEW
        // offset is the one that should commit.
        assert!(committed_n_plus_1.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn produce_error_does_not_advance_offsets_under_repeated_failure() {
        // Three consecutive failures: aggregator keeps state, offsets stay
        // in pending across all of them. This is the "broker is gone for a
        // while" scenario.
        let mut agg = Aggregator::new();
        agg.record(tuple(2, "k1", "v1"));

        let (offset, committed) = TestOffset::new(0);
        let mut pending: HashMap<i32, TestOffset> = HashMap::new();
        pending.insert(0, offset);

        let producer = MockProducer::new().fail_on(1).fail_on(2).fail_on(3);

        for _ in 0..3 {
            flush(
                &mut agg,
                &mut pending,
                &producer,
                Duration::from_secs(1),
                FLUSH_REASON_TIMER,
            )
            .await;
        }

        assert_eq!(agg.len(), 1, "counts persist across repeated failures");
        assert_eq!(pending.len(), 1, "offsets persist across repeated failures");
        assert!(!committed.load(Ordering::SeqCst));
        assert_eq!(producer.call_count(), 3);
    }
}
