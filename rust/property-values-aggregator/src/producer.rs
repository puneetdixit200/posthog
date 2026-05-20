use std::time::Duration;

use async_trait::async_trait;
use common_kafka::kafka_producer::{send_keyed_iter_to_kafka, KafkaContext};
use rdkafka::producer::FutureProducer;
use thiserror::Error;
use tracing::error;

use crate::types::{OutputMessage, TupleKey};

#[derive(Debug, Error)]
pub enum ProduceError {
    #[error("kafka produce timed out after {0:?}")]
    Timeout(Duration),
    #[error("{failed}/{total} records failed delivery")]
    PartialFailure { failed: usize, total: usize },
}

/// Abstracts the output stage so the worker is testable without a real
/// Kafka producer. Implementations must wait for broker acknowledgment of
/// every record before returning `Ok`, otherwise the worker's offset
/// commit step would advance past records that were never durable.
#[async_trait]
pub trait Producer: Send + Sync {
    async fn produce_batch(
        &self,
        items: Vec<(TupleKey, u64)>,
        timeout: Duration,
    ) -> Result<(), ProduceError>;
}

/// Real producer: serializes tuples and pushes them through
/// `send_keyed_iter_to_kafka`, which collects the per-record delivery
/// futures and awaits each one. Per-record broker rejections surface as
/// errors instead of being silently dropped.
pub struct AggregatedProducer {
    producer: FutureProducer<KafkaContext>,
    topic: String,
}

impl AggregatedProducer {
    pub fn new(producer: FutureProducer<KafkaContext>, topic: String) -> Self {
        Self { producer, topic }
    }
}

#[async_trait]
impl Producer for AggregatedProducer {
    async fn produce_batch(
        &self,
        items: Vec<(TupleKey, u64)>,
        timeout: Duration,
    ) -> Result<(), ProduceError> {
        if items.is_empty() {
            return Ok(());
        }
        let total = items.len();

        let messages: Vec<OutputMessage> = items
            .into_iter()
            .map(|(tuple, count)| OutputMessage {
                team_id: tuple.team_id,
                property_type: tuple.property_type.as_str().to_string(),
                property_key: tuple.property_key,
                property_value: tuple.property_value,
                property_count: count,
            })
            .collect();

        let send_fut = send_keyed_iter_to_kafka(
            &self.producer,
            &self.topic,
            |m| Some(m.team_id.to_string()),
            messages,
        );

        let results = tokio::time::timeout(timeout, send_fut)
            .await
            .map_err(|_| ProduceError::Timeout(timeout))?;

        let mut failed = 0;
        for result in &results {
            if let Err(e) = result {
                failed += 1;
                error!(error = %e, "kafka delivery failed");
            }
        }
        if failed > 0 {
            return Err(ProduceError::PartialFailure { failed, total });
        }
        Ok(())
    }
}
