use std::sync::Arc;
use std::time::Duration;

use axum::{response::IntoResponse, routing::get, Router};
use common_kafka::kafka_consumer::SingleTopicConsumer;
use lifecycle::{ComponentOptions, Manager};
use property_vals_rs::{
    app_context::AppContext, config::Config, producer::AggregatedProducer, worker::worker_loop,
};
use serve_metrics::setup_metrics_routes;
use tokio::net::TcpListener;
use tracing::level_filters::LevelFilter;
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

common_alloc::used!();

fn setup_tracing() {
    let log_layer = tracing_subscriber::fmt::layer().with_filter(
        EnvFilter::builder()
            .with_default_directive(LevelFilter::INFO.into())
            .from_env_lossy()
            .add_directive("pyroscope=warn".parse().unwrap()),
    );
    tracing_subscriber::registry().with(log_layer).init();
}

pub async fn index() -> &'static str {
    "property-vals-rs"
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_tracing();
    info!("Starting up property-vals-rs...");

    let config = Config::init_with_defaults()?;

    let _profiling_agent = match config.continuous_profiling.start_agent() {
        Ok(agent) => agent,
        Err(e) => {
            warn!("Failed to start continuous profiling agent: {e}");
            None
        }
    };

    let mut manager = Manager::builder("property-vals-rs")
        .with_global_shutdown_timeout(Duration::from_secs(60))
        .build();

    let worker_handle = manager.register(
        "worker",
        ComponentOptions::new()
            .with_graceful_shutdown(Duration::from_secs(30))
            .with_liveness_deadline(Duration::from_secs(60))
            .with_stall_threshold(3),
    );

    let metrics_handle =
        manager.register("metrics", ComponentOptions::new().is_observability(true));

    let readiness = manager.readiness_handler();
    let liveness = manager.liveness_handler();

    info!(
        topic = %config.consumer.kafka_consumer_topic,
        consumer_group = %config.consumer.kafka_consumer_group,
        output_topic = %config.output_topic,
        flush_interval_secs = config.flush_interval_secs,
        transactional_id = %config.kafka_transactional_id,
        "config loaded"
    );

    let consumer = SingleTopicConsumer::new(config.kafka.clone(), config.consumer.clone())?;
    let producer = AggregatedProducer::new(
        &config.kafka,
        &config.kafka_transactional_id,
        config.output_topic.clone(),
        Duration::from_secs(config.kafka_transaction_timeout_secs),
        consumer.clone(),
    )?;

    let ctx = Arc::new(AppContext::new(&config));

    let guard = manager.monitor_background();

    // Single worker per pod: rdkafka enforces one outstanding transaction
    // per transactional.id, and there's one transactional.id per pod.
    tokio::spawn(worker_loop(
        ctx.clone(),
        consumer,
        producer,
        worker_handle.clone(),
    ));
    drop(worker_handle);

    let app = Router::new()
        .route("/", get(index))
        .route(
            "/_readiness",
            get({
                let r = readiness.clone();
                move || {
                    let r = r.clone();
                    async move { r.check().await }
                }
            }),
        )
        .route(
            "/_liveness",
            get({
                let l = liveness.clone();
                move || {
                    let l = l.clone();
                    async move { l.check().into_response() }
                }
            }),
        );
    let app = setup_metrics_routes(app);

    let bind = format!("{}:{}", config.host, config.port);
    info!(address = %bind, "HTTP server starting");
    let listener = TcpListener::bind(&bind).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(metrics_handle.shutdown_signal())
        .await?;
    metrics_handle.work_completed();

    guard.wait().await?;

    info!("property-vals-rs stopped");
    Ok(())
}
