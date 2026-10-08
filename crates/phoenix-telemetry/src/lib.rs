//! Phoenix-compatible telemetry at the fork boundary; upstream instrumentation stays intact.

use std::{future::Future, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use opentelemetry::{KeyValue, trace::TracerProvider};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    Resource,
    propagation::TraceContextPropagator,
    trace::{BatchConfigBuilder, BatchSpanProcessor, Sampler, SdkTracerProvider},
};
use serde::Deserialize;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{
    EnvFilter, Layer, filter::FilterExt, fmt, layer::SubscriberExt, util::SubscriberInitExt,
};

mod log;

#[derive(Debug, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
struct Config {
    enabled: bool,
    endpoint: String,
    sample_ratio: f64,
    export_interval: u64,
    max_batch_size: usize,
    max_queue_size: usize,
    track_idle_time: bool,
    span_filter: Vec<String>,
    #[serde(skip)]
    json_logs: bool,
    #[serde(skip)]
    log_filter: String,
    #[serde(skip)]
    trace_filter: String,
    #[serde(skip)]
    attributes: Vec<KeyValue>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: "http://localhost:4317".into(),
            sample_ratio: 0.1,
            export_interval: 5,
            max_batch_size: 512,
            max_queue_size: 2048,
            track_idle_time: false,
            span_filter: vec![],
            json_logs: false,
            log_filter: "info,sqlx=error".into(),
            trace_filter: "info,sqlx=error".into(),
            attributes: vec![],
        }
    }
}

impl Config {
    fn load(contents: &str, service: &str, env: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let table: toml::Value = toml::from_str(contents).context("Invalid service TOML")?;
        let mut config: Self = table
            .get("tracing")
            .cloned()
            .unwrap_or_else(|| toml::Value::Table(Default::default()))
            .try_into()
            .context("Invalid [tracing] configuration")?;
        if let Some(endpoint) =
            env("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").or_else(|| env("OTEL_EXPORTER_OTLP_ENDPOINT"))
        {
            config.endpoint = endpoint;
            config.enabled = true;
        }
        if let Some(ratio) = env("OTEL_TRACES_SAMPLER_ARG") {
            config.sample_ratio = ratio.parse().context("Invalid OTEL_TRACES_SAMPLER_ARG")?;
        }
        if let Some(disabled) = env("OTEL_SDK_DISABLED") {
            ensure!(
                disabled == "true" || disabled == "false",
                "OTEL_SDK_DISABLED must be true or false"
            );
            config.enabled &= disabled != "true";
        }
        if let Some(format) = env("TRACING_LOG_FORMAT") {
            config.json_logs = match format.as_str() {
                "json" => true,
                "normal" => false,
                _ => bail!("TRACING_LOG_FORMAT must be json or normal"),
            };
        }
        config.log_filter = env("RUST_LOG").unwrap_or(config.log_filter);
        config.trace_filter = env("RUST_TRACING").unwrap_or_else(|| config.log_filter.clone());
        if let Some(attributes) = env("OTEL_RESOURCE_ATTRIBUTES") {
            for attribute in attributes.split(',').filter(|s| !s.trim().is_empty()) {
                let (key, value) = attribute
                    .split_once('=')
                    .context("Invalid OTEL_RESOURCE_ATTRIBUTES: expected key=value")?;
                ensure!(!key.trim().is_empty(), "Empty OTEL resource attribute key");
                config.attributes.push(KeyValue::new(
                    key.trim().to_owned(),
                    value.trim().to_owned(),
                ));
            }
        }
        for (key, variable) in [
            ("deployment.environment.name", "DD_ENV"),
            ("service.version", "DD_VERSION"),
        ] {
            if !config.attributes.iter().any(|kv| kv.key.as_str() == key)
                && let Some(value) = env(variable).filter(|v| !v.is_empty())
            {
                config.attributes.push(KeyValue::new(key, value));
            }
        }
        config.attributes.push(KeyValue::new(
            "service.name",
            env("OTEL_SERVICE_NAME").unwrap_or_else(|| service.into()),
        ));
        ensure!(
            config.sample_ratio.is_finite() && (0.0..=1.0).contains(&config.sample_ratio),
            "Sampling ratio must be finite and between 0 and 1"
        );
        ensure!(
            config.export_interval > 0
                && config.max_batch_size > 0
                && config.max_queue_size >= config.max_batch_size,
            "Invalid tracing batch configuration"
        );
        Ok(config)
    }
}

/// Owns the exporter until shutdown, before the Tokio runtime is destroyed.
pub struct TelemetryGuard {
    provider: Option<SdkTracerProvider>,
    pub log_filter_handle:
        tracing_subscriber::reload::Handle<EnvFilter, tracing_subscriber::Registry>,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.provider.take()
            && let Err(error) = provider.shutdown()
        {
            eprintln!("Failed to flush telemetry: {error}");
        }
    }
}

pub fn init(path: &str, service: &str) -> Result<TelemetryGuard> {
    let contents =
        std::fs::read_to_string(path).context("Read service configuration for telemetry")?;
    init_config(Config::load(&contents, service, |name| {
        std::env::var(name).ok()
    })?)
}

fn init_config(config: Config) -> Result<TelemetryGuard> {
    let filter = EnvFilter::try_new(&config.log_filter).context("Invalid RUST_LOG")?;
    let (filter, handle) = tracing_subscriber::reload::Layer::new(filter);
    let trace_filter = EnvFilter::try_new(&config.trace_filter).context("Invalid RUST_TRACING")?;
    let provider = if config.enabled {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(&config.endpoint)
            .with_timeout(Duration::from_secs(3))
            .build()?;
        let processor = BatchSpanProcessor::builder(exporter)
            .with_batch_config(
                BatchConfigBuilder::default()
                    .with_max_queue_size(config.max_queue_size)
                    .with_max_export_batch_size(config.max_batch_size)
                    .with_scheduled_delay(Duration::from_secs(config.export_interval))
                    .build(),
            )
            .build();
        Some(
            SdkTracerProvider::builder()
                .with_resource(
                    Resource::builder()
                        .with_attributes(config.attributes)
                        .build(),
                )
                .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
                    config.sample_ratio,
                ))))
                .with_span_processor(processor)
                .build(),
        )
    } else {
        None
    };
    let guard = TelemetryGuard {
        provider,
        log_filter_handle: handle,
    };
    let otel_layer = guard.provider.as_ref().map(|provider| {
        tracing_opentelemetry::layer()
            .with_tracer(provider.tracer("cloudbreak"))
            .with_tracked_inactivity(config.track_idle_time)
            .with_filter(
                trace_filter.and(tracing_subscriber::filter::filter_fn(move |metadata| {
                    !metadata.is_span()
                        || config.span_filter.is_empty()
                        || config
                            .span_filter
                            .iter()
                            .any(|name| name == metadata.name())
                })),
            )
    });
    let logs = if config.json_logs {
        fmt::layer()
            .json()
            .with_ansi(false)
            .event_format(log::JsonWithTraceId::new(fmt::format().json()))
            .with_filter(filter)
            .boxed()
    } else {
        fmt::layer().with_filter(filter).boxed()
    };
    tracing_subscriber::registry()
        .with(logs)
        .with(otel_layer)
        .try_init()
        .map_err(|error| anyhow::anyhow!("Initialize telemetry subscriber: {error}"))?;
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    if let Some(provider) = &guard.provider {
        opentelemetry::global::set_tracer_provider(provider.clone());
    }
    Ok(guard)
}

struct Headers<'a>(&'a hyper::HeaderMap);
impl opentelemetry::propagation::Extractor for Headers<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key)?.to_str().ok()
    }
    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|key| key.as_str()).collect()
    }
}

/// Attach the caller's W3C context before entering and starting the request span.
pub fn attach_parent(span: &tracing::Span, headers: &hyper::HeaderMap) {
    let context = opentelemetry::global::get_text_map_propagator(|p| p.extract(&Headers(headers)));
    let _ = span.set_parent(context);
}

/// Handle container termination while the telemetry guard and runtime are alive.
pub async fn run_until_shutdown(run: impl Future<Output = Result<()>>) -> Result<()> {
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let termination = async {
        #[cfg(unix)]
        terminate.recv().await;
        #[cfg(not(unix))]
        std::future::pending::<()>().await;
    };
    tokio::select! {
        result = run => result,
        result = tokio::signal::ctrl_c() => {
            result?;
            tracing::info!("Shutdown signal received");
            Ok(())
        }
        _ = termination => {
            tracing::info!("Termination signal received");
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests;
