use super::*;
use opentelemetry::trace::TraceContextExt;
use opentelemetry_proto::tonic::{
    collector::trace::v1::{
        ExportTraceServiceRequest, ExportTraceServiceResponse,
        trace_service_server::{TraceService, TraceServiceServer},
    },
    common::v1::any_value::Value,
};
use std::sync::{Arc, Mutex};

fn config(toml: &str, vars: &[(&str, &str)]) -> Result<Config> {
    Config::load(toml, "cloudbreak-api", |name| {
        vars.iter()
            .find(|(key, _)| *key == name)
            .map(|(_, value)| (*value).into())
    })
}

#[test]
fn phoenix_environment_enables_export_and_overrides_local_endpoint() {
    let config = config(
        "[tracing]\nendpoint='http://localhost:4317'",
        &[
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://agent:4317"),
            ("TRACING_LOG_FORMAT", "json"),
            ("DD_ENV", "prod"),
            ("DD_VERSION", "image-tag"),
            (
                "OTEL_RESOURCE_ATTRIBUTES",
                "k8s.pod.uid=pod-123,deployment.environment.name=custom",
            ),
        ],
    )
    .unwrap();
    assert!(config.enabled && config.json_logs);
    assert_eq!(config.endpoint, "http://agent:4317");
    let resource = Resource::builder_empty()
        .with_attributes(config.attributes)
        .build();
    assert_eq!(
        resource.get(&"service.name".into()).unwrap().to_string(),
        "cloudbreak-api"
    );
    assert_eq!(
        resource.get(&"service.version".into()).unwrap().to_string(),
        "image-tag"
    );
    assert_eq!(
        resource
            .get(&"deployment.environment.name".into())
            .unwrap()
            .to_string(),
        "custom"
    );
    assert_eq!(
        resource.get(&"k8s.pod.uid".into()).unwrap().to_string(),
        "pod-123"
    );
}

#[test]
fn tracing_endpoint_and_service_overrides_take_precedence() {
    let config = config(
        "",
        &[
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://agent:4317"),
            ("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://traces:4317"),
            ("OTEL_SERVICE_NAME", "custom-api"),
            ("OTEL_SDK_DISABLED", "true"),
        ],
    )
    .unwrap();
    assert!(!config.enabled);
    assert_eq!(config.endpoint, "http://traces:4317");
    assert_eq!(
        config.attributes.last().unwrap().value.to_string(),
        "custom-api"
    );
}

#[test]
fn local_toml_and_disabled_default_remain_supported() {
    assert!(!config("", &[]).unwrap().enabled);
    let config = config(
        "[tracing]\nenabled=true\nsample-ratio=0.25\nspan-filter=['http_request']",
        &[],
    )
    .unwrap();
    assert!(config.enabled);
    assert_eq!(config.sample_ratio, 0.25);
    assert_eq!(config.span_filter, ["http_request"]);
}

#[test]
fn invalid_configuration_is_reported() {
    for input in [
        "[tracing]\nsample-ratio=2.0",
        "[tracing]\nmax-queue-size=1",
        "[tracing]\nenabled='yes'",
    ] {
        assert!(config(input, &[]).is_err());
    }
    for vars in [
        vec![("OTEL_TRACES_SAMPLER_ARG", "NaN")],
        vec![("OTEL_RESOURCE_ATTRIBUTES", "malformed")],
        vec![("TRACING_LOG_FORMAT", "unknown")],
    ] {
        assert!(config("", &vars).is_err());
    }
}

#[derive(Clone, Default)]
struct Collector(Arc<Mutex<Vec<ExportTraceServiceRequest>>>);

#[tonic::async_trait]
impl TraceService for Collector {
    async fn export(
        &self,
        request: tonic::Request<ExportTraceServiceRequest>,
    ) -> std::result::Result<tonic::Response<ExportTraceServiceResponse>, tonic::Status> {
        self.0.lock().unwrap().push(request.into_inner());
        Ok(tonic::Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn otlp_export_preserves_parent_and_flushes_on_exit() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let collector = Collector::default();
    let received = collector.0.clone();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(TraceServiceServer::new(collector))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let guard = init_config(
        config(
            "[tracing]\nexport-interval=3600\nsample-ratio=0.0",
            &[
                ("OTEL_EXPORTER_OTLP_ENDPOINT", &endpoint),
                ("DD_ENV", "prod"),
                ("DD_VERSION", "test-version"),
                ("TRACING_LOG_FORMAT", "json"),
            ],
        )
        .unwrap(),
    )
    .unwrap();
    let mut headers = hyper::HeaderMap::new();
    headers.insert(
        "traceparent",
        "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
            .parse()
            .unwrap(),
    );
    let span = tracing::info_span!(parent: None, "http_request");
    attach_parent(&span, &headers);
    {
        let _entered = span.enter();
        assert_eq!(
            span.context().span().span_context().trace_id().to_string(),
            "0123456789abcdef0123456789abcdef"
        );
        tracing::info!("correlated request log");
    }
    drop(span);
    drop(guard);
    let requests = received.lock().unwrap();
    assert!(
        !requests.is_empty(),
        "shutdown must flush without waiting for the scheduled export"
    );
    let resource_spans = &requests[0].resource_spans[0];
    let attributes = &resource_spans.resource.as_ref().unwrap().attributes;
    assert!(attributes.iter().any(|kv| kv.key == "service.name"
        && kv.value.as_ref().unwrap().value == Some(Value::StringValue("cloudbreak-api".into()))));
    assert!(attributes.iter().any(|kv| kv.key == "service.version"
        && kv.value.as_ref().unwrap().value == Some(Value::StringValue("test-version".into()))));
    let exported = &resource_spans.scope_spans[0].spans[0];
    assert_eq!(
        exported.trace_id,
        [
            1, 35, 69, 103, 137, 171, 205, 239, 1, 35, 69, 103, 137, 171, 205, 239
        ]
    );
    assert_eq!(
        exported.parent_span_id,
        [1, 35, 69, 103, 137, 171, 205, 239]
    );
    assert_eq!(exported.name, "http_request");
    server.abort();
}
