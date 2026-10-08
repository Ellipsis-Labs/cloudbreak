use tracing::Subscriber;
use tracing_opentelemetry::OtelData;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;

/// Wraps the default JSON formatter and adds dd.trace_id and dd.span_id fields.
pub struct JsonWithTraceId<F> {
    inner: F,
}

impl<F> JsonWithTraceId<F> {
    pub fn new(inner: F) -> Self {
        Self { inner }
    }
}

impl<S, N, F> FormatEvent<S, N> for JsonWithTraceId<F>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
    F: FormatEvent<S, N>,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        // Get trace context from current span using OtelData helper methods
        let (trace_id, span_id) = if let Some(span_ref) = ctx.lookup_current() {
            let extensions = span_ref.extensions();
            if let Some(otel_data) = extensions.get::<OtelData>() {
                (
                    otel_data.trace_id().map(|tid| tid.to_string()),
                    otel_data.span_id().map(|sid| sid.to_string()),
                )
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        // If we have trace context, capture output, modify JSON, and re-write
        if let (Some(tid), Some(sid)) = (&trace_id, &span_id) {
            // Capture inner formatter output to a string
            let mut buffer = String::new();
            {
                let temp_writer = Writer::new(&mut buffer);
                self.inner.format_event(ctx, temp_writer, event)?;
            }

            // Parse the JSON output (remove trailing newline first)
            let json_str = buffer.trim_end();

            if let Ok(mut json) = serde_json::from_str::<serde_json::Value>(json_str) {
                if let Some(obj) = json.as_object_mut() {
                    obj.insert("dd.trace_id".to_string(), serde_json::json!(tid));
                    obj.insert("dd.span_id".to_string(), serde_json::json!(sid));
                }
                writeln!(writer, "{}", json)?;
                Ok(())
            } else {
                // If parsing fails, just write the original output
                write!(writer, "{}", buffer)?;
                Ok(())
            }
        } else {
            // No trace context, just use inner formatter
            self.inner.format_event(ctx, writer, event)
        }
    }
}
