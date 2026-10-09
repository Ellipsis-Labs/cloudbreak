# Opt-in snapshot download recovery

Set `CLOUDBREAK_SNAPSHOT_DOWNLOAD_RECOVERY=true` on the indexer (or standalone
snapshot command) to retry transport failures without restarting the process.
Unset, `false`, or `0` preserves the existing upstream downloader. `1` also enables
recovery; other values fail with a configuration error.

```yaml
env:
  - name: CLOUDBREAK_SNAPSHOT_DOWNLOAD_RECOVERY
    value: "true"
```

This requires an image containing the recovery implementation. The flag is read
when each snapshot download starts. Removing it or setting it to `false` selects
the upstream implementation on the next download.

## Recovery behavior

- One initial attempt and at most five retries, with 1/2/4/8/16-second backoff.
- Connections time out after 10 seconds; stalled reads time out after 60 seconds.
  There is no total download timeout for large full snapshots.
- A partial file stays in place across retries within the same invocation.
  Resumption sends `Range` and `If-Match` with a strong ETag. The response must
  match the offset, total size, remaining length and original ETag.
- If there is no strong ETag, or a server ignores the Range header and returns a
  complete response, the file is downloaded from the beginning. Bytes are never
  appended to an incompatible response.
- HTTP 401/403 refreshes the signed URL through the configured tracker. The new
  URL must be for the exact selected filename. Recovery never substitutes a
  newer full or incremental snapshot. If that file is no longer offered,
  recovery eventually fails.
- The archive is written as `.part`, checked against Content-Length, synced,
  then renamed to its normal filename. Unpacking only begins after success.
- Network/body failures, HTTP 429 and HTTP 5xx are retried. Disk errors and
  inconsistent range/identity responses fail immediately. Archive validation
  and database processing failures retain their existing behavior.

Retry and progress logs include byte counts and snapshot type. Signed URLs are
omitted from request errors. Existing download-size/progress message formats are
retained for dashboard parsing.

## Deployment boundary

This recovers downloads **inside a running process**, not across pod/container
restarts. The current infrastructure startup script deletes snapshot directories
and runs `cloudbreak-migration fresh` on every start. This PR does not change
those database rebuild or restart policies, disable health checks, or enable
recovery in production.

A rollout therefore still rebuilds the index. Allow it to bootstrap before
routing traffic, or validate on an inactive independent stack. After enabling,
a transient download failure should log a retry and retain the byte offset;
container restart counts should remain unchanged. The service remains unhealthy
until snapshot processing and cleanup finish. Exhausted retries still propagate
to the existing restart path.

## Checks

`cargo test -p cloudbreak-snapshot download_recovery` uses local HTTP fixtures for
truncated bodies, validated range resumption, ignored ranges, missing validators,
changed ETags, wrong offsets, expiring URLs, snapshot identity and retry limits.
These fixtures do not verify an external provider's live range behavior.
