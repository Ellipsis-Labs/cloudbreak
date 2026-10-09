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

## Completed archive cache and PVC cleanup

Recovery also reuses completed downloads across restarts. A completed archive is
synced and atomically renamed before a completion marker is published. The marker
records filename, slot, source URL without credentials/query/fragment, byte length
and strong ETag. Files without a marker (including downloads from older images),
partial files, malformed markers and size mismatches are never reused.

Reuse probes the selected remote object with `GET`, `Range: bytes=0-0` and
`If-Match`. The ETag and total length must still match. This works with URLs signed
for GET, whose signatures may reject HEAD. Changing signed query parameters does
not invalidate the resource identity. Missing validators, failed probes or changed
sources cause a fresh download. No cross-process partial-download resume is added.

After the tracker selects a startup pair covering the new received slot, the
indexer retains only that pair's named archives and markers. Obsolete numeric
`snapshot_<slot>` and timestamped snapshot directories are deleted, along with
partial files, obsolete filenames and extraction directories in retained slots.
Cleanup happens before the full/incremental tasks start and does not follow
symlinks. A new incremental can reuse its unchanged full base; a different full
base removes the old cache. Run one indexer per snapshot directory/PVC.

## Pre-populating the PVC

A separate tool can download/upload archives before an indexer rollout without
changing Cloudbreak. Place the archive under `/data/snapshot_<slot>/<filename>`
and publish one sibling `<filename>.complete.json` only after the upload completes:

```json
{
  "filename": "snapshot-123-example.tar.zst",
  "slot": 123,
  "source": "https://provider.example/mainnet-beta/snapshot-123-example.tar.zst",
  "etag": "\"the-provider-strong-etag\"",
  "bytes": 123456789
}
```

Use the ETag and full Content-Length observed while downloading that exact object,
not values guessed from a pre-existing local file. `source` is the selected download
URL with query, fragment and user credentials removed. Upload to a temporary name,
then atomically rename the complete archive and finally its marker. The ETag is an
opaque object validator; multipart ETags are not local-file MD5 checksums.

The marker is a trusted assertion from the downloader that it finished transferring
the object. Cloudbreak checks local length and remote identity; it does not hash
all archive bytes on restart. A bare archive is intentionally not reused. If an
upload tool cannot publish this record, Cloudbreak downloads once to establish it.
The tracker must still select the matching full base in a pair covering the new
startup slot; uploading an old full/incremental pair does not force its selection.
Unpacking, ingestion and database startup cleanup still run after cache reuse.

## Deployment boundary

The startup script must preserve `/data/snapshot_*` when recovery is enabled.
Deploy an image containing this cache implementation before enabling retention;
the old recovery implementation has no startup cache pruning. Keep the original
snapshot deletion when recovery is disabled. Tracker-response files can still be
removed on every start.

Database rebuild policy is unchanged: `cloudbreak-migration fresh`, archive
unpacking, account ingestion, indexes and startup cleanup still run. Cache reuse
saves download time, not total bootstrap time. The service remains unhealthy
until processing and cleanup complete. A rollout therefore still rebuilds the
index; validate on an inactive independent stack or allow bootstrap before routing
traffic. No live deployment is required to validate the cache with local fixtures.

## Checks

`cargo test -p cloudbreak-snapshot download_recovery` uses local HTTP fixtures for
truncated bodies, validated range resumption, ignored ranges, missing validators,
changed ETags, wrong offsets, expiring URLs, snapshot identity and retry limits.
These fixtures do not verify an external provider's live range behavior.

Cache fixtures also cover restart reuse with renewed signed queries, remote ETag
changes, truncated/unmarked archives, corrupt markers, changed sources, failed
probes, selected-pair PVC pruning and symlink-safe cleanup.
