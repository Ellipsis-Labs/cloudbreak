//! Opt-in fork boundary: recover download transport failures without restarting the indexer.

use anyhow::{Context, bail, ensure};
use futures::StreamExt;
use reqwest::{Client, StatusCode, header};
use std::{path::Path, time::Duration};
use tokio::{fs::File, io::AsyncWriteExt, time::Instant};

use crate::sidecar::SnapshotData;

mod cache;

pub(crate) async fn prepare_cache(pair: &crate::sidecar::SnapshotPair) -> anyhow::Result<()> {
    cache::prepare(Path::new("."), pair).await
}

/// Keep corrupt-cache recovery at the archive boundary, before any database writes.
pub(crate) async fn unpack(
    snapshot: &SnapshotData,
    directory: &Path,
) -> anyhow::Result<crate::sidecar::UnpackedSnapshot> {
    let result = crate::sidecar::unpack_compressed_snapshot(
        directory.join(&snapshot.file_name),
        directory,
        snapshot.slot,
    );
    if let Err(error) = &result {
        invalidate_bad_archive(snapshot, directory, error).await?;
    }
    result
}

async fn invalidate_bad_archive(
    snapshot: &SnapshotData,
    directory: &Path,
    error: &anyhow::Error,
) -> anyhow::Result<()> {
    // Disk-full/permission/device failures do not prove that a valid archive is corrupt.
    let storage_failure = error.chain().any(|cause| {
        cause.downcast_ref::<std::io::Error>().is_some_and(|io| {
            matches!(
                io.kind(),
                std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::StorageFull
            ) || (io.raw_os_error().is_some() && io.kind() != std::io::ErrorKind::NotFound)
        })
    });
    if !storage_failure {
        tracing::warn!(target: "snapshot_cache", "Invalidating snapshot {} after archive validation failed", snapshot.file_name);
        cache::invalidate(snapshot, directory).await?;
        cache::remove(&directory.join("uncompressed_snapshot")).await?;
        cache::sync_directory(directory).await?;
    }
    Ok(())
}

const MAX_RETRIES: u32 = 5;

pub(crate) fn enabled(value: Option<&str>) -> anyhow::Result<bool> {
    flag_enabled(value, "CLOUDBREAK_SNAPSHOT_DOWNLOAD_RECOVERY")
}

fn flag_enabled(value: Option<&str>, name: &str) -> anyhow::Result<bool> {
    match value {
        None | Some("false") | Some("0") => Ok(false),
        Some("true") | Some("1") => Ok(true),
        _ => bail!("{name} must be true, false, 1 or 0"),
    }
}

pub(crate) fn cache_enabled() -> anyhow::Result<bool> {
    cache_requested(
        std::env::var("CLOUDBREAK_SNAPSHOT_CACHE").ok().as_deref(),
        std::env::var("CLOUDBREAK_SNAPSHOT_DOWNLOAD_RECOVERY")
            .ok()
            .as_deref(),
    )
}

fn cache_requested(cache: Option<&str>, recovery: Option<&str>) -> anyhow::Result<bool> {
    let cache = flag_enabled(cache, "CLOUDBREAK_SNAPSHOT_CACHE")?;
    ensure!(
        !cache || enabled(recovery)?,
        "CLOUDBREAK_SNAPSHOT_CACHE requires CLOUDBREAK_SNAPSHOT_DOWNLOAD_RECOVERY"
    );
    Ok(cache)
}

#[derive(Default)]
struct Transfer {
    downloaded: u64,
    total: Option<u64>,
    etag: Option<String>,
}

enum Outcome {
    Complete,
    Retry,
    RefreshUrl,
}

pub(crate) async fn download(
    endpoint: &str,
    tracker: &str,
    snapshot: &SnapshotData,
    directory: &Path,
) -> anyhow::Result<()> {
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(60))
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .no_zstd()
        .build()?;
    download_with_client(
        &client,
        endpoint,
        tracker,
        snapshot,
        directory,
        Duration::from_secs(1),
        cache_enabled()?,
    )
    .await
}

async fn download_with_client(
    client: &Client,
    endpoint: &str,
    tracker: &str,
    snapshot: &SnapshotData,
    directory: &Path,
    retry_delay: Duration,
    use_cache: bool,
) -> anyhow::Result<()> {
    tokio::fs::create_dir_all(directory).await?;
    let destination = directory.join(&snapshot.file_name);
    let partial = directory.join(format!("{}.part", snapshot.file_name));
    let mut url = snapshot
        .download_url
        .clone()
        .unwrap_or_else(|| format!("{endpoint}/v1/snapshot/{}", snapshot.file_name));
    if use_cache {
        cache::sync_directory(
            directory
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new(".")),
        )
        .await?;
        if reuse_with_retries(client, tracker, snapshot, directory, &mut url, retry_delay).await? {
            tracing::info!(target: "snapshot_cache", "Reusing completed snapshot {}", snapshot.file_name);
            return Ok(());
        }
    }
    // Never leave an old completion record describing a replacement archive.
    cache::invalidate(snapshot, directory).await?;
    // Only resume bytes written in this invocation, with the corresponding in-memory ETag.
    let mut file = File::create(&partial).await?;
    let mut transfer = Transfer::default();
    let started = Instant::now();
    let mut refresh = false;
    for attempt in 0..=MAX_RETRIES {
        if attempt > 0 {
            tracing::warn!(target: "snapshot_download_retry", attempt, downloaded_bytes = transfer.downloaded,
                snapshot_type = ?snapshot.snapshot_type, "Retrying snapshot download");
            tokio::time::sleep(retry_delay * (1 << (attempt - 1))).await;
        }
        if refresh {
            match refresh_url(client, tracker, snapshot).await {
                Ok(fresh) => {
                    url = fresh;
                    refresh = false;
                }
                Err(error) => {
                    tracing::warn!(target: "snapshot_download_retry", "Could not refresh snapshot URL: {error:#}");
                    continue;
                }
            }
        }
        match receive(client, &url, &mut file, &mut transfer, snapshot, started).await? {
            Outcome::Complete => {
                file.sync_all().await?;
                drop(file);
                tokio::fs::rename(&partial, &destination).await?;
                if use_cache {
                    cache::sync_directory(directory).await?;
                    cache::publish(
                        &url,
                        snapshot,
                        directory,
                        transfer.downloaded,
                        transfer.etag.as_deref(),
                    )
                    .await?;
                }
                tracing::info!(target: "download_snapshot_file", downloaded_bytes = transfer.downloaded,
                    "File {} downloaded successfully in {} secs ({:?})",
                    snapshot.file_name, started.elapsed().as_secs_f64(), snapshot.snapshot_type);
                return Ok(());
            }
            Outcome::RefreshUrl => refresh = true,
            Outcome::Retry => (),
        }
    }
    bail!(
        "Snapshot download exhausted {MAX_RETRIES} retries after {} bytes ({:?})",
        transfer.downloaded,
        snapshot.snapshot_type
    )
}

async fn reuse_with_retries(
    client: &Client,
    tracker: &str,
    snapshot: &SnapshotData,
    directory: &Path,
    url: &mut String,
    retry_delay: Duration,
) -> anyhow::Result<bool> {
    let mut refresh = false;
    for attempt in 0..=MAX_RETRIES {
        if attempt > 0 {
            tokio::time::sleep(retry_delay * (1 << (attempt - 1))).await;
        }
        if refresh {
            match refresh_url(client, tracker, snapshot).await {
                Ok(fresh) => {
                    *url = fresh;
                    refresh = false;
                }
                Err(error) => {
                    tracing::warn!(target: "snapshot_cache", "Could not refresh cache validation URL: {error:#}");
                    continue;
                }
            }
        }
        match cache::reuse(client, url, snapshot, directory).await? {
            cache::Probe::Hit => return Ok(true),
            cache::Probe::Miss => return Ok(false),
            cache::Probe::RefreshUrl => refresh = true,
            cache::Probe::Retry => (),
        }
        tracing::warn!(target: "snapshot_cache", attempt, "Retrying snapshot cache validation; retaining completed archive");
    }
    bail!("Snapshot cache validation exhausted retries; completed archive retained")
}

async fn receive(
    client: &Client,
    url: &str,
    file: &mut File,
    transfer: &mut Transfer,
    snapshot: &SnapshotData,
    started: Instant,
) -> anyhow::Result<Outcome> {
    if transfer.downloaded > 0 && transfer.etag.is_none() {
        // Without a strong validator, start over rather than mix bytes from different objects.
        file.set_len(0).await?;
        tokio::io::AsyncSeekExt::rewind(file).await?;
        *transfer = Transfer::default();
    }
    let mut request = client.get(url).header(header::ACCEPT_ENCODING, "identity");
    if transfer.downloaded > 0 {
        request = request
            .header(header::RANGE, format!("bytes={}-", transfer.downloaded))
            .header(header::IF_MATCH, transfer.etag.as_ref().unwrap());
    }
    let response = match request.send().await {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(target: "snapshot_download_retry", "Snapshot request failed: {}", error.without_url());
            return Ok(Outcome::Retry);
        }
    };
    let status = response.status();
    if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
        return Ok(Outcome::RefreshUrl);
    }
    if status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS {
        tracing::warn!(target: "snapshot_download_retry", %status, "Snapshot provider returned a retryable status");
        return Ok(Outcome::Retry);
    }
    let etag = response
        .headers()
        .get(header::ETAG)
        .and_then(|v| v.to_str().ok())
        .filter(|v| v.starts_with('"') && v.ends_with('"'))
        .map(str::to_owned);
    let length = response
        .content_length()
        .context("Snapshot response has no Content-Length")?;
    if status == StatusCode::PARTIAL_CONTENT && transfer.downloaded > 0 {
        let range = response
            .headers()
            .get(header::CONTENT_RANGE)
            .context("Resume response has no Content-Range")?
            .to_str()?;
        validate_range(range, transfer.downloaded, transfer.total.unwrap(), length)?;
        ensure!(etag == transfer.etag, "Snapshot ETag changed during resume");
    } else if status == StatusCode::OK {
        if transfer.downloaded > 0 {
            ensure!(
                etag == transfer.etag && Some(length) == transfer.total,
                "Snapshot identity or size changed when the server ignored Range"
            );
        }
        // A server may ignore Range. A full response must replace, never append to, the partial file.
        file.set_len(0).await?;
        tokio::io::AsyncSeekExt::rewind(file).await?;
        *transfer = Transfer {
            downloaded: 0,
            total: Some(length),
            etag,
        };
        ensure!(length > 0, "Snapshot response is empty");
        tracing::info!(target: "download_snapshot_file", size_bytes = length,
            "Starting to download file {} of size: {} MB ({:?}) from endpoint: [signed URL omitted]",
            snapshot.file_name, length / 1024 / 1024, snapshot.snapshot_type);
    } else {
        bail!("Unexpected snapshot download status: {status}");
    }
    let total = transfer.total.unwrap();
    let mut stream = response.bytes_stream();
    let mut last_log = Instant::now();
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(error) => {
                tracing::warn!(target: "snapshot_download_retry", "Snapshot stream interrupted: {}", error.without_url());
                return Ok(Outcome::Retry);
            }
        };
        ensure!(
            chunk.len() as u64 <= total - transfer.downloaded,
            "Snapshot response exceeds expected length"
        );
        file.write_all(&chunk).await?;
        transfer.downloaded += chunk.len() as u64;
        if last_log.elapsed() >= Duration::from_secs(10) {
            let elapsed = started.elapsed().as_secs_f64();
            tracing::info!(target: "snapshot_download_progress", downloaded_bytes = transfer.downloaded,
                total_bytes = total, snapshot_type = ?snapshot.snapshot_type,
                "Progress: {:.1}% ({}/{}) - {} seconds - {:.1} MB/s",
                transfer.downloaded as f64 * 100.0 / total as f64,
                transfer.downloaded / 1024 / 1024, total / 1024 / 1024,
                elapsed, transfer.downloaded as f64 / 1024.0 / 1024.0 / elapsed);
            last_log = Instant::now();
        }
    }
    if transfer.downloaded != total {
        return Ok(Outcome::Retry);
    }
    Ok(Outcome::Complete)
}

fn validate_range(value: &str, offset: u64, total: u64, length: u64) -> anyhow::Result<()> {
    let range = value
        .strip_prefix("bytes ")
        .context("Invalid Content-Range unit")?;
    let (bounds, size) = range.split_once('/').context("Invalid Content-Range")?;
    let (start, end) = bounds
        .split_once('-')
        .context("Invalid Content-Range bounds")?;
    ensure!(
        start.parse::<u64>()? == offset
            && size.parse::<u64>()? == total
            && end.parse::<u64>()?.checked_add(1) == Some(total)
            && length == total - offset,
        "Resume response does not match the requested snapshot range"
    );
    Ok(())
}

async fn refresh_url(
    client: &Client,
    tracker: &str,
    snapshot: &SnapshotData,
) -> anyhow::Result<String> {
    let response = client
        .get(format!("{tracker}/v1/snapshots"))
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .map_err(reqwest::Error::without_url)?;
    ensure!(
        response.status().is_success(),
        "Snapshot tracker returned {}",
        response.status()
    );
    let pairs: serde_json::Value = response.json().await.map_err(reqwest::Error::without_url)?;
    for pair in pairs
        .as_array()
        .context("Snapshot tracker did not return an array")?
    {
        for file in pair
            .get("files")
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
        {
            if file.get("file_name").and_then(|v| v.as_str()) == Some(&snapshot.file_name) {
                return file
                    .get("download_url")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned)
                    .context("Tracker has no signed URL for the selected snapshot");
            }
        }
    }
    bail!(
        "Tracker no longer offers the selected snapshot; refusing to switch snapshots during recovery"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sidecar::SnapshotType;
    use tokio::{io::AsyncReadExt, net::TcpListener};

    async fn server(replies: Vec<String>) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let mut requests = Vec::new();
            for reply in replies {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    assert_eq!(socket.read(&mut byte).await.unwrap(), 1);
                    request.push(byte[0]);
                }
                requests.push(String::from_utf8(request).unwrap().to_lowercase());
                socket.write_all(reply.as_bytes()).await.unwrap();
                socket.shutdown().await.unwrap();
            }
            requests
        });
        (address, task)
    }

    fn response(status: &str, headers: &str, body: &str) -> String {
        format!("HTTP/1.1 {status}\r\nConnection: close\r\n{headers}\r\n{body}")
    }

    fn snapshot(url: &str) -> SnapshotData {
        SnapshotData {
            file_name: "snapshot-1-test.tar.zst".into(),
            base_slot: None,
            slot: 1,
            snapshot_type: SnapshotType::Full,
            download_url: Some(url.into()),
        }
    }

    async fn run(replies: Vec<String>) -> (anyhow::Result<()>, Vec<String>, tempfile::TempDir) {
        let (url, task) = server(replies).await;
        let dir = tempfile::tempdir().unwrap();
        let client = Client::builder().no_proxy().build().unwrap();
        let result = download_with_client(
            &client,
            &url,
            &url,
            &snapshot(&url),
            dir.path(),
            Duration::ZERO,
            true,
        )
        .await;
        let requests = tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap();
        (result, requests, dir)
    }

    #[tokio::test]
    async fn restart_reuses_completed_archive_with_fresh_signed_query() {
        let (url, task) = server(vec![
            response(
                "200 OK",
                "Content-Length: 8\r\nETag: \"same\"\r\n",
                "abcdefgh",
            ),
            response(
                "206 Partial Content",
                "Content-Length: 1\r\nContent-Range: bytes 0-0/8\r\nETag: \"same\"\r\n",
                "a",
            ),
        ])
        .await;
        let dir = tempfile::tempdir().unwrap();
        let client = Client::builder().no_proxy().build().unwrap();
        for query in ["?signature=old", "?signature=new"] {
            download_with_client(
                &client,
                &url,
                &url,
                &snapshot(&format!("{url}{query}")),
                dir.path(),
                Duration::ZERO,
                true,
            )
            .await
            .unwrap();
        }
        let requests = task.await.unwrap();
        assert!(requests[0].starts_with("get "));
        assert!(requests[1].contains("range: bytes=0-0"));
        assert!(requests[1].contains("if-match: \"same\""));
        assert_eq!(
            std::fs::read(dir.path().join("snapshot-1-test.tar.zst")).unwrap(),
            b"abcdefgh"
        );
        let marker =
            std::fs::read_to_string(dir.path().join("snapshot-1-test.tar.zst.complete.json"))
                .unwrap();
        assert!(!marker.contains("signature"));
    }

    #[tokio::test]
    async fn changed_etag_redownloads_completed_archive() {
        let (url, task) = server(vec![
            response(
                "200 OK",
                "Content-Length: 8\r\nETag: \"old\"\r\n",
                "abcdefgh",
            ),
            response("200 OK", "Content-Length: 8\r\nETag: \"new\"\r\n", ""),
            response(
                "200 OK",
                "Content-Length: 8\r\nETag: \"new\"\r\n",
                "ijklmnop",
            ),
        ])
        .await;
        let dir = tempfile::tempdir().unwrap();
        let client = Client::builder().no_proxy().build().unwrap();
        for _ in 0..2 {
            download_with_client(
                &client,
                &url,
                &url,
                &snapshot(&url),
                dir.path(),
                Duration::ZERO,
                true,
            )
            .await
            .unwrap();
        }
        assert_eq!(task.await.unwrap().len(), 3);
        assert_eq!(
            std::fs::read(dir.path().join("snapshot-1-test.tar.zst")).unwrap(),
            b"ijklmnop"
        );
    }

    #[tokio::test]
    async fn unmarked_or_truncated_archives_are_not_reused() {
        let (url, task) = server(vec![
            response(
                "200 OK",
                "Content-Length: 8\r\nETag: \"same\"\r\n",
                "abcdefgh",
            ),
            response(
                "200 OK",
                "Content-Length: 8\r\nETag: \"same\"\r\n",
                "abcdefgh",
            ),
        ])
        .await;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snapshot-1-test.tar.zst");
        std::fs::write(&path, b"partial").unwrap();
        let client = Client::builder().no_proxy().build().unwrap();
        download_with_client(
            &client,
            &url,
            &url,
            &snapshot(&url),
            dir.path(),
            Duration::ZERO,
            true,
        )
        .await
        .unwrap();
        std::fs::write(&path, b"abc").unwrap();
        download_with_client(
            &client,
            &url,
            &url,
            &snapshot(&url),
            dir.path(),
            Duration::ZERO,
            true,
        )
        .await
        .unwrap();
        assert!(task.await.unwrap().iter().all(|r| r.starts_with("get ")));
        assert_eq!(std::fs::read(path).unwrap(), b"abcdefgh");
    }

    #[tokio::test]
    async fn missing_validator_or_mismatched_probe_falls_back_to_download() {
        for probe in [
            response("200 OK", "Content-Length: 8\r\n", ""),
            response(
                "206 Partial Content",
                "Content-Length: 1\r\nContent-Range: bytes 0-0/9\r\nETag: \"same\"\r\n",
                "a",
            ),
        ] {
            let (url, task) = server(vec![
                response(
                    "200 OK",
                    "Content-Length: 8\r\nETag: \"same\"\r\n",
                    "abcdefgh",
                ),
                probe,
                response(
                    "200 OK",
                    "Content-Length: 8\r\nETag: \"same\"\r\n",
                    "abcdefgh",
                ),
            ])
            .await;
            let dir = tempfile::tempdir().unwrap();
            let client = Client::builder().no_proxy().build().unwrap();
            for _ in 0..2 {
                download_with_client(
                    &client,
                    &url,
                    &url,
                    &snapshot(&url),
                    dir.path(),
                    Duration::ZERO,
                    true,
                )
                .await
                .unwrap();
            }
            assert_eq!(task.await.unwrap().len(), 3);
        }
    }

    #[tokio::test]
    async fn different_source_and_corrupt_markers_are_cache_misses() {
        let (url, task) = server(vec![
            response(
                "200 OK",
                "Content-Length: 8\r\nETag: \"same\"\r\n",
                "abcdefgh",
            ),
            response(
                "200 OK",
                "Content-Length: 8\r\nETag: \"same\"\r\n",
                "abcdefgh",
            ),
            response(
                "200 OK",
                "Content-Length: 8\r\nETag: \"same\"\r\n",
                "abcdefgh",
            ),
        ])
        .await;
        let dir = tempfile::tempdir().unwrap();
        let client = Client::builder().no_proxy().build().unwrap();
        for path in ["/old", "/new"] {
            download_with_client(
                &client,
                &url,
                &url,
                &snapshot(&format!("{url}{path}")),
                dir.path(),
                Duration::ZERO,
                true,
            )
            .await
            .unwrap();
        }
        std::fs::write(
            dir.path().join("snapshot-1-test.tar.zst.complete.json"),
            b"invalid",
        )
        .unwrap();
        download_with_client(
            &client,
            &url,
            &url,
            &snapshot(&format!("{url}/new")),
            dir.path(),
            Duration::ZERO,
            true,
        )
        .await
        .unwrap();
        assert!(task.await.unwrap().iter().all(|r| !r.contains("range:")));
    }

    #[tokio::test]
    async fn transient_cache_probes_retry_without_refetching() {
        let (url, task) = server(vec![
            response(
                "200 OK",
                "Content-Length: 8\r\nETag: \"same\"\r\n",
                "abcdefgh",
            ),
            response("503 Service Unavailable", "Content-Length: 0\r\n", ""),
            response("429 Too Many Requests", "Content-Length: 0\r\n", ""),
            "broken response".into(),
            response(
                "206 Partial Content",
                "Content-Length: 1\r\nContent-Range: bytes 0-0/8\r\nETag: \"same\"\r\n",
                "a",
            ),
        ])
        .await;
        let dir = tempfile::tempdir().unwrap();
        let client = Client::builder().no_proxy().build().unwrap();
        for _ in 0..2 {
            download_with_client(
                &client,
                &url,
                &url,
                &snapshot(&url),
                dir.path(),
                Duration::ZERO,
                true,
            )
            .await
            .unwrap();
        }
        let requests = task.await.unwrap();
        assert!(requests[1..].iter().all(|r| r.contains("range: bytes=0-0")));
        assert_eq!(
            std::fs::read(dir.path().join("snapshot-1-test.tar.zst")).unwrap(),
            b"abcdefgh"
        );
    }

    #[tokio::test]
    async fn exhausted_cache_probes_preserve_completed_archive() {
        let mut replies = vec![response(
            "200 OK",
            "Content-Length: 8\r\nETag: \"same\"\r\n",
            "abcdefgh",
        )];
        replies.extend(
            (0..=MAX_RETRIES)
                .map(|_| response("503 Service Unavailable", "Content-Length: 0\r\n", "")),
        );
        let (url, task) = server(replies).await;
        let dir = tempfile::tempdir().unwrap();
        let client = Client::builder().no_proxy().build().unwrap();
        download_with_client(
            &client,
            &url,
            &url,
            &snapshot(&url),
            dir.path(),
            Duration::ZERO,
            true,
        )
        .await
        .unwrap();
        assert!(
            download_with_client(
                &client,
                &url,
                &url,
                &snapshot(&url),
                dir.path(),
                Duration::ZERO,
                true
            )
            .await
            .is_err()
        );
        assert_eq!(task.await.unwrap().len(), (MAX_RETRIES + 2) as usize);
        assert_eq!(
            std::fs::read(dir.path().join("snapshot-1-test.tar.zst")).unwrap(),
            b"abcdefgh"
        );
        assert!(
            dir.path()
                .join("snapshot-1-test.tar.zst.complete.json")
                .exists()
        );
    }

    #[tokio::test]
    async fn expired_cache_url_refreshes_validation_without_refetching() {
        let (url, task) = server(vec![
            response(
                "200 OK",
                "Content-Length: 8\r\nETag: \"same\"\r\n",
                "abcdefgh",
            ),
            response("403 Forbidden", "Content-Length: 0\r\n", ""),
            response(
                "206 Partial Content",
                "Content-Length: 1\r\nContent-Range: bytes 0-0/8\r\nETag: \"same\"\r\n",
                "a",
            ),
        ])
        .await;
        let body = serde_json::json!([{"files":[{"file_name":"snapshot-1-test.tar.zst","download_url":format!("{url}?signature=fresh")}]}]).to_string();
        let (tracker_url, tracker) = server(vec![response(
            "200 OK",
            &format!("Content-Length: {}\r\n", body.len()),
            &body,
        )])
        .await;
        let dir = tempfile::tempdir().unwrap();
        let client = Client::builder().no_proxy().build().unwrap();
        for _ in 0..2 {
            download_with_client(
                &client,
                &url,
                &tracker_url,
                &snapshot(&url),
                dir.path(),
                Duration::ZERO,
                true,
            )
            .await
            .unwrap();
        }
        let requests = task.await.unwrap();
        assert!(requests[2].contains("signature=fresh"));
        assert!(requests[2].contains("range: bytes=0-0"));
        assert_eq!(tracker.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn corrupt_archive_is_evicted_then_refetched() {
        let (url, task) = server(vec![
            response(
                "200 OK",
                "Content-Length: 8\r\nETag: \"same\"\r\n",
                "abcdefgh",
            ),
            response(
                "200 OK",
                "Content-Length: 8\r\nETag: \"same\"\r\n",
                "abcdefgh",
            ),
        ])
        .await;
        let dir = tempfile::tempdir().unwrap();
        let client = Client::builder().no_proxy().build().unwrap();
        download_with_client(
            &client,
            &url,
            &url,
            &snapshot(&url),
            dir.path(),
            Duration::ZERO,
            true,
        )
        .await
        .unwrap();
        let archive = dir.path().join("snapshot-1-test.tar.zst");
        std::fs::write(&archive, b"badbytes").unwrap();
        assert!(unpack(&snapshot(&url), dir.path()).await.is_err());
        assert!(!archive.exists());
        assert!(
            !dir.path()
                .join("snapshot-1-test.tar.zst.complete.json")
                .exists()
        );
        download_with_client(
            &client,
            &url,
            &url,
            &snapshot(&url),
            dir.path(),
            Duration::ZERO,
            true,
        )
        .await
        .unwrap();
        assert_eq!(task.await.unwrap().len(), 2);
        assert_eq!(std::fs::read(archive).unwrap(), b"abcdefgh");
    }

    #[tokio::test]
    async fn storage_failure_does_not_evict_valid_archive() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot = snapshot("https://example.com");
        let archive = dir.path().join(&snapshot.file_name);
        std::fs::write(&archive, b"cached").unwrap();
        for errno in [13, 28, 5] {
            let error = anyhow::Error::new(std::io::Error::from_raw_os_error(errno));
            invalidate_bad_archive(&snapshot, dir.path(), &error)
                .await
                .unwrap();
            assert_eq!(std::fs::read(&archive).unwrap(), b"cached");
        }
    }

    #[tokio::test]
    async fn disabled_cache_keeps_transport_recovery_without_cache_reads() {
        let (url, task) = server(vec![response(
            "200 OK",
            "Content-Length: 8\r\nETag: \"same\"\r\n",
            "abcdefgh",
        )])
        .await;
        let dir = tempfile::tempdir().unwrap();
        let client = Client::builder().no_proxy().build().unwrap();
        std::fs::write(
            dir.path().join("snapshot-1-test.tar.zst.complete.json"),
            b"invalid",
        )
        .unwrap();
        download_with_client(
            &client,
            &url,
            &url,
            &snapshot(&url),
            dir.path(),
            Duration::ZERO,
            false,
        )
        .await
        .unwrap();
        assert_eq!(task.await.unwrap().len(), 1);
        assert!(
            !dir.path()
                .join("snapshot-1-test.tar.zst.complete.json")
                .exists()
        );
    }

    #[test]
    fn cache_requires_both_explicit_flags() {
        assert!(!cache_requested(None, Some("true")).unwrap());
        assert!(!cache_requested(Some("false"), Some("true")).unwrap());
        assert!(cache_requested(Some("true"), Some("1")).unwrap());
        assert!(cache_requested(Some("true"), None).is_err());
        assert!(cache_requested(Some("true"), Some("false")).is_err());
        assert!(cache_requested(Some("invalid"), Some("true")).is_err());
    }

    #[tokio::test]
    async fn malformed_etag_in_valid_json_is_a_cache_miss() {
        let (url, task) = server(vec![response(
            "200 OK",
            "Content-Length: 8\r\nETag: \"same\"\r\n",
            "abcdefgh",
        )])
        .await;
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("snapshot-1-test.tar.zst"), b"abcdefgh").unwrap();
        let record = serde_json::json!({"filename":"snapshot-1-test.tar.zst","slot":1,"source":format!("{url}/"),"etag":"\"bad\netag\"","bytes":8});
        std::fs::write(
            dir.path().join("snapshot-1-test.tar.zst.complete.json"),
            record.to_string(),
        )
        .unwrap();
        let client = Client::builder().no_proxy().build().unwrap();
        download_with_client(
            &client,
            &url,
            &url,
            &snapshot(&url),
            dir.path(),
            Duration::ZERO,
            true,
        )
        .await
        .unwrap();
        let requests = task.await.unwrap();
        assert_eq!(requests.len(), 1);
        assert!(!requests[0].contains("range:"));
    }

    #[test]
    fn opt_in_preserves_default_and_rejects_typos() {
        assert!(!enabled(None).unwrap());
        assert!(!enabled(Some("false")).unwrap());
        assert!(enabled(Some("true")).unwrap());
        assert!(enabled(Some("ture")).is_err());
    }

    #[tokio::test]
    async fn resumes_truncated_body_with_range_and_validator() {
        let (result, requests, dir) = run(vec![
            response("200 OK", "Content-Length: 8\r\nETag: \"same\"\r\n", "abc"),
            response(
                "206 Partial Content",
                "Content-Length: 5\r\nContent-Range: bytes 3-7/8\r\nETag: \"same\"\r\n",
                "defgh",
            ),
        ])
        .await;
        result.unwrap();
        assert!(requests[1].contains("range: bytes=3-"));
        assert!(requests[1].contains("if-match: \"same\""));
        assert_eq!(
            std::fs::read(dir.path().join("snapshot-1-test.tar.zst")).unwrap(),
            b"abcdefgh"
        );
        assert!(!dir.path().join("snapshot-1-test.tar.zst.part").exists());
    }

    #[tokio::test]
    async fn ignored_range_replaces_partial_instead_of_appending() {
        let (result, _, dir) = run(vec![
            response("200 OK", "Content-Length: 8\r\nETag: \"same\"\r\n", "abc"),
            response(
                "200 OK",
                "Content-Length: 8\r\nETag: \"same\"\r\n",
                "abcdefgh",
            ),
        ])
        .await;
        result.unwrap();
        assert_eq!(
            std::fs::read(dir.path().join("snapshot-1-test.tar.zst")).unwrap(),
            b"abcdefgh"
        );
    }

    #[tokio::test]
    async fn ignored_range_with_changed_object_is_rejected() {
        let (result, _, dir) = run(vec![
            response("200 OK", "Content-Length: 8\r\nETag: \"same\"\r\n", "abc"),
            response(
                "200 OK",
                "Content-Length: 8\r\nETag: \"changed\"\r\n",
                "abcdefgh",
            ),
        ])
        .await;
        assert!(result.is_err());
        assert!(!dir.path().join("snapshot-1-test.tar.zst").exists());
    }

    #[tokio::test]
    async fn no_validator_restarts_download_without_range() {
        let (result, requests, dir) = run(vec![
            response("200 OK", "Content-Length: 8\r\n", "abc"),
            response("200 OK", "Content-Length: 8\r\n", "abcdefgh"),
        ])
        .await;
        result.unwrap();
        assert!(!requests[1].contains("range:"));
        assert_eq!(
            std::fs::read(dir.path().join("snapshot-1-test.tar.zst")).unwrap(),
            b"abcdefgh"
        );
    }

    #[tokio::test]
    async fn invalid_range_and_changed_etag_never_publish_partial_file() {
        for (range, etag) in [("bytes 2-6/8", "same"), ("bytes 3-7/8", "changed")] {
            let (result, _, dir) = run(vec![
                response("200 OK", "Content-Length: 8\r\nETag: \"same\"\r\n", "abc"),
                response(
                    "206 Partial Content",
                    &format!("Content-Length: 5\r\nContent-Range: {range}\r\nETag: \"{etag}\"\r\n"),
                    "defgh",
                ),
            ])
            .await;
            assert!(result.is_err());
            assert!(!dir.path().join("snapshot-1-test.tar.zst").exists());
        }
    }

    #[tokio::test]
    async fn expires_url_refreshes_same_filename_and_resumes() {
        // A separate tracker supplies a fresh URL for the same object.
        let (download_url, downloads) = server(vec![
            response("200 OK", "Content-Length: 8\r\nETag: \"same\"\r\n", "abc"),
            response("403 Forbidden", "Content-Length: 0\r\n", ""),
            response(
                "206 Partial Content",
                "Content-Length: 5\r\nContent-Range: bytes 3-7/8\r\nETag: \"same\"\r\n",
                "defgh",
            ),
        ])
        .await;
        let body = serde_json::json!([{"files":[{"file_name":"snapshot-1-test.tar.zst","download_url":format!("{download_url}/fresh")}]}]).to_string();
        let (tracker_url, tracker) = server(vec![response(
            "200 OK",
            &format!(
                "Content-Length: {}\r\nContent-Type: application/json\r\n",
                body.len()
            ),
            &body,
        )])
        .await;
        let dir = tempfile::tempdir().unwrap();
        let client = Client::builder().no_proxy().build().unwrap();
        download_with_client(
            &client,
            &download_url,
            &tracker_url,
            &snapshot(&download_url),
            dir.path(),
            Duration::ZERO,
            true,
        )
        .await
        .unwrap();
        let requests = downloads.await.unwrap();
        assert!(requests[2].starts_with("get /fresh "));
        assert!(requests[2].contains("range: bytes=3-"));
        assert!(tracker.await.unwrap()[0].starts_with("get /v1/snapshots "));
        assert_eq!(
            std::fs::read(dir.path().join("snapshot-1-test.tar.zst")).unwrap(),
            b"abcdefgh"
        );
    }

    #[tokio::test]
    async fn retries_are_bounded_and_no_file_is_published() {
        let (result, requests, dir) = run(vec![
            response(
                "503 Service Unavailable",
                "Content-Length: 0\r\n",
                ""
            );
            6
        ])
        .await;
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("exhausted 5 retries")
        );
        assert_eq!(requests.len(), 6);
        assert!(!dir.path().join("snapshot-1-test.tar.zst").exists());
    }

    #[tokio::test]
    async fn url_refresh_does_not_switch_to_a_new_snapshot() {
        let body = r#"[{"files":[{"file_name":"snapshot-2-other.tar.zst","download_url":"http://example.invalid"}]}]"#;
        let (url, server) = server(vec![response(
            "200 OK",
            &format!("Content-Length: {}\r\n", body.len()),
            body,
        )])
        .await;
        let client = Client::builder().no_proxy().build().unwrap();
        assert!(
            refresh_url(&client, &url, &snapshot(&url))
                .await
                .unwrap_err()
                .to_string()
                .contains("refusing to switch snapshots")
        );
        server.await.unwrap();
    }
}
