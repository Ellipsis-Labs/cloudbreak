//! Opt-in fork boundary: recover download transport failures without restarting the indexer.

use anyhow::{Context, bail, ensure};
use futures::StreamExt;
use reqwest::{Client, StatusCode, header};
use std::{path::Path, time::Duration};
use tokio::{fs::File, io::AsyncWriteExt, time::Instant};

use crate::sidecar::SnapshotData;

const MAX_RETRIES: u32 = 5;

pub(crate) fn enabled(value: Option<&str>) -> anyhow::Result<bool> {
    match value {
        None | Some("false") | Some("0") => Ok(false),
        Some("true") | Some("1") => Ok(true),
        _ => bail!("CLOUDBREAK_SNAPSHOT_DOWNLOAD_RECOVERY must be true, false, 1 or 0"),
    }
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
) -> anyhow::Result<()> {
    tokio::fs::create_dir_all(directory).await?;
    let destination = directory.join(&snapshot.file_name);
    let partial = directory.join(format!("{}.part", snapshot.file_name));
    // Only resume bytes written in this invocation, with the corresponding in-memory ETag.
    let mut file = File::create(&partial).await?;
    let mut transfer = Transfer::default();
    let mut url = snapshot
        .download_url
        .clone()
        .unwrap_or_else(|| format!("{endpoint}/v1/snapshot/{}", snapshot.file_name));
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
        )
        .await;
        let requests = tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap();
        (result, requests, dir)
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
