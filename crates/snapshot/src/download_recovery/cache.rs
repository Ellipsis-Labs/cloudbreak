//! Restart cache and PVC cleanup kept inside the opt-in download-recovery fork boundary.

use std::{io::ErrorKind, path::Path};

use anyhow::ensure;
use reqwest::{Client, StatusCode, Url, header};
use serde::{Deserialize, Serialize};

use crate::sidecar::{SnapshotData, SnapshotPair};

#[derive(Serialize, Deserialize)]
struct Completion {
    filename: String,
    slot: u64,
    source: String,
    etag: String,
    bytes: u64,
}

fn source(url: &str) -> anyhow::Result<String> {
    let mut url = Url::parse(url)?;
    url.set_query(None);
    url.set_fragment(None);
    url.set_username("").ok();
    url.set_password(None).ok();
    Ok(url.into())
}

fn marker(snapshot: &SnapshotData) -> String {
    format!("{}.complete.json", snapshot.file_name)
}

pub(super) async fn remove(path: &Path) -> anyhow::Result<()> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.is_dir() => tokio::fs::remove_dir_all(path).await?,
        Ok(_) => tokio::fs::remove_file(path).await?,
        Err(error) if error.kind() == ErrorKind::NotFound => (),
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

/// Run once after selecting the startup pair, before either download is spawned.
pub(super) async fn prepare(root: &Path, pair: &SnapshotPair) -> anyhow::Result<()> {
    let selected = std::iter::once(&pair.full_snapshot).chain(pair.incremental_snapshot.iter());
    let selected: Vec<_> = selected.collect();
    for snapshot in &selected {
        ensure!(
            Path::new(&snapshot.file_name)
                .file_name()
                .and_then(|p| p.to_str())
                == Some(&snapshot.file_name),
            "Snapshot filename must be a basename"
        );
    }
    let mut entries = tokio::fs::read_dir(root).await?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(suffix) = name.strip_prefix("snapshot_") else {
            continue;
        };
        if !suffix
            .split('_')
            .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
        {
            continue;
        }
        let snapshot = selected
            .iter()
            .find(|s| name == format!("snapshot_{}", s.slot));
        let is_directory = entry.file_type().await?.is_dir();
        if let Some(snapshot) = snapshot.filter(|_| is_directory) {
            let mut contents = tokio::fs::read_dir(entry.path()).await?;
            while let Some(file) = contents.next_entry().await? {
                if file.file_name() != snapshot.file_name.as_str()
                    && file.file_name() != marker(snapshot).as_str()
                {
                    remove(&file.path()).await?;
                }
            }
        } else {
            tracing::info!(target: "snapshot_cache", "Removing obsolete snapshot directory {}", name);
            remove(&entry.path()).await?;
        }
    }
    Ok(())
}

#[derive(Debug, PartialEq)]
pub(super) enum Probe {
    Hit,
    Miss,
    Retry,
    RefreshUrl,
}

pub(super) async fn reuse(
    client: &Client,
    url: &str,
    snapshot: &SnapshotData,
    directory: &Path,
) -> anyhow::Result<Probe> {
    let record = match tokio::fs::read(directory.join(marker(snapshot))).await {
        Ok(bytes) => match serde_json::from_slice::<Completion>(&bytes) {
            Ok(record) => record,
            Err(_) => return Ok(Probe::Miss),
        },
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Probe::Miss),
        Err(error) => return Err(error.into()),
    };
    if record.filename != snapshot.file_name
        || record.slot != snapshot.slot
        || record.source != source(url)?
        || record.bytes == 0
        || !record.etag.starts_with('"')
        || !record.etag.ends_with('"')
        || header::HeaderValue::from_str(&record.etag).is_err()
    {
        return Ok(Probe::Miss);
    }
    let metadata = match tokio::fs::symlink_metadata(directory.join(&snapshot.file_name)).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Probe::Miss),
        Err(error) => return Err(error.into()),
    };
    if !metadata.is_file() || metadata.len() != record.bytes {
        return Ok(Probe::Miss);
    }
    // A presigned GET URL may reject HEAD. Probe without transferring the archive.
    // Do not evict a completed archive while its remote identity is uncertain.
    let response = match client
        .get(url)
        .header(header::ACCEPT_ENCODING, "identity")
        .header(header::RANGE, "bytes=0-0")
        .header(header::IF_MATCH, &record.etag)
        .timeout(std::time::Duration::from_secs(30))
        .send()
        .await
    {
        Ok(response) => response,
        Err(_) => return Ok(Probe::Retry),
    };
    if matches!(
        response.status(),
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
    ) {
        return Ok(Probe::RefreshUrl);
    }
    if response.status().is_server_error() || response.status() == StatusCode::TOO_MANY_REQUESTS {
        return Ok(Probe::Retry);
    }
    let length = response
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    let matching_size = match response.status() {
        StatusCode::OK => length == Some(record.bytes),
        StatusCode::PARTIAL_CONTENT => {
            length == Some(1)
                && response
                    .headers()
                    .get(header::CONTENT_RANGE)
                    .and_then(|v| v.to_str().ok())
                    == Some(format!("bytes 0-0/{}", record.bytes).as_str())
        }
        _ => false,
    };
    let matches = matching_size
        && response
            .headers()
            .get(header::ETAG)
            .and_then(|v| v.to_str().ok())
            == Some(record.etag.as_str());
    Ok(if matches { Probe::Hit } else { Probe::Miss })
}

/// Persist directory entries after rename so completed downloads survive node failures.
pub(super) async fn sync_directory(directory: &Path) -> anyhow::Result<()> {
    tokio::fs::File::open(directory).await?.sync_all().await?;
    Ok(())
}

pub(super) async fn publish(
    url: &str,
    snapshot: &SnapshotData,
    directory: &Path,
    bytes: u64,
    etag: Option<&str>,
) -> anyhow::Result<()> {
    // Unvalidated downloads remain usable now, but are never reused after a restart.
    let Some(etag) = etag else { return Ok(()) };
    let record = Completion {
        filename: snapshot.file_name.clone(),
        slot: snapshot.slot,
        source: source(url)?,
        etag: etag.into(),
        bytes,
    };
    let path = directory.join(marker(snapshot));
    let temporary = path.with_extension("json.part");
    tokio::fs::write(&temporary, serde_json::to_vec(&record)?).await?;
    tokio::fs::File::open(&temporary).await?.sync_all().await?;
    tokio::fs::rename(temporary, path).await?;
    sync_directory(directory).await
}

pub(super) async fn invalidate(snapshot: &SnapshotData, directory: &Path) -> anyhow::Result<()> {
    remove(&directory.join(marker(snapshot))).await?;
    remove(&directory.join(&snapshot.file_name)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sidecar::SnapshotType;

    fn pair() -> SnapshotPair {
        SnapshotPair {
            full_snapshot: SnapshotData {
                file_name: "snapshot-1-new.tar.zst".into(),
                base_slot: None,
                slot: 1,
                snapshot_type: SnapshotType::Full,
                download_url: None,
            },
            incremental_snapshot: Some(SnapshotData {
                file_name: "incremental-snapshot-1-2-new.tar.zst".into(),
                base_slot: Some(1),
                slot: 2,
                snapshot_type: SnapshotType::Incremental,
                download_url: None,
            }),
            downloading_endpoint: "https://example.com".into(),
        }
    }

    #[tokio::test]
    async fn selected_pair_retains_only_matching_archives_and_cleans_pvc() {
        let root = tempfile::tempdir().unwrap();
        for name in [
            "snapshot_1",
            "snapshot_2",
            "snapshot_3",
            "snapshot_3_123",
            "snapshot_notes",
        ] {
            std::fs::create_dir(root.path().join(name)).unwrap();
        }
        let pair = pair();
        let full = root.path().join("snapshot_1");
        for filename in [
            &pair.full_snapshot.file_name,
            &marker(&pair.full_snapshot),
            "snapshot-1-old.tar.zst",
            "snapshot-1-new.tar.zst.part",
        ] {
            std::fs::write(full.join(filename), b"cached").unwrap();
        }
        std::fs::create_dir(full.join("uncompressed_snapshot")).unwrap();
        std::fs::write(
            root.path()
                .join("snapshot_2")
                .join(&pair.incremental_snapshot.as_ref().unwrap().file_name),
            b"incremental",
        )
        .unwrap();
        prepare(root.path(), &pair).await.unwrap();
        assert!(full.join(&pair.full_snapshot.file_name).exists());
        assert!(full.join(marker(&pair.full_snapshot)).exists());
        assert_eq!(std::fs::read_dir(full).unwrap().count(), 2);
        assert!(root.path().join("snapshot_2").exists());
        assert!(!root.path().join("snapshot_3").exists());
        assert!(!root.path().join("snapshot_3_123").exists());
        assert!(root.path().join("snapshot_notes").exists());
        let mut different = pair.clone();
        different.full_snapshot.slot = 4;
        different.incremental_snapshot = None;
        prepare(root.path(), &different).await.unwrap();
        assert!(!root.path().join("snapshot_1").exists());
        assert!(!root.path().join("snapshot_2").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cleanup_unlinks_symlinks_without_following_them() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("keep"), b"keep").unwrap();
        std::os::unix::fs::symlink(outside.path(), root.path().join("snapshot_1")).unwrap();
        prepare(root.path(), &pair()).await.unwrap();
        assert!(!root.path().join("snapshot_1").exists());
        assert!(outside.path().join("keep").exists());
    }
}
