//! Versioned, program-independent GPA snapshots, compressed as one zstd frame.

use std::{collections::HashSet, io::Read};

use wincode::{SchemaRead, SchemaWrite};

pub const SNAPSHOT_MAGIC: [u8; 8] = *b"CBPHX001";
pub const SNAPSHOT_VERSION: u16 = 1;
/// Bound decompression and individual wincode allocations on client input.
pub const MAX_SNAPSHOT_BYTES: usize = 2 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct SnapshotAccount {
    pub pubkey: [u8; 32],
    pub owner: [u8; 32],
    pub lamports: u64,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct AccountSnapshot {
    pub slot: u64,
    /// Last transaction index at the complete slot boundary; None for an empty block.
    pub slot_index: Option<u64>,
    pub transaction_count: u64,
    pub blockhash: String,
    pub programs: Vec<ProgramAccounts>,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct ProgramAccounts {
    pub program_id: [u8; 32],
    pub accounts: Vec<SnapshotAccount>,
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("snapshot exceeds the {MAX_SNAPSHOT_BYTES}-byte limit")]
    TooLarge,
    #[error("invalid snapshot magic or version")]
    InvalidVersion,
    #[error("snapshot accounts must be unique and sorted by pubkey")]
    InvalidOrder,
    #[error("snapshot I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("snapshot encoding: {0}")]
    Write(#[from] wincode::WriteError),
    #[error("snapshot decoding: {0}")]
    Read(#[from] wincode::ReadError),
}

fn config() -> impl wincode::config::Config {
    wincode::config::Configuration::default().with_preallocation_size_limit::<MAX_SNAPSHOT_BYTES>()
}

fn validate_order(snapshot: &AccountSnapshot) -> Result<(), SnapshotError> {
    if snapshot.slot_index != snapshot.transaction_count.checked_sub(1)
        || snapshot
            .programs
            .windows(2)
            .any(|p| p[0].program_id >= p[1].program_id)
        || snapshot.programs.iter().any(|group| {
            group
                .accounts
                .windows(2)
                .any(|a| a[0].pubkey >= a[1].pubkey)
                || group.accounts.iter().any(|a| a.owner != group.program_id)
        })
    {
        return Err(SnapshotError::InvalidOrder);
    }
    let mut keys = HashSet::new();
    if snapshot
        .programs
        .iter()
        .flat_map(|p| &p.accounts)
        .any(|a| !keys.insert(a.pubkey))
    {
        return Err(SnapshotError::InvalidOrder);
    }
    Ok(())
}

/// Encode directly into zstd, avoiding a second uncompressed snapshot buffer.
pub fn encode_snapshot(snapshot: &AccountSnapshot) -> Result<Vec<u8>, SnapshotError> {
    validate_order(snapshot)?;
    let envelope = (SNAPSHOT_MAGIC, SNAPSHOT_VERSION, snapshot);
    if wincode::config::serialized_size(&envelope, config())? > MAX_SNAPSHOT_BYTES as u64 {
        return Err(SnapshotError::TooLarge);
    }
    let mut encoder = zstd::stream::Encoder::new(Vec::new(), 3)?;
    wincode::config::serialize_into(
        wincode::io::std_write::WriteAdapter::new(&mut encoder),
        &envelope,
        config(),
    )?;
    Ok(encoder.finish()?)
}

/// Decode a zstd payload after removing the JSON result's base64 wrapper.
pub fn decode_snapshot(compressed: &[u8]) -> Result<AccountSnapshot, SnapshotError> {
    let mut bytes = Vec::new();
    zstd::stream::Decoder::new(compressed)?
        .take(MAX_SNAPSHOT_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_SNAPSHOT_BYTES {
        return Err(SnapshotError::TooLarge);
    }
    // Check the fixed prefix before allowing any variable-length allocation.
    if bytes.get(..8) != Some(SNAPSHOT_MAGIC.as_slice())
        || bytes.get(8..10) != Some(SNAPSHOT_VERSION.to_le_bytes().as_slice())
    {
        return Err(SnapshotError::InvalidVersion);
    }
    let (_, _, snapshot): ([u8; 8], u16, AccountSnapshot) =
        wincode::config::deserialize_exact(&bytes, config())?;
    validate_order(&snapshot)?;
    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn snapshot() -> AccountSnapshot {
        AccountSnapshot {
            slot: 987,
            slot_index: Some(2),
            transaction_count: 3,
            blockhash: "block-hash".into(),
            programs: vec![ProgramAccounts {
                program_id: [9; 32],
                accounts: vec![SnapshotAccount {
                    pubkey: [1; 32],
                    owner: [9; 32],
                    lamports: 123,
                    executable: true,
                    rent_epoch: u64::MAX,
                    data: vec![7; 5 * 1024 * 1024],
                }],
            }],
        }
    }
    #[test]
    fn preserves_large_accounts_metadata_groups_and_transaction_boundary() {
        let snapshot = snapshot();
        let compressed = encode_snapshot(&snapshot).unwrap();
        assert!(compressed.len() < 1024);
        assert_eq!(decode_snapshot(&compressed).unwrap(), snapshot);
    }
    #[test]
    fn validates_version_and_rejects_trailing_bytes() {
        let mut snapshot = snapshot();
        snapshot.programs.clear();
        let mut bytes = zstd::decode_all(encode_snapshot(&snapshot).unwrap().as_slice()).unwrap();
        assert_eq!(&bytes[..10], b"CBPHX001\x01\x00");
        assert_eq!(&bytes[10..18], &987u64.to_le_bytes());
        bytes[8] = 2;
        assert!(matches!(
            decode_snapshot(&zstd::encode_all(bytes.as_slice(), 3).unwrap()),
            Err(SnapshotError::InvalidVersion)
        ));
        bytes[8] = 1;
        bytes.push(0);
        assert!(decode_snapshot(&zstd::encode_all(bytes.as_slice(), 3).unwrap()).is_err());
    }
    #[test]
    fn rejects_duplicate_keys_wrong_group_and_invented_position() {
        let mut s = snapshot();
        let duplicate = s.programs[0].accounts[0].clone();
        s.programs[0].accounts.push(duplicate);
        assert!(encode_snapshot(&s).is_err());
        s.programs[0].accounts.pop();
        s.programs[0].program_id = [3; 32];
        assert!(encode_snapshot(&s).is_err());
        s.programs.clear();
        s.slot_index = Some(3);
        assert!(encode_snapshot(&s).is_err());
        s.transaction_count = 0;
        s.slot_index = None;
        assert_eq!(decode_snapshot(&encode_snapshot(&s).unwrap()).unwrap(), s);
    }
}
