//! Token-holder selection shared by indexing and atomic account reads.

use serde::{Deserialize, Serialize};
use solana_pubkey::Pubkey;
use spl_token_2022_interface::{extension::StateWithExtensions, state::Account};

use crate::PubkeyDef;

pub const TOKEN_PROGRAM_ID: Pubkey =
    Pubkey::from_str_const("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
pub const TOKEN_2022_PROGRAM_ID: Pubkey =
    Pubkey::from_str_const("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb");

/// A mint and its owning token program, not the mint account itself.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TokenMintFilter {
    pub mint: PubkeyDef,
    #[serde(default = "legacy_token_program", alias = "token-program")]
    pub token_program: PubkeyDef,
}

fn legacy_token_program() -> PubkeyDef {
    PubkeyDef(TOKEN_PROGRAM_ID)
}

impl TokenMintFilter {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.token_program.0 == TOKEN_PROGRAM_ID
                || self.token_program.0 == TOKEN_2022_PROGRAM_ID,
            "tokenProgram must be SPL Token or Token-2022"
        );
        Ok(())
    }

    pub fn matches(&self, owner: &Pubkey, data: &[u8]) -> bool {
        self.token_program.0 == *owner && token_account_mint(owner, data) == Some(self.mint.0)
    }

    /// Discovery subscription for a future filtered feed. Exact-key subscriptions are also
    /// required for known holders, whose closure/reinitialization can stop matching this filter.
    pub fn subscription_filter(
        &self,
    ) -> anyhow::Result<yellowstone_grpc_proto::geyser::SubscribeRequestFilterAccounts> {
        use yellowstone_grpc_proto::geyser::{
            SubscribeRequestFilterAccounts, SubscribeRequestFilterAccountsFilter,
            SubscribeRequestFilterAccountsFilterMemcmp,
            subscribe_request_filter_accounts_filter::Filter,
            subscribe_request_filter_accounts_filter_memcmp::Data,
        };
        self.validate()?;
        Ok(SubscribeRequestFilterAccounts {
            owner: vec![self.token_program.0.to_string()],
            filters: vec![
                SubscribeRequestFilterAccountsFilter {
                    filter: Some(Filter::Memcmp(SubscribeRequestFilterAccountsFilterMemcmp {
                        offset: 0,
                        data: Some(Data::Bytes(self.mint.0.to_bytes().to_vec())),
                    })),
                },
                SubscribeRequestFilterAccountsFilter {
                    filter: Some(Filter::TokenAccountState(true)),
                },
            ],
            ..Default::default()
        })
    }
}

pub fn matches_token_mint_filters(
    filters: &[TokenMintFilter],
    owner: &Pubkey,
    data: &[u8],
) -> bool {
    if !filters
        .iter()
        .any(|filter| filter.token_program.0 == *owner)
    {
        return false;
    }
    let Some(mint) = token_account_mint(owner, data) else {
        return false;
    };
    filters
        .iter()
        .any(|filter| filter.token_program.0 == *owner && filter.mint.0 == mint)
}

/// Borrow the token layout; no Phoenix dependencies or account-data allocation.
pub fn token_account_mint(owner: &Pubkey, data: &[u8]) -> Option<Pubkey> {
    if *owner != TOKEN_PROGRAM_ID && *owner != TOKEN_2022_PROGRAM_ID {
        return None;
    }
    if *owner == TOKEN_PROGRAM_ID && data.len() != 165 {
        return None;
    }
    let account = StateWithExtensions::<Account>::unpack(data).ok()?;
    Some(Pubkey::new_from_array(account.base.mint.to_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_mint_filter_checks_owner_type_and_initialized_or_frozen_state() {
        let mint = Pubkey::new_unique();
        let filter = TokenMintFilter {
            mint: PubkeyDef(mint),
            token_program: legacy_token_program(),
        };
        let mut data = vec![0; 165];
        data[..32].copy_from_slice(mint.as_ref());
        assert!(!filter.matches(&TOKEN_PROGRAM_ID, &data)); // uninitialized
        for state in [1, 2] {
            data[108] = state;
            assert!(filter.matches(&TOKEN_PROGRAM_ID, &data));
        }
        assert!(!filter.matches(&Pubkey::new_unique(), &data));
        assert!(!filter.matches(&TOKEN_2022_PROGRAM_ID, &data));
        let extensions = TokenMintFilter {
            token_program: PubkeyDef(TOKEN_2022_PROGRAM_ID),
            ..filter.clone()
        };
        assert!(extensions.matches(&TOKEN_2022_PROGRAM_ID, &data));
        data.push(2); // Token-2022 AccountType::Account, followed by an empty TLV area
        data.extend_from_slice(&[0; 4]);
        assert!(extensions.matches(&TOKEN_2022_PROGRAM_ID, &data));
        assert!(!filter.matches(&TOKEN_PROGRAM_ID, &data));
        data[165] = 1; // AccountType::Mint
        assert!(!extensions.matches(&TOKEN_2022_PROGRAM_ID, &data));
        assert!(!filter.matches(&TOKEN_PROGRAM_ID, &[0; 82]));
        assert!(!filter.matches(&TOKEN_PROGRAM_ID, &[0; 355]));
        data[..32].fill(0);
        assert!(!extensions.matches(&TOKEN_2022_PROGRAM_ID, &data));
        assert!(
            TokenMintFilter {
                token_program: PubkeyDef(Pubkey::new_unique()),
                ..filter
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn token_mint_filter_supports_rpc_and_toml_names_and_legacy_default() {
        let mint = Pubkey::new_unique();
        let rpc: TokenMintFilter =
            serde_json::from_value(serde_json::json!({"mint":mint.to_string()})).unwrap();
        assert_eq!(rpc.token_program.0, TOKEN_PROGRAM_ID);
        let subscription = rpc.subscription_filter().unwrap();
        assert_eq!(subscription.owner, vec![TOKEN_PROGRAM_ID.to_string()]);
        assert!(subscription.account.is_empty());
        use yellowstone_grpc_proto::geyser::{
            subscribe_request_filter_accounts_filter::Filter,
            subscribe_request_filter_accounts_filter_memcmp::Data,
        };
        assert!(subscription.filters.iter().any(|filter| matches!(&filter.filter, Some(Filter::Memcmp(value)) if value.offset == 0 && value.data == Some(Data::Bytes(mint.to_bytes().to_vec())))));
        assert!(
            subscription
                .filters
                .iter()
                .any(|filter| filter.filter == Some(Filter::TokenAccountState(true)))
        );
        let config: TokenMintFilter = toml::from_str(&format!(
            "mint = '{mint}'\ntoken-program = '{TOKEN_2022_PROGRAM_ID}'"
        ))
        .unwrap();
        assert_eq!(config.token_program.0, TOKEN_2022_PROGRAM_ID);
        assert_eq!(
            serde_json::to_value(config).unwrap()["tokenProgram"],
            TOKEN_2022_PROGRAM_ID.to_string()
        );
    }
}
