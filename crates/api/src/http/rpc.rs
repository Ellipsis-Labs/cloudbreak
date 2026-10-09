// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use bytes::Bytes;
use cloudbreak_core::modules::rpc_filter_type::RpcProgramAccountsConfig;
use http_body_util::combinators::UnsyncBoxBody;
use hyper::body::Incoming;
use hyper::{Request, StatusCode};
use serde::Serialize;
use solana_commitment_config::CommitmentConfig;
use solana_rpc_client_api::config::{
    RpcAccountInfoConfig, RpcContextConfig, RpcSimulateTransactionConfig, RpcSupplyConfig,
};
use std::io;
use std::sync::Arc;
use tokio::time::Instant;

use crate::error::RpcError;
use crate::http::CloudbreakRpcState;
use crate::http::server::{HttpHandlerResponse, ResponseBody};
use crate::http::streaming::gpa_streaming_response_body;
use crate::http::{
    JsonRpcRequest, JsonRpcResponse, RequestContext, RpcRequestPayload, extract_optional_param,
    extract_param, http_status_for_error, make_error_response, make_rpc_error_response,
};
use crate::methods::slot::RpcGetSlotConfig;
use crate::methods::token::{
    TokenAccountsFilter, TokenQueryType, get_token_accounts_by_owner_or_delegate,
};
use crate::{db_query, methods, metrics};

pub async fn handle_rpc_request(
    req: Request<Incoming>,
    state: Arc<CloudbreakRpcState>,
    ctx: &Arc<RequestContext>,
) -> HttpHandlerResponse {
    let body = match http_body_util::BodyExt::collect(req.into_body()).await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            tracing::debug!("Failed to read request body: {e}");
            return make_rpc_error_response(serde_json::Value::Null, &RpcError::ParseError);
        }
    };

    let payload: RpcRequestPayload = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => {
            tracing::debug!("Failed to parse request body: {e}");
            return make_rpc_error_response(serde_json::Value::Null, &RpcError::ParseError);
        }
    };

    match payload {
        RpcRequestPayload::Single(req) => process_single_request(req, &state, ctx, false).await,
        RpcRequestPayload::Batch(requests) => process_batch(requests, state, ctx).await,
    }
}

async fn process_batch(
    requests: Vec<JsonRpcRequest>,
    state: Arc<CloudbreakRpcState>,
    ctx: &Arc<RequestContext>,
) -> HttpHandlerResponse {
    let batch_size = requests.len();
    metrics::CLOUDBREAK_API_BATCH_REQUESTS
        .with_label_values(&[metrics::batch_size_bucket(batch_size)])
        .inc();

    let semaphore = Arc::new(tokio::sync::Semaphore::new(
        state.batch_handling_max_concurrency,
    ));
    let mut handles = Vec::with_capacity(batch_size);

    for req in requests {
        let state = state.clone();
        let ctx = ctx.clone();
        let sem = semaphore.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            process_single_request(req, &state, &ctx, true).await
        }));
    }

    let mut all_results = Vec::with_capacity(batch_size);
    for handle in handles {
        all_results.push(handle.await.unwrap());
    }

    let responses: Vec<serde_json::Value> = all_results
        .into_iter()
        .filter_map(|r| match r.body {
            ResponseBody::Buffered(bytes) => serde_json::from_slice(&bytes).ok(),
            ResponseBody::Streaming(_) => {
                // Unreachable in practice because we pass `in_batch = true`
                tracing::error!("Streaming body encountered inside batch context; dropping entry");
                None
            }
        })
        .collect();
    let body = serde_json::to_vec(&responses).unwrap_or_default();

    HttpHandlerResponse {
        status: StatusCode::OK,
        body: ResponseBody::Buffered(body),
    }
}

/// Note: `in_batch` param, will make the streamed response to be buffered
/// into a `Vec<u8>` and returned as a `ResponseBody::Buffered(Vec<u8>)`.
async fn process_single_request(
    rpc_request: JsonRpcRequest,
    state: &Arc<CloudbreakRpcState>,
    ctx: &Arc<RequestContext>,
    in_batch: bool,
) -> HttpHandlerResponse {
    let id = rpc_request.id.clone();
    let method = rpc_request.method.as_str();

    let (response_bytes, status): (Vec<u8>, StatusCode) = match method {
        "getHealth" => {
            let healthy = db_query::get_service_health(&state.database).await;

            let result = if !healthy {
                Err(state.node_unhealthy())
            } else {
                Ok(serde_json::Value::String("ok".to_string()))
            };

            json_serialize_response(id, result, ctx).await
        }
        "getSlot" => {
            let config: Option<RpcGetSlotConfig> =
                extract_param(&rpc_request.params, 0).ok().flatten();
            let slot = methods::slot::get_slot(state, config).await;

            json_serialize_response(id, slot, ctx).await
        }
        "getVersion" => {
            let version = methods::version::get_version(state).await;

            json_serialize_response(id, version, ctx).await
        }
        "getGenesisHash" => {
            let hash = methods::genesis::get_genesis_hash(state).await;

            json_serialize_response(id, hash, ctx).await
        }
        // Unsupported or disabled optional methods fall through to the method-not-found arm.
        "getVoteAccounts" if state.vote_accounts_supported => {
            let config: Option<methods::vote_accounts::GetVoteAccountsConfig> =
                extract_param(&rpc_request.params, 0).ok().flatten();
            let result = methods::vote_accounts::get_vote_accounts(state, config).await;
            json_serialize_response(id, result, ctx).await
        }
        "getSupply" if state.supply_enabled => {
            let config: Option<RpcSupplyConfig> =
                extract_param(&rpc_request.params, 0).ok().flatten();
            let result = methods::get_supply::get_supply(state, config).await;
            json_serialize_response(id, result, ctx).await
        }
        "simulateTransaction" if state.simulation_supported => {
            let transaction: String = match extract_param(&rpc_request.params, 0) {
                Ok(p) => p,
                Err(e) => return make_error_response(id, -32602, e),
            };
            let config: Option<RpcSimulateTransactionConfig> =
                extract_param(&rpc_request.params, 1).ok().flatten();
            let result =
                methods::simulate_transaction::simulate_transaction(state, transaction, config)
                    .await;
            json_serialize_response(id, result, ctx).await
        }
        "getAccountInfo" => {
            let start_time = Instant::now();

            let pubkey: String = match extract_param(&rpc_request.params, 0) {
                Ok(p) => p,
                Err(e) => return make_error_response(id, -32602, e),
            };
            let config: Option<RpcAccountInfoConfig> =
                extract_param(&rpc_request.params, 1).ok().flatten();

            let result = methods::get_account_info::get_account_info(state, pubkey, config).await;

            let status_label = if result.is_ok() {
                "success"
            } else {
                tracing::error!(target: "api_request_errors_count", "getAccountInfo error: {:?}", result.as_ref().unwrap_err());
                "error"
            };
            metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                .with_label_values(&["gAI", status_label])
                .inc();

            let json_response = json_serialize_response(id, result, ctx).await;

            metrics::CLOUDBREAK_API_REQUEST_DURATION_MS
                .with_label_values(&["gAI", metrics::bytes_bucket(json_response.0.len() as u64)])
                .observe(start_time.elapsed().as_secs_f64() * 1000.0);

            json_response
        }
        "getBalance" => {
            let start_time = Instant::now();

            let pubkey: String = match extract_param(&rpc_request.params, 0) {
                Ok(p) => p,
                Err(e) => return make_error_response(id, -32602, e),
            };
            let config: Option<RpcContextConfig> =
                extract_param(&rpc_request.params, 1).ok().flatten();

            let result = methods::get_balance::get_balance(state, pubkey, config).await;

            let status_label = if result.is_ok() {
                "success"
            } else {
                tracing::error!(target: "api_request_errors_count", "getBalance error: {:?}", result.as_ref().unwrap_err());
                "error"
            };
            metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                .with_label_values(&["getBalance", status_label])
                .inc();

            let json_response = json_serialize_response(id, result, ctx).await;

            metrics::CLOUDBREAK_API_REQUEST_DURATION_MS
                .with_label_values(&[
                    "getBalance",
                    metrics::bytes_bucket(json_response.0.len() as u64),
                ])
                .observe(start_time.elapsed().as_secs_f64() * 1000.0);

            json_response
        }
        "getMultipleAccounts" => {
            let start_time = Instant::now();

            let pubkeys: Vec<String> = match extract_param(&rpc_request.params, 0) {
                Ok(p) => p,
                Err(e) => return make_error_response(id, -32602, e),
            };
            let config: Option<RpcAccountInfoConfig> =
                extract_param(&rpc_request.params, 1).ok().flatten();

            let result =
                methods::get_multiple_accounts::get_multiple_accounts(state, pubkeys, config).await;

            let status_label = if result.is_ok() {
                "success"
            } else {
                tracing::error!(
                    target: "api_request_errors_count",
                    "getMultipleAccounts error: {:?}",
                    result.as_ref().unwrap_err()
                );
                "error"
            };
            metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                .with_label_values(&["getMultipleAccounts", status_label])
                .inc();

            let json_response = json_serialize_response(id, result, ctx).await;

            metrics::CLOUDBREAK_API_REQUEST_DURATION_MS
                .with_label_values(&[
                    "getMultipleAccounts",
                    metrics::bytes_bucket(json_response.0.len() as u64),
                ])
                .observe(start_time.elapsed().as_secs_f64() * 1000.0);

            json_response
        }
        "getPhoenixAccounts" => {
            let config: Option<methods::phoenix_accounts::GetPhoenixAccountsConfig> =
                match extract_optional_param(&rpc_request.params, 0) {
                    Ok(config) => config,
                    Err(e) => return make_error_response(id, -32602, e),
                };
            let start = Instant::now();
            let result =
                methods::phoenix_accounts::get_phoenix_accounts(state, config.unwrap_or_default())
                    .await;
            let (response, metrics_data) = match result {
                Ok((response, metrics)) => (Ok(response), Some(metrics)),
                Err(error) => (Err(error), None),
            };
            metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                .with_label_values(&[
                    "getPhoenixAccounts",
                    if response.is_ok() { "success" } else { "error" },
                ])
                .inc();
            let json_start = Instant::now();
            let serialized = json_serialize_response(id, response, ctx).await;
            if let Some(metrics) = metrics_data {
                metrics.record_metrics(
                    json_start.elapsed().as_secs_f64() * 1000.0,
                    start.elapsed(),
                    serialized.0.len() as u64,
                    0,
                    0.0,
                    &ctx.subscription_id,
                );
            }
            serialized
        }
        "getProgramAccounts" => {
            let gpa_global_start_time = Instant::now();

            let program: String = match extract_param(&rpc_request.params, 0) {
                Ok(p) => p,
                Err(e) => return make_error_response(id, -32602, e),
            };
            let config: Option<RpcProgramAccountsConfig> =
                extract_param(&rpc_request.params, 1).ok().flatten();

            let gpa_response = match methods::program::get_program_accounts(state, program, config)
                .await
            {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(target: "api_request_errors_count", "getProgramAccounts error: {:?}", e);
                    metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                        .with_label_values(&["gPA", "error"])
                        .inc();
                    return make_rpc_error_response(id, &e);
                }
            };

            let body = match gpa_streaming_response_body(
                id.clone(),
                gpa_response,
                gpa_global_start_time,
                ctx.clone(),
            )
            .await
            {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(target: "api_request_errors_count", "getProgramAccounts error: {:?}", e);
                    metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                        .with_label_values(&["gPA", "error"])
                        .inc();
                    return make_rpc_error_response(id, &e);
                }
            };

            if in_batch {
                // Await and collect the streaming body into a `Vec<u8>`
                (gpa_streamed_to_buffered(body, id).await, StatusCode::OK)
            } else {
                return HttpHandlerResponse {
                    status: StatusCode::OK,
                    body: ResponseBody::Streaming(body),
                };
            }
        }
        "getTokenAccountsByMint" => {
            let gpa_global_start_time = Instant::now();

            let mint: String = match extract_param(&rpc_request.params, 0) {
                Ok(m) => m,
                Err(e) => return make_error_response(id, -32602, e),
            };
            let config: Option<methods::mint_accounts::GetTokenAccountsByMintConfig> =
                extract_param(&rpc_request.params, 1).ok().flatten();

            let gpa_response = match methods::mint_accounts::get_token_accounts_by_mint(
                state, mint, config,
            )
            .await
            {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(target: "api_request_errors_count", "getTokenAccountsByMint error: {:?}", e);
                    metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                        .with_label_values(&["gTABM", "error"])
                        .inc();
                    return make_rpc_error_response(id, &e);
                }
            };

            let body = match gpa_streaming_response_body(
                id.clone(),
                gpa_response,
                gpa_global_start_time,
                ctx.clone(),
            )
            .await
            {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(target: "api_request_errors_count", "getTokenAccountsByMint error: {:?}", e);
                    metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                        .with_label_values(&["gTABM", "error"])
                        .inc();
                    return make_rpc_error_response(id, &e);
                }
            };

            if in_batch {
                (gpa_streamed_to_buffered(body, id).await, StatusCode::OK)
            } else {
                return HttpHandlerResponse {
                    status: StatusCode::OK,
                    body: ResponseBody::Streaming(body),
                };
            }
        }
        "getTokenAccountBalance" => {
            let start_time = Instant::now();

            let pubkey: String = match extract_param(&rpc_request.params, 0) {
                Ok(p) => p,
                Err(e) => return make_error_response(id, -32602, e),
            };
            // Agave: only a CommitmentConfig is accepted here (no minContextSlot).
            let commitment: Option<CommitmentConfig> =
                extract_param(&rpc_request.params, 1).ok().flatten();

            let result = methods::get_token_account_balance::get_token_account_balance(
                state, pubkey, commitment,
            )
            .await;

            let status_label = if result.is_ok() {
                "success"
            } else {
                tracing::error!(
                    target: "api_request_errors_count",
                    "getTokenAccountBalance error: {:?}",
                    result.as_ref().unwrap_err()
                );
                "error"
            };
            metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                .with_label_values(&["getTokenAccountBalance", status_label])
                .inc();

            let json_response = json_serialize_response(id, result, ctx).await;

            metrics::CLOUDBREAK_API_REQUEST_DURATION_MS
                .with_label_values(&[
                    "getTokenAccountBalance",
                    metrics::bytes_bucket(json_response.0.len() as u64),
                ])
                .observe(start_time.elapsed().as_secs_f64() * 1000.0);

            json_response
        }
        "getTokenSupply" => {
            let start_time = Instant::now();

            let pubkey: String = match extract_param(&rpc_request.params, 0) {
                Ok(p) => p,
                Err(e) => return make_error_response(id, -32602, e),
            };

            let commitment: Option<CommitmentConfig> =
                extract_param(&rpc_request.params, 1).ok().flatten();

            let result =
                methods::get_token_supply::get_token_supply(state, pubkey, commitment).await;

            let status_label = if result.is_ok() {
                "success"
            } else {
                tracing::error!(
                    target: "api_request_errors_count",
                    "getTokenSupply error: {:?}",
                    result.as_ref().unwrap_err()
                );
                "error"
            };
            metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                .with_label_values(&["getTokenSupply", status_label])
                .inc();

            let json_response = json_serialize_response(id, result, ctx).await;

            metrics::CLOUDBREAK_API_REQUEST_DURATION_MS
                .with_label_values(&[
                    "getTokenSupply",
                    metrics::bytes_bucket(json_response.0.len() as u64),
                ])
                .observe(start_time.elapsed().as_secs_f64() * 1000.0);

            json_response
        }
        "getLargestAccounts" => {
            // Disabled method: clean JSON-RPC "Method not found".
            if !state.largest_accounts.enabled {
                return make_rpc_error_response(id, &RpcError::MethodNotFound);
            }

            let start_time = Instant::now();

            let config: Option<solana_rpc_client_api::config::RpcLargestAccountsConfig> =
                match extract_optional_param(&rpc_request.params, 0) {
                    Ok(config) => config,
                    Err(e) => return make_error_response(id, -32602, e),
                };

            let result = methods::get_largest_accounts::get_largest_accounts(state, config).await;

            let status_label = match &result {
                Ok(_) => "success",
                Err(e) => {
                    tracing::error!(
                        target: "api_request_errors_count",
                        "getLargestAccounts error: {:?}",
                        e
                    );
                    "error"
                }
            };
            metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                .with_label_values(&["getLargestAccounts", status_label])
                .inc();

            let json_response = json_serialize_response(id, result, ctx).await;

            metrics::CLOUDBREAK_API_REQUEST_DURATION_MS
                .with_label_values(&[
                    "getLargestAccounts",
                    metrics::bytes_bucket(json_response.0.len() as u64),
                ])
                .observe(start_time.elapsed().as_millis() as f64);

            json_response
        }
        "getTokenLargestAccounts" => {
            // Disabled method: clean JSON-RPC "Method not found".
            if !state.token_largest_accounts.enabled {
                return make_rpc_error_response(id, &RpcError::MethodNotFound);
            }

            let start_time = Instant::now();

            let pubkey: String = match extract_param(&rpc_request.params, 0) {
                Ok(p) => p,
                Err(e) => return make_error_response(id, -32602, e),
            };
            let commitment: Option<CommitmentConfig> =
                extract_param(&rpc_request.params, 1).ok().flatten();

            let result = methods::get_token_largest_accounts::get_token_largest_accounts(
                state, pubkey, commitment,
            )
            .await;

            let status_label = if result.is_ok() {
                "success"
            } else {
                tracing::error!(
                    target: "api_request_errors_count",
                    "getTokenLargestAccounts error: {:?}",
                    result.as_ref().unwrap_err()
                );
                "error"
            };
            metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                .with_label_values(&["getTokenLargestAccounts", status_label])
                .inc();

            let json_response = json_serialize_response(id, result, ctx).await;

            metrics::CLOUDBREAK_API_REQUEST_DURATION_MS
                .with_label_values(&[
                    "getTokenLargestAccounts",
                    metrics::bytes_bucket(json_response.0.len() as u64),
                ])
                .observe(start_time.elapsed().as_millis() as f64);

            json_response
        }
        "getTokenAccountsByOwner" => {
            let owner: String = match extract_param(&rpc_request.params, 0) {
                Ok(o) => o,
                Err(e) => return make_error_response(id, -32602, e),
            };
            let filter: TokenAccountsFilter = match extract_param(&rpc_request.params, 1) {
                Ok(f) => f,
                Err(e) => return make_error_response(id, -32602, e),
            };
            let config: Option<RpcAccountInfoConfig> =
                extract_param(&rpc_request.params, 2).ok().flatten();

            let start_time = Instant::now();
            let query_type = TokenQueryType::GetTokenAccountsByOwner;

            let result = get_token_accounts_by_owner_or_delegate(
                state, owner, filter, config, query_type, None,
            )
            .await;
            let (response, metrics_data) = match result {
                Ok(result) => (Ok(result.response), result.metrics_data),
                Err(e) => (Err(e), None),
            };

            let json_start_time = Instant::now();

            let json_response = json_serialize_response(id, response, ctx).await;
            let response_size = json_response.0.len() as u64;

            if let Some(metrics_data) = metrics_data {
                metrics_data.record_metrics(
                    json_start_time.elapsed().as_millis() as f64,
                    start_time.elapsed(),
                    response_size,
                    0,
                    0.0,
                    &ctx.subscription_id,
                );
            } else {
                tracing::error!(target: "api_request_errors_count", "getTokenAccountsByOwner error: no metrics data");
                metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                    .with_label_values(&["gTABO", "error"])
                    .inc();
            }

            json_response
        }
        "getTokenAccountsByDelegate" => {
            let delegate: String = match extract_param(&rpc_request.params, 0) {
                Ok(d) => d,
                Err(e) => return make_error_response(id, -32602, e),
            };
            let filter: TokenAccountsFilter = match extract_param(&rpc_request.params, 1) {
                Ok(f) => f,
                Err(e) => return make_error_response(id, -32602, e),
            };
            let config: Option<RpcAccountInfoConfig> =
                extract_param(&rpc_request.params, 2).ok().flatten();

            let start_time = Instant::now();
            let query_type = TokenQueryType::GetTokenAccountsByDelegate;

            let result = get_token_accounts_by_owner_or_delegate(
                state, delegate, filter, config, query_type, None,
            )
            .await;
            let (response, metrics_data) = match result {
                Ok(result) => (Ok(result.response), result.metrics_data),
                Err(e) => (Err(e), None),
            };
            let json_start_time = Instant::now();

            let json_response = json_serialize_response(id, response, ctx).await;
            let response_size = json_response.0.len() as u64;

            if let Some(metrics_data) = metrics_data {
                metrics_data.record_metrics(
                    json_start_time.elapsed().as_millis() as f64,
                    start_time.elapsed(),
                    response_size,
                    0,
                    0.0,
                    &ctx.subscription_id,
                );
            } else {
                tracing::error!(target: "api_request_errors_count", "getTokenAccountsByDelegate error: no metrics data");
                metrics::CLOUDBREAK_API_REQUESTS_TOTAL
                    .with_label_values(&["gTABD", "error"])
                    .inc();
            }

            json_response
        }
        _ => {
            let reason = if matches!(method, "getVoteAccounts" | "simulateTransaction" | "getSupply") {
                "not enabled on this node"
            } else {
                "unknown method"
            };
            tracing::debug!("Method not found: {method} ({reason})");
            return make_rpc_error_response(id, &RpcError::MethodNotFound);
        }
    };

    HttpHandlerResponse {
        status,
        body: ResponseBody::Buffered(response_bytes),
    }
}

/// Serializes an RPC method result into JSON-RPC response bytes and the HTTP
/// status the response should carry. The status is always `200 OK` except for
/// an unhealthy node when `unhealthy-response = "http-unavailable"` (baked into
/// the error at its source; see [`http_status_for_error`]).
#[tracing::instrument(
    name = "json_encoding",
    skip_all,
    fields(
        request_id = %ctx.request_id,
        subscription_id = %ctx.subscription_id,
        client_ip = %ctx.client_ip,
    )
)]
async fn json_serialize_response<T: Serialize + Send + 'static>(
    id: serde_json::Value,
    result: Result<T, RpcError>,
    ctx: &RequestContext,
) -> (Vec<u8>, StatusCode) {
    let status = match &result {
        Ok(_) => StatusCode::OK,
        Err(e) => http_status_for_error(e),
    };

    let res = match result {
        Ok(value) => tokio::task::spawn_blocking(move || {
            let response = JsonRpcResponse::success(id, value);
            serde_json::to_vec(&response)
        })
        .await
        .unwrap_or_else(|_| {
            tracing::error!("Failed to join handle for json_serialize_response");
            Ok(vec![])
        }),
        Err(e) => {
            let response = JsonRpcResponse::<()>::from_rpc_error(id, &e);
            serde_json::to_vec(&response)
        }
    };

    let bytes = res.unwrap_or_else(|_| {
        tracing::error!("Failed to json_serialize_response");
        vec![]
    });

    (bytes, status)
}

async fn gpa_streamed_to_buffered(
    body: UnsyncBoxBody<Bytes, io::Error>,
    id: serde_json::Value,
) -> Vec<u8> {
    // A body error (mid-stream failure) or invalid JSON both become an internal error entry.
    let bytes = http_body_util::BodyExt::collect(body)
        .await
        .ok()
        .map(|collected| collected.to_bytes().to_vec())
        .filter(|bytes| serde_json::from_slice::<serde_json::Value>(bytes).is_ok());

    if let Some(bytes) = bytes {
        bytes
    } else {
        tracing::error!(target: "api_request_errors_count", "getProgramAccounts streaming body failed mid-flight;");
        metrics::CLOUDBREAK_API_REQUESTS_TOTAL
            .with_label_values(&["gPA", "error"])
            .inc();
        let err_response = JsonRpcResponse::<()>::from_rpc_error(id, &RpcError::InternalError);
        serde_json::to_vec(&err_response).unwrap_or_default()
    }
}

#[cfg(test)]
mod compatibility_tests {
    use super::*;
    use crate::modules::{
        cache::GpaProcessor, supply_cache::SupplySnapshot, vote_accounts_cache::StakesSnapshot,
    };
    use cloudbreak_core::modules::processed::ProcessedAccounts;
    use cloudbreak_core::{
        AccountSelectorConfig, MethodSection, PhoenixAccountsConfig, ProcessedCommitmentBehavior,
        UnhealthyResponseBehavior,
    };
    use std::{sync::RwLock, time::Duration};

    #[tokio::test]
    async fn compatibility_disabled_phoenix_rpc_never_uses_database_and_legacy_rpc_works() {
        let mut state = CloudbreakRpcState::new(
            sea_orm::DatabaseConnection::Disconnected,
            Duration::from_secs(1),
            None,
            None,
            Arc::new(AccountSelectorConfig::default()),
            1,
            None,
            Duration::from_secs(1),
            ProcessedCommitmentBehavior::default(),
            UnhealthyResponseBehavior::default(),
            GpaProcessor::new(None),
            "legacy-genesis".into(),
            false,
            Arc::new(RwLock::new(Arc::new(StakesSnapshot::empty()))),
            100,
            false,
            false,
            Arc::new(RwLock::new(Arc::new(SupplySnapshot::default()))),
            MethodSection::default(),
            MethodSection::default(),
            ProcessedAccounts::default(),
        );
        let ctx = Arc::new(RequestContext {
            subscription_id: "compatibility".into(),
            request_id: "test".into(),
            client_ip: "local".into(),
        });
        for section in [None, Some(PhoenixAccountsConfig::default())] {
            state.phoenix_accounts = section;
            for params in [
                serde_json::Value::Null,
                serde_json::json!([]),
                serde_json::json!([null]),
                serde_json::json!([{}]),
            ] {
                let request = serde_json::from_value(serde_json::json!({"jsonrpc":"2.0","id":7,"method":"getPhoenixAccounts","params":params})).unwrap();
                let reply =
                    process_single_request(request, &Arc::new(state.clone()), &ctx, false).await;
                assert_eq!(reply.status, StatusCode::OK);
                let ResponseBody::Buffered(bytes) = reply.body else {
                    panic!("expected buffered error")
                };
                let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(json["id"], 7);
                assert_eq!(json["error"]["code"], -32601);
            }
        }
        let request = serde_json::from_value(
            serde_json::json!({"jsonrpc":"2.0","id":8,"method":"getGenesisHash","params":[]}),
        )
        .unwrap();
        let reply = process_single_request(request, &Arc::new(state), &ctx, false).await;
        let ResponseBody::Buffered(bytes) = reply.body else {
            panic!("expected buffered result")
        };
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
            serde_json::json!({"jsonrpc":"2.0","id":8,"result":"legacy-genesis"})
        );
    }
}
