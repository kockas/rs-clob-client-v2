//! Wire spelling regressions: POST execution evidence must not be discarded.
#![cfg(feature = "clob")]

use polymarket_client_sdk_v2::clob::types::response::PostOrderResponse;
use serde_json::json;

#[test]
fn post_response_accepts_both_trade_id_spellings() -> anyhow::Result<()> {
    for field in ["tradeIds", "tradeIDs"] {
        let mut raw = json!({"success":true,"orderID":"order","status":"matched",
            "errorMsg":"","makingAmount":"1","takingAmount":"5"});
        raw[field] = json!(["trade-1", "trade-2"]);
        let response: PostOrderResponse = serde_json::from_value(raw)?;
        assert_eq!(response.trade_ids, ["trade-1", "trade-2"], "{field}");
    }
    Ok(())
}

#[test]
fn post_response_trade_ids_null_or_absent_remain_empty() -> anyhow::Result<()> {
    let raw = json!({"success":false,"orderID":"","status":"", "errorMsg":"rejected",
        "makingAmount":"","takingAmount":""});
    let missing: PostOrderResponse = serde_json::from_value(raw.clone())?;
    assert!(missing.trade_ids.is_empty());
    for field in ["tradeIds", "tradeIDs"] {
        let mut value = raw.clone();
        value[field] = serde_json::Value::Null;
        let response: PostOrderResponse = serde_json::from_value(value)?;
        assert!(response.trade_ids.is_empty());
    }
    Ok(())
}

#[test]
fn contradictory_duplicate_spellings_are_rejected() {
    let raw = json!({"success":true,"orderID":"","status":"", "errorMsg":"rejected",
        "makingAmount":"","takingAmount":"","tradeIds":[],"tradeIDs":["execution"]});
    assert!(serde_json::from_value::<PostOrderResponse>(raw).is_err());
}
