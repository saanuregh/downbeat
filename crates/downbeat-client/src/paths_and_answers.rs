//! The paths the client asks for, and how it reads a scheduler's answer.

use downbeat_api::types::{ApiError, ErrorCode, PageRequest};
use downbeat_api::with_defaults;
use reqwest::{StatusCode, Url};
use serde::de::DeserializeOwned;

use crate::Error;

/// The absolute path made of `segments`, each percent-encoded, so a name with a space or a `/`
/// stays one segment.
pub(crate) fn path(segments: &[&str]) -> String {
    let mut url = Url::parse("http://scheduler/").expect("a fixed URL parses");
    url.path_segments_mut().expect("an http URL has a path").clear().extend(segments);
    url.path().to_owned()
}

/// The path of one page of a list: `segments`, with the page's `limit` and its cursor, sent as the
/// query parameter `cursor_name`, percent-encoded.
pub(crate) fn page_path(segments: &[&str], cursor_name: &str, page: &PageRequest) -> String {
    let mut url = Url::parse("http://scheduler/").expect("a fixed URL parses");
    url.set_path(&path(segments));
    let mut pairs = Vec::new();
    if let Some(limit) = page.limit {
        pairs.push(("limit", limit.to_string()));
    }
    if let Some(cursor) = &page.cursor {
        pairs.push((cursor_name, cursor.clone()));
    }
    if pairs.is_empty() {
        return url.path().to_owned();
    }
    url.query_pairs_mut().extend_pairs(pairs);
    format!("{}?{}", url.path(), url.query().unwrap_or_default())
}

/// The answer decoded as `T`, or the scheduler's error.
pub(crate) async fn decode<T: DeserializeOwned>(response: reqwest::Response) -> Result<T, Error> {
    let status = response.status();
    if status.is_success() {
        return response.json().await.map_err(Error::connection);
    }
    let body = response.bytes().await.map_err(Error::connection)?;
    let error: ApiError = serde_json::from_slice(&body).unwrap_or_else(|_| {
        with_defaults!(ApiError { error: ErrorCode::Unknown, message: undecoded_message(status, &body) })
    });
    if status == StatusCode::UNAUTHORIZED && error.message.contains("expired") {
        return Err(Error::Api {
            status: status.as_u16(),
            code: error.error,
            message: format!("{} — check this machine's clock", error.message),
        });
    }
    Err(Error::Api { status: status.as_u16(), code: error.error, message: error.message })
}

/// The status's reason and the start of a body that is not an `ApiError`, such as a proxy's page.
fn undecoded_message(status: StatusCode, body: &[u8]) -> String {
    const SHOWN: usize = 200;
    let reason = status.canonical_reason().unwrap_or("error");
    let text = String::from_utf8_lossy(&body[..body.len().min(SHOWN)]);
    let text = text.trim();
    if text.is_empty() {
        reason.to_owned()
    } else if body.len() > SHOWN {
        format!("{reason}: {text}…")
    } else {
        format!("{reason}: {text}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_path_segment_is_percent_encoded() {
        assert_eq!(path(&["v1", "jobs", "a b/c?d#e%f", "runs"]), "/v1/jobs/a%20b%2Fc%3Fd%23e%25f/runs");
        assert_eq!(path(&["v1", "jobs", "plain_name"]), "/v1/jobs/plain_name");
    }

    #[test]
    fn a_page_path_carries_only_what_the_page_sets() {
        let first = PageRequest::new();
        assert_eq!(page_path(&["v1", "backfills"], "cursor", &first), "/v1/backfills");
        let next = PageRequest::new().limit(20).cursor("2030-01-07/a b&c");
        assert_eq!(
            page_path(&["v1", "overrides"], "cursor", &next),
            "/v1/overrides?limit=20&cursor=2030-01-07%2Fa+b%26c"
        );
        assert_eq!(page_path(&["v1", "audit"], "before", &PageRequest::new().cursor("41")), "/v1/audit?before=41");
    }

    #[test]
    fn an_undecodable_error_body_keeps_its_start() {
        let long = "x".repeat(500);
        assert_eq!(undecoded_message(StatusCode::BAD_GATEWAY, b""), "Bad Gateway");
        assert_eq!(undecoded_message(StatusCode::BAD_GATEWAY, b" <h1>down</h1> "), "Bad Gateway: <h1>down</h1>");
        assert_eq!(
            undecoded_message(StatusCode::BAD_GATEWAY, long.as_bytes()),
            format!("Bad Gateway: {}…", &long[..200])
        );
    }
}
