//! The screens, compiled into the binary: a single-page app whose hashed files are cached for a
//! long time and whose entry page is always fetched fresh.

use axum::http::{HeaderValue, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "web/dist/"]
#[allow_missing = true]
struct Screens;

const ENTRY: &str = "index.html";

/// Serves a file of the screens, or the entry page for any other path (the app routes itself).
pub(crate) async fn serve(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    match Screens::get(path).filter(|_| !path.is_empty()) {
        Some(file) => {
            let mime = file.metadata.mimetype().to_owned();
            let mut response = (StatusCode::OK, file.data).into_response();
            set(&mut response, header::CONTENT_TYPE, &mime);
            set(&mut response, header::CACHE_CONTROL, "public, max-age=31536000, immutable");
            response
        }
        None if path.starts_with("api/") || path.starts_with("auth/") => StatusCode::NOT_FOUND.into_response(),
        None => entry_page(),
    }
}

fn entry_page() -> Response {
    let Some(file) = Screens::get(ENTRY) else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "The console's screens were not built into this binary. Run `mise run web`, then build the console again.",
        )
            .into_response();
    };
    let mut response = (StatusCode::OK, file.data).into_response();
    set(&mut response, header::CONTENT_TYPE, "text/html; charset=utf-8");
    set(&mut response, header::CACHE_CONTROL, "no-cache");
    response
}

fn set(response: &mut Response, name: header::HeaderName, value: &str) {
    if let Ok(value) = HeaderValue::from_str(value) {
        response.headers_mut().insert(name, value);
    }
}
