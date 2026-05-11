//! `GET /openapi.json` — serves the bundled OpenAPI 3.1 spec.
//! `GET /docs`        — Swagger UI rendered inline (CDN-pulled JS pointing
//!                       at /openapi.json).
//!
//! Spec is `crates/api/openapi.json`, embedded at compile time. To update,
//! edit that file + restart — no derive-macro churn, no rebuild ripple
//! through every handler.

use axum::Router;
use axum::http::HeaderValue;
use axum::http::header::CONTENT_TYPE;
use axum::response::{Html, IntoResponse};
use axum::routing::get;

use crate::SharedState;

const OPENAPI_JSON: &str = include_str!("../../openapi.json");

const SWAGGER_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Sentrix Chain Indexer API</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
    <div id="swagger-ui"></div>
    <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
    <script>
        window.onload = () => {
            window.ui = SwaggerUIBundle({
                url: "/openapi.json",
                dom_id: "#swagger-ui",
                deepLinking: true,
                presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset],
            });
        };
    </script>
</body>
</html>"##;

async fn openapi_json() -> impl IntoResponse {
    (
        [(
            CONTENT_TYPE,
            HeaderValue::from_static("application/json; charset=utf-8"),
        )],
        OPENAPI_JSON,
    )
}

async fn docs() -> Html<&'static str> {
    Html(SWAGGER_HTML)
}

/// Router for `/openapi.json` + `/docs`.
pub fn router() -> Router<SharedState> {
    Router::new()
        .route("/openapi.json", get(openapi_json))
        .route("/docs", get(docs))
}
