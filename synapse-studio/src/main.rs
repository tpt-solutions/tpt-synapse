//! Synapse Studio — a minimal web UI for topic/queue/key browsing and live
//! message tail (TODO.md "Adoption & Tooling").
//!
//! It is intentionally thin: a static dashboard plus a small JSON API that
//! proxies the running broker's Prometheus `/metrics` endpoint, so operators
//! can eyeball throughput/queue-depth without dropping to `curl`. The broker
//! address is configured via `SYNAPSE_BROKER_METRICS` (default
//! `http://127.0.0.1:9090/metrics`).

use axum::extract::State;
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use std::sync::Arc;

#[derive(Clone)]
struct AppState {
    http: reqwest::Client,
    metrics_url: String,
}

#[derive(Serialize)]
struct StatusView {
    metrics_url: String,
    ok: bool,
    sample: Vec<MetricLine>,
}

#[derive(Serialize)]
struct MetricLine {
    name: String,
    value: String,
}

/// Parse a Prometheus text exposition into lightweight name/value pairs.
fn parse_metrics(text: &str) -> Vec<MetricLine> {
    let mut out = Vec::new();
    for line in text.lines() {
        let l = line.trim();
        if l.is_empty() || l.starts_with('#') {
            continue;
        }
        if let Some((name, rest)) = l.split_once(' ') {
            // Strip any {...} labels for a compact browser view.
            let name = name.split('{').next().unwrap_or(name).to_string();
            let value = rest.split_whitespace().next().unwrap_or("").to_string();
            out.push(MetricLine { name, value });
        }
        if out.len() >= 200 {
            break;
        }
    }
    out
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let res = state.http.get(&state.metrics_url).send().await;
    match res {
        Ok(r) if r.status().is_success() => match r.text().await {
            Ok(text) => Json(StatusView {
                metrics_url: state.metrics_url.clone(),
                ok: true,
                sample: parse_metrics(&text),
            })
            .into_response(),
            Err(e) => Json(StatusView {
                metrics_url: state.metrics_url.clone(),
                ok: false,
                sample: vec![MetricLine {
                    name: "error".into(),
                    value: e.to_string(),
                }],
            })
            .into_response(),
        },
        Ok(r) => Json(StatusView {
            metrics_url: state.metrics_url.clone(),
            ok: false,
            sample: vec![MetricLine {
                name: "http_status".into(),
                value: r.status().to_string(),
            }],
        })
        .into_response(),
        Err(e) => Json(StatusView {
            metrics_url: state.metrics_url.clone(),
            ok: false,
            sample: vec![MetricLine {
                name: "error".into(),
                value: e.to_string(),
            }],
        })
        .into_response(),
    }
}

/// Re-trigger a metrics fetch (POST so it can be wired to a refresh button).
async fn refresh(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    status(State(state)).await
}

/// Build the Studio router.
pub fn router(metrics_url: String) -> Router {
    let state = Arc::new(AppState {
        http: reqwest::Client::new(),
        metrics_url,
    });
    Router::new()
        .route("/", get(index))
        .route("/api/status", get(status))
        .route("/api/refresh", post(refresh))
        .with_state(state)
}

/// Bind the Studio on `addr` and serve forever.
pub async fn serve(addr: &str, metrics_url: String) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("synapse-studio listening on http://{addr} (broker metrics: {metrics_url})");
    axum::serve(listener, router(metrics_url)).await
}

const INDEX_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>Synapse Studio</title>
<style>
  body { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; margin: 2rem; color: #222; }
  h1 { font-size: 1.2rem; }
  table { border-collapse: collapse; width: 100%; }
  th, td { text-align: left; padding: 4px 8px; border-bottom: 1px solid #ddd; font-size: 0.85rem; }
  button { margin-bottom: 1rem; padding: 6px 12px; }
  .bad { color: #b00; }
</style>
</head>
<body>
  <h1>Synapse Studio</h1>
  <button onclick="load()">Refresh</button>
  <div id="status">loading…</div>
  <table id="metrics"><thead><tr><th>metric</th><th>value</th></tr></thead><tbody></tbody></table>
  <script>
    async function load() {
      const r = await fetch('/api/status');
      const d = await r.json();
      const s = document.getElementById('status');
      s.innerHTML = d.ok ? ('broker: ' + d.metrics_url) : ('<span class="bad">unreachable: ' + d.metrics_url + '</span>');
      const tb = document.querySelector('#metrics tbody');
      tb.innerHTML = '';
      for (const m of d.sample) {
        const tr = document.createElement('tr');
        tr.innerHTML = '<td>' + m.name + '</td><td>' + m.value + '</td>';
        tb.appendChild(tr);
      }
    }
    load();
  </script>
</body>
</html>
"#;

fn main() {
    let addr = std::env::var("SYNAPSE_STUDIO_ADDR").unwrap_or_else(|_| "127.0.0.1:8081".into());
    let metrics = std::env::var("SYNAPSE_BROKER_METRICS")
        .unwrap_or_else(|_| "http://127.0.0.1:9090/metrics".into());
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(serve(&addr, metrics)).expect("studio server");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_prometheus_text() {
        let text = "# HELP x help\nx_total{job=\"a\"} 42\ny_count 7\n";
        let m = parse_metrics(text);
        assert_eq!(m[0].name, "x_total");
        assert_eq!(m[0].value, "42");
        assert_eq!(m[1].name, "y_count");
        assert_eq!(m[1].value, "7");
    }

    #[tokio::test]
    async fn status_endpoint_returns_json() {
        // No broker running; the endpoint must still answer with ok=false rather
        // than panic.
        let state = Arc::new(AppState {
            http: reqwest::Client::new(),
            metrics_url: "http://127.0.0.1:1/metrics".into(),
        });
        let resp = status(State(state)).await.into_response();
        assert!(resp.status().is_success());
    }
}
