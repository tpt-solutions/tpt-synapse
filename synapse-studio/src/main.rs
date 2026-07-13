//! Synapse Studio — a minimal web UI for topic/queue/key browsing and live
//! message tail (TODO.md "Adoption & Tooling").
//!
//! It proxies the running broker's admin API (`core/src/admin.rs`): `GET
//! /api/snapshot` returns a JSON tree of every tenant's logs/queues/maps, and
//! `GET /api/tail` is a Server-Sent Events stream of live mutations. The broker
//! admin address is configured via `SYNAPSE_BROKER_ADMIN` (default
//! `http://127.0.0.1:9091`). Optionally `SYNAPSE_BROKER_METRICS` still points
//! at the Prometheus `/metrics` endpoint for the stats line.

use axum::extract::State;
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use std::sync::Arc;

#[derive(Clone)]
struct AppState {
    http: reqwest::Client,
    admin_url: String,
    metrics_url: String,
}

#[derive(Serialize)]
struct StatusView {
    admin_url: String,
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

async fn index(State(state): State<Arc<AppState>>) -> Html<String> {
    Html(INDEX_HTML.replace("/*ADMIN_URL*/", &state.admin_url))
}

async fn status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let res = state.http.get(&state.metrics_url).send().await;
    match res {
        Ok(r) if r.status().is_success() => match r.text().await {
            Ok(text) => Json(StatusView {
                admin_url: state.admin_url.clone(),
                ok: true,
                sample: parse_metrics(&text),
            })
            .into_response(),
            Err(e) => Json(StatusView {
                admin_url: state.admin_url.clone(),
                ok: false,
                sample: vec![MetricLine {
                    name: "error".into(),
                    value: e.to_string(),
                }],
            })
            .into_response(),
        },
        Ok(r) => Json(StatusView {
            admin_url: state.admin_url.clone(),
            ok: false,
            sample: vec![MetricLine {
                name: "http_status".into(),
                value: r.status().to_string(),
            }],
        })
        .into_response(),
        Err(e) => Json(StatusView {
            admin_url: state.admin_url.clone(),
            ok: false,
            sample: vec![MetricLine {
                name: "error".into(),
                value: e.to_string(),
            }],
        })
        .into_response(),
    }
}

async fn refresh(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    status(State(state)).await
}

/// Build the Studio router.
pub fn router(admin_url: String, metrics_url: String) -> Router {
    let state = Arc::new(AppState {
        http: reqwest::Client::new(),
        admin_url,
        metrics_url,
    });
    Router::new()
        .route("/", get(index))
        .route("/api/status", get(status))
        .route("/api/refresh", post(refresh))
        .with_state(state)
}

/// Bind the Studio on `addr` and serve forever.
pub async fn serve(addr: &str, admin_url: String, metrics_url: String) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!(
        "synapse-studio listening on http://{addr} (broker admin: {admin_url}, metrics: {metrics_url})"
    );
    axum::serve(listener, router(admin_url, metrics_url)).await
}

const INDEX_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>Synapse Studio</title>
<style>
  body { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; margin: 2rem; color: #222; }
  h1 { font-size: 1.2rem; }
  h2 { font-size: 1rem; margin-top: 1.5rem; }
  table { border-collapse: collapse; width: 100%; margin-bottom: 1rem; }
  th, td { text-align: left; padding: 4px 8px; border-bottom: 1px solid #ddd; font-size: 0.82rem; vertical-align: top; }
  button { margin-bottom: 1rem; padding: 6px 12px; }
  .bad { color: #b00; }
  .tenant { background: #f4f4f4; padding: 0.5rem; margin-top: 1rem; border-radius: 4px; }
  .tail { background: #111; color: #0f0; padding: 0.5rem; height: 12rem; overflow: auto; font-size: 0.78rem; white-space: pre-wrap; }
  .kind { color: #888; }
</style>
</head>
<body>
  <h1>Synapse Studio</h1>
  <button onclick="loadSnapshot()">Refresh</button>
  <div id="status">loading…</div>
  <div id="browse"></div>
  <h2>Live tail <span class="kind">(SSE)</span></h2>
  <div id="tail" class="tail"></div>
  <script>
    const admin = "/*ADMIN_URL*/";

    async function loadSnapshot() {
      const r = await fetch(admin + '/api/snapshot');
      const d = await r.json();
      const el = document.getElementById('browse');
      el.innerHTML = '';
      if (!d.tenants || d.tenants.length === 0) {
        el.innerHTML = '<p class="kind">no resources yet</p>';
        return;
      }
      for (const t of d.tenants) {
        const box = document.createElement('div');
        box.className = 'tenant';
        box.innerHTML = '<strong>tenant: ' + t.tenant + '</strong>';
        box.appendChild(renderTable('Logs', t.resources.logs, ['name', 'len'], (r) =>
          '<td>' + r.name + '</td><td>' + r.len + '</td><td class="kind">' + (r.sample || []).join(' ') + '</td>'));
        box.appendChild(renderTable('Queues', t.resources.queues, ['name', 'depth'], (q) =>
          '<td>' + q.name + '</td><td>' + q.depth + '</td><td class="kind">' + (q.sample || []).map(s => s[1]).join(' ') + '</td>'));
        box.appendChild(renderTable('Maps (keys)', t.resources.maps, ['name', 'size'], (m) =>
          '<td>' + m.name + '</td><td>' + m.size + '</td><td class="kind">' + (m.keys || []).map(k => k[0] + '=' + k[1]).join(' ') + '</td>'));
        el.appendChild(box);
      }
    }

    function renderTable(title, rows, cols, rowHtml) {
      const wrap = document.createElement('div');
      const h = document.createElement('h2');
      h.textContent = title + ' (' + (rows ? rows.length : 0) + ')';
      wrap.appendChild(h);
      const table = document.createElement('table');
      const thead = document.createElement('thead');
      thead.innerHTML = '<tr>' + cols.map(c => '<th>' + c + '</th>').join('') + '<th>preview</th></tr>';
      table.appendChild(thead);
      const tbody = document.createElement('tbody');
      if (rows) for (const row of rows) {
        const tr = document.createElement('tr');
        tr.innerHTML = rowHtml(row);
        tbody.appendChild(tr);
      }
      table.appendChild(tbody);
      wrap.appendChild(table);
      return wrap;
    }

    function connectTail() {
      const es = new EventSource(admin + '/api/tail');
      const box = document.getElementById('tail');
      es.onmessage = (ev) => {
        const e = JSON.parse(ev.data);
        const line = '[' + e.kind + '] ' + e.tenant + '/' + e.name + (e.key ? (':' + e.key) : '') + ' ' + e.preview;
        box.textContent += line + '\n';
        box.scrollTop = box.scrollHeight;
      };
      es.onerror = () => { box.textContent += '(tail disconnected)\n'; };
    }

    loadSnapshot();
    connectTail();
    setInterval(loadSnapshot, 5000);
  </script>
</body>
</html>
"#;

fn main() {
    let addr = std::env::var("SYNAPSE_STUDIO_ADDR").unwrap_or_else(|_| "127.0.0.1:8081".into());
    let admin = std::env::var("SYNAPSE_BROKER_ADMIN")
        .unwrap_or_else(|_| "http://127.0.0.1:9091".into());
    let metrics = std::env::var("SYNAPSE_BROKER_METRICS")
        .unwrap_or_else(|_| "http://127.0.0.1:9090/metrics".into());
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime
        .block_on(serve(&addr, admin, metrics))
        .expect("studio server");
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
            admin_url: "http://127.0.0.1:1/admin".into(),
            metrics_url: "http://127.0.0.1:1/metrics".into(),
        });
        let resp = status(State(state)).await.into_response();
        assert!(resp.status().is_success());
    }
}
