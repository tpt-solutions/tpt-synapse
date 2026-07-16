//! Synapse Studio — a minimal web UI for topic/queue/key browsing and live
//! message tail (TODO.md "Adoption & Tooling").
//!
//! Studio is a thin, single-origin front end over the running broker's admin
//! API (`core/src/admin.rs`). The browser only ever talks to Studio, which
//! proxies to the broker:
//!
//! * `GET /`            — the dashboard HTML/JS.
//! * `GET /api/snapshot`— proxies the broker's `GET /api/snapshot` (a JSON tree
//!                        of every tenant's logs/queues/maps) so the UI can
//!                        browse topics/queues/keys.
//! * `GET /api/tail`    — proxies the broker's `GET /api/tail` Server-Sent
//!                        Events stream of live mutations for the live tail.
//! * `GET /api/status`  — parses the broker's Prometheus `/metrics` into a small
//!                        name/value table for the status line.
//!
//! Proxying (rather than pointing the browser straight at the broker) keeps
//! everything same-origin: no CORS reliance, and the broker admin port need not
//! be reachable from the browser, only from Studio.
//!
//! Configuration (env):
//! * `SYNAPSE_STUDIO_ADDR`    — where Studio listens (default `127.0.0.1:8081`).
//! * `SYNAPSE_BROKER_ADMIN`   — broker admin base URL (default
//!                              `http://127.0.0.1:9091`).
//! * `SYNAPSE_BROKER_METRICS` — Prometheus `/metrics` URL (default
//!                              `{admin}/metrics`; the admin server serves it).
//! * `SYNAPSE_STUDIO_DEMO`    — when truthy (`1`/`true`/`yes`), Studio starts an
//!                              in-process demo broker (seeded resources + a
//!                              background traffic generator) and points itself
//!                              at it, so it can be evaluated standalone.

use axum::body::Body;
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{Html, IntoResponse, Response};
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

/// A JSON error body, used when the upstream broker is unreachable.
fn upstream_error(status: StatusCode, msg: &str) -> Response {
    let body = serde_json::json!({ "ok": false, "error": msg });
    (status, Json(body)).into_response()
}

/// Proxy the broker's browsable snapshot (`GET /api/snapshot`) through Studio so
/// the browser stays same-origin.
async fn snapshot(State(state): State<Arc<AppState>>) -> Response {
    let url = format!("{}/api/snapshot", state.admin_url.trim_end_matches('/'));
    match state.http.get(&url).send().await {
        Ok(r) if r.status().is_success() => match r.bytes().await {
            Ok(body) => (
                [(header::CONTENT_TYPE, "application/json")],
                body,
            )
                .into_response(),
            Err(e) => upstream_error(StatusCode::BAD_GATEWAY, &e.to_string()),
        },
        Ok(r) => upstream_error(
            StatusCode::BAD_GATEWAY,
            &format!("broker admin returned {}", r.status()),
        ),
        Err(e) => upstream_error(StatusCode::BAD_GATEWAY, &e.to_string()),
    }
}

/// Proxy the broker's Server-Sent Events live tail (`GET /api/tail`) through
/// Studio, streaming upstream bytes straight to the browser's `EventSource`.
async fn tail(State(state): State<Arc<AppState>>) -> Response {
    let url = format!("{}/api/tail", state.admin_url.trim_end_matches('/'));
    match state.http.get(&url).send().await {
        Ok(r) if r.status().is_success() => {
            let stream = r.bytes_stream();
            Response::builder()
                .header(header::CONTENT_TYPE, "text/event-stream")
                .header(header::CACHE_CONTROL, "no-cache")
                .header(header::CONNECTION, "keep-alive")
                .body(Body::from_stream(stream))
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
        }
        Ok(r) => upstream_error(
            StatusCode::BAD_GATEWAY,
            &format!("broker admin returned {}", r.status()),
        ),
        Err(e) => upstream_error(StatusCode::BAD_GATEWAY, &e.to_string()),
    }
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
        .route("/api/snapshot", get(snapshot))
        .route("/api/tail", get(tail))
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

/// An optional in-process demo broker so Studio can be evaluated standalone
/// (`SYNAPSE_STUDIO_DEMO=1`). Starts a real [`synapse_core::SynapseCore`] behind
/// the broker admin API, seeds a couple of tenants' logs/queues/maps, and drives
/// a slow stream of mutations so the live tail shows activity. Returns the admin
/// base URL the rest of Studio points at.
mod demo {
    use std::sync::Arc;
    use std::time::Duration;

    use synapse_core::{spawn_admin_server, SynapseCore};

    /// Seed demo resources into a fresh core. Shared by `start` and its tests.
    pub fn seed(core: &SynapseCore) {
        for tenant in ["acme", "beta"] {
            let _ = core.create_log(tenant, "events");
            let _ = core.log_append(tenant, "events", b"welcome to synapse studio");
            let _ = core.create_queue(tenant, "jobs");
            let _ = core.queue_enqueue(tenant, "jobs", b"job-1");
            let _ = core.create_map(tenant, "cache");
            let _ = core.map_set(tenant, "cache", "greeting", b"hello", None);
        }
    }

    /// Start the demo broker and its background traffic generator, returning the
    /// admin base URL (e.g. `http://127.0.0.1:53211`).
    pub async fn start() -> std::io::Result<String> {
        let core = Arc::new(SynapseCore::new());
        seed(&core);
        let metrics = core.metrics();
        let (addr, handle) = spawn_admin_server("127.0.0.1:0".parse().unwrap(), core.clone(), metrics)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        // Detach the admin server; dropping the handle does not abort the task.
        std::mem::forget(handle);

        // Background traffic so the live tail is never empty during a demo.
        tokio::spawn(async move {
            let mut n: u64 = 1;
            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;
                let _ = core.log_append("acme", "events", format!("tick #{n}").as_bytes());
                let _ = core.queue_enqueue("beta", "jobs", format!("job-{n}").as_bytes());
                let _ = core.map_set(
                    "acme",
                    "cache",
                    "counter",
                    n.to_string().as_bytes(),
                    None,
                );
                n += 1;
            }
        });

        Ok(format!("http://{addr}"))
    }
}

fn env_truthy(name: &str) -> bool {
    matches!(
        std::env::var(name).ok().as_deref(),
        Some("1") | Some("true") | Some("yes") | Some("on")
    )
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
  .ok { color: #070; }
  .tenant { background: #f4f4f4; padding: 0.5rem; margin-top: 1rem; border-radius: 4px; }
  .tail { background: #111; color: #0f0; padding: 0.5rem; height: 12rem; overflow: auto; font-size: 0.78rem; white-space: pre-wrap; }
  .kind { color: #888; }
</style>
</head>
<body>
  <h1>Synapse Studio</h1>
  <button onclick="loadSnapshot()">Refresh</button>
  <div id="status" class="kind">loading…</div>
  <div id="browse"></div>
  <h2>Live tail <span class="kind">(SSE)</span></h2>
  <div id="tail" class="tail"></div>
  <script>
    // Same-origin: Studio proxies /api/snapshot and /api/tail to the broker.
    const brokerAdmin = "/*ADMIN_URL*/";

    async function loadStatus() {
      const el = document.getElementById('status');
      try {
        const r = await fetch('/api/status');
        const d = await r.json();
        if (d.ok) {
          el.className = 'ok';
          el.textContent = 'broker ' + d.admin_url + ' — ' + d.sample.length + ' metrics';
        } else {
          el.className = 'bad';
          el.textContent = 'broker ' + d.admin_url + ' unreachable';
        }
      } catch (e) {
        el.className = 'bad';
        el.textContent = 'status error: ' + e;
      }
    }

    async function loadSnapshot() {
      const el = document.getElementById('browse');
      let d;
      try {
        const r = await fetch('/api/snapshot');
        d = await r.json();
      } catch (e) {
        el.innerHTML = '<p class="bad">snapshot error: ' + e + '</p>';
        return;
      }
      el.innerHTML = '';
      if (d.error) { el.innerHTML = '<p class="bad">' + d.error + '</p>'; return; }
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
      const es = new EventSource('/api/tail');
      const box = document.getElementById('tail');
      es.onmessage = (ev) => {
        const e = JSON.parse(ev.data);
        const line = '[' + e.kind + '] ' + e.tenant + '/' + e.name + (e.key ? (':' + e.key) : '') + ' ' + e.preview;
        box.textContent += line + '\n';
        box.scrollTop = box.scrollHeight;
      };
      es.onerror = () => { box.textContent += '(tail disconnected)\n'; };
    }

    loadStatus();
    loadSnapshot();
    connectTail();
    setInterval(loadStatus, 5000);
    setInterval(loadSnapshot, 5000);
  </script>
</body>
</html>
"#;

fn main() {
    let addr = std::env::var("SYNAPSE_STUDIO_ADDR").unwrap_or_else(|_| "127.0.0.1:8081".into());
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(async move {
        let (admin, metrics) = if env_truthy("SYNAPSE_STUDIO_DEMO") {
            let admin = demo::start().await.expect("start demo broker");
            println!("synapse-studio: started embedded demo broker at {admin}");
            let metrics = format!("{admin}/metrics");
            (admin, metrics)
        } else {
            let admin = std::env::var("SYNAPSE_BROKER_ADMIN")
                .unwrap_or_else(|_| "http://127.0.0.1:9091".into());
            let metrics = std::env::var("SYNAPSE_BROKER_METRICS")
                .unwrap_or_else(|_| format!("{}/metrics", admin.trim_end_matches('/')));
            (admin, metrics)
        };
        serve(&addr, admin, metrics).await.expect("studio server");
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use synapse_core::{spawn_admin_server, Metrics, SynapseCore};
    use tokio::time::{sleep, timeout, Duration};

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

    #[tokio::test]
    async fn snapshot_unreachable_broker_returns_502() {
        let state = Arc::new(AppState {
            http: reqwest::Client::new(),
            admin_url: "http://127.0.0.1:1".into(),
            metrics_url: "http://127.0.0.1:1/metrics".into(),
        });
        let resp = snapshot(State(state)).await;
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    }

    /// Start a real broker admin server, seeded, and return its base URL.
    async fn start_seeded_admin() -> (Arc<SynapseCore>, String) {
        let core = Arc::new(SynapseCore::new());
        demo::seed(&core);
        let (addr, handle) = spawn_admin_server(
            "127.0.0.1:0".parse().unwrap(),
            core.clone(),
            Arc::new(Metrics::new()),
        )
        .await
        .unwrap();
        std::mem::forget(handle);
        (core, format!("http://{addr}"))
    }

    /// Bind Studio on an ephemeral port pointed at `admin_url`; return its addr.
    async fn start_studio(admin_url: String) -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(admin_url.clone(), format!("{admin_url}/metrics"));
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        addr
    }

    #[tokio::test]
    async fn proxies_snapshot_from_broker() {
        let (_core, admin_url) = start_seeded_admin().await;
        let studio = start_studio(admin_url).await;

        let client = reqwest::Client::new();
        let body = client
            .get(format!("http://{studio}/api/snapshot"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();

        // The demo seed creates tenant "acme" with an "events" log, a "jobs"
        // queue, and a "cache" map — all of which must be browsable via Studio.
        assert!(body.contains("\"acme\""), "snapshot missing tenant: {body}");
        assert!(body.contains("\"events\""), "snapshot missing log: {body}");
        assert!(body.contains("\"jobs\""), "snapshot missing queue: {body}");
        assert!(body.contains("\"cache\""), "snapshot missing map: {body}");
    }

    #[tokio::test]
    async fn proxies_live_tail_sse() {
        let (core, admin_url) = start_seeded_admin().await;
        let studio = start_studio(admin_url).await;

        let client = reqwest::Client::new();
        let mut resp = client
            .get(format!("http://{studio}/api/tail"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get(reqwest::header::CONTENT_TYPE)
                .unwrap()
                .to_str()
                .unwrap(),
            "text/event-stream"
        );

        // Give the proxy chain time to subscribe upstream, then mutate.
        let mutate = tokio::spawn(async move {
            sleep(Duration::from_millis(150)).await;
            // base64("live") == "bGl2ZQ=="
            core.log_append("acme", "events", b"live").unwrap();
        });

        let mut seen = String::new();
        let found = timeout(Duration::from_secs(5), async {
            while let Ok(Some(chunk)) = resp.chunk().await {
                seen.push_str(&String::from_utf8_lossy(&chunk));
                if seen.contains("bGl2ZQ==") {
                    return true;
                }
            }
            false
        })
        .await
        .unwrap_or(false);

        mutate.await.unwrap();
        assert!(found, "expected proxied SSE frame with preview, got: {seen}");
    }
}
