//! First-boot setup page.
//!
//! When the server boots in setup mode it serves ONLY this page, on
//! localhost, and the broker listener stays down until the operator makes a
//! choice: generate per-deployment TLS material, supply their own PEMs, or
//! explicitly continue without TLS. The page runs before broker state
//! exists, so it is a standalone router rather than part of the dashboard,
//! and it is bound to loopback because the supply path uploads a private
//! key. Applying the choice is injected by the server binary, which owns
//! the config overlay and the completed-setup marker.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::response::Html;
use axum::routing::{get, post};
use axum::{Form, Router};
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio::sync::Notify;

use crate::server::AdminServerError;

/// The TLS part of the operator's setup decision.
pub enum TlsSetupChoice {
    /// Generate per-deployment material (CA + server certificate).
    AutoSelfSigned,
    /// Operator-supplied PEM text.
    Provided { cert_pem: String, key_pem: String },
    /// Continue without TLS, as an explicit decision.
    SkipTls,
}

/// The cluster-secret part of the setup decision.
pub enum ClusterSecretChoice {
    /// Generate a fresh secret and write it to the data dir.
    Generate,
    /// Use the secret pasted from an existing node.
    Provided(String),
}

/// A full first-boot setup submission: the TLS choice, optional admin
/// credentials, and an optional cluster secret.
pub struct SetupSubmission {
    pub tls: TlsSetupChoice,
    pub admin_credentials: Option<(String, String)>,
    pub cluster_secret: Option<ClusterSecretChoice>,
}

/// What an applied choice produced, echoed on the success page and in logs.
#[derive(Debug, Clone)]
pub struct SetupApplied {
    pub summary: String,
}

/// Applies a setup choice: validates, persists material and the overlay,
/// and records the completed-setup marker. An `Err` string is shown on the
/// form so the operator can correct and retry.
pub type ApplySetup = Arc<dyn Fn(SetupSubmission) -> Result<SetupApplied, String> + Send + Sync>;

struct SetupState {
    apply: ApplySetup,
    applied: Mutex<Option<SetupApplied>>,
    done: Notify,
}

/// Serve the setup page on `bind` until a choice is applied, then return it.
pub async fn run_setup_server(
    bind: SocketAddr,
    apply: ApplySetup,
) -> Result<SetupApplied, AdminServerError> {
    let state = Arc::new(SetupState {
        apply,
        applied: Mutex::new(None),
        done: Notify::new(),
    });
    let app = Router::new()
        .route("/", get(setup_page))
        .route("/setup", post(apply_setup))
        .with_state(state.clone());

    let listener = TcpListener::bind(bind)
        .await
        .map_err(|source| AdminServerError::Bind {
            bind: bind.to_string(),
            source,
        })?;
    let shutdown_state = state.clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown_state.done.notified().await })
        .await
        .map_err(AdminServerError::Serve)?;

    let applied = state
        .applied
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take();
    applied.ok_or_else(|| {
        AdminServerError::Serve(std::io::Error::other(
            "setup server stopped before a choice was applied",
        ))
    })
}

#[derive(Deserialize)]
struct SetupForm {
    mode: String,
    #[serde(default)]
    cert_pem: String,
    #[serde(default)]
    key_pem: String,
    #[serde(default)]
    admin_username: String,
    #[serde(default)]
    admin_password: String,
    #[serde(default)]
    secret_mode: String,
    #[serde(default)]
    secret_value: String,
}

async fn setup_page(State(_state): State<Arc<SetupState>>) -> Html<String> {
    Html(render_form(None))
}

async fn apply_setup(
    State(state): State<Arc<SetupState>>,
    Form(form): Form<SetupForm>,
) -> Html<String> {
    let tls = match form.mode.as_str() {
        "auto" => TlsSetupChoice::AutoSelfSigned,
        "provided" => TlsSetupChoice::Provided {
            cert_pem: form.cert_pem,
            key_pem: form.key_pem,
        },
        "skip" => TlsSetupChoice::SkipTls,
        other => return Html(render_form(Some(&format!("unknown setup mode `{other}`")))),
    };
    let admin_username = form.admin_username.trim();
    let admin_credentials = if admin_username.is_empty() && form.admin_password.is_empty() {
        None
    } else if admin_username.is_empty() || form.admin_password.is_empty() {
        return Html(render_form(Some(
            "set both an admin username and password, or leave both blank",
        )));
    } else {
        Some((admin_username.to_string(), form.admin_password.clone()))
    };
    let cluster_secret = match form.secret_mode.as_str() {
        "" | "none" => None,
        "generate" => Some(ClusterSecretChoice::Generate),
        "provided" => {
            if form.secret_value.trim().is_empty() {
                return Html(render_form(Some(
                    "paste the cluster secret from your first node, or choose generate",
                )));
            }
            Some(ClusterSecretChoice::Provided(
                form.secret_value.trim().to_string(),
            ))
        }
        other => return Html(render_form(Some(&format!("unknown secret mode `{other}`")))),
    };
    let submission = SetupSubmission {
        tls,
        admin_credentials,
        cluster_secret,
    };
    match (state.apply)(submission) {
        Ok(applied) => {
            let summary = applied.summary.clone();
            *state.applied.lock().unwrap_or_else(|e| e.into_inner()) = Some(applied);
            state.done.notify_one();
            Html(render_done(&summary))
        }
        Err(message) => Html(render_form(Some(&message))),
    }
}

const PAGE_STYLE: &str = r#"
  body { font-family: ui-sans-serif, system-ui, sans-serif; background: #101216;
         color: #e6e6e6; max-width: 44rem; margin: 3rem auto; padding: 0 1.25rem; }
  h1 { font-size: 1.4rem; } h1 span { color: #7dd3a0; }
  p, label, li { line-height: 1.5; }
  .card { background: #181b21; border: 1px solid #2a2e37; border-radius: 10px;
          padding: 1.25rem 1.5rem; margin: 1rem 0; }
  .error { border-color: #b3564d; background: #241a19; }
  textarea { width: 100%; min-height: 8rem; background: #101216; color: #e6e6e6;
             border: 1px solid #2a2e37; border-radius: 6px; font-family: monospace;
             padding: 0.5rem; }
  button { background: #7dd3a0; color: #101216; border: 0; border-radius: 6px;
           padding: 0.6rem 1.2rem; font-weight: 600; cursor: pointer; margin-top: 0.75rem; }
  .muted { color: #9aa2af; font-size: 0.9rem; }
  fieldset { border: 0; padding: 0; margin: 0.75rem 0; }
  code { background: #101216; padding: 0.1rem 0.35rem; border-radius: 4px; }
"#;

fn render_form(error: Option<&str>) -> String {
    let error_block = error
        .map(|message| {
            format!(r#"<div class="card error"><strong>Not applied:</strong> {message}</div>"#)
        })
        .unwrap_or_default();
    format!(
        r#"<!doctype html><html><head><meta charset="utf-8">
<title>Fibril first-boot setup</title><style>{PAGE_STYLE}</style></head><body>
<h1><span>fibril</span> first-boot setup</h1>
<p>The broker is not serving yet. Choose how connections will be secured;
the broker starts as soon as a choice is applied.</p>
{error_block}
<form method="post" action="/setup">
  <div class="card">
    <label><input type="radio" name="mode" value="auto" checked>
      <strong>Generate TLS material for this deployment</strong></label>
    <p class="muted">Creates a CA and server certificate under the data dir
    and prints the CA fingerprint. Clients trust <code>ca.pem</code> or pin
    the fingerprint.</p>
  </div>
  <div class="card">
    <label><input type="radio" name="mode" value="provided">
      <strong>Supply a certificate</strong></label>
    <p class="muted">Paste a PEM certificate chain and its private key. The
    pair is validated before anything is written.</p>
    <label class="muted">Certificate chain (PEM)</label>
    <textarea name="cert_pem" placeholder="-----BEGIN CERTIFICATE-----"></textarea>
    <label class="muted">Private key (PEM)</label>
    <textarea name="key_pem" placeholder="-----BEGIN PRIVATE KEY-----"></textarea>
  </div>
  <div class="card">
    <label><input type="radio" name="mode" value="skip">
      <strong>Continue without TLS</strong></label>
    <p class="muted">Connections stay plaintext. This is recorded as an
    explicit choice and can be changed later via the <code>tls</code> config
    section or <code>fibrilctl cert generate</code>.</p>
  </div>

  <div class="card">
    <strong>Admin user (optional)</strong>
    <p class="muted">Create a broker user now for remote access. Leave blank
    to keep the loopback-only default and add users later from the dashboard
    or <code>fibrilctl user add</code>.</p>
    <label class="muted">Username</label>
    <input type="text" name="admin_username" autocomplete="off" placeholder="ops">
    <label class="muted">Password</label>
    <input type="password" name="admin_password" autocomplete="new-password">
  </div>

  <div class="card">
    <strong>Cluster secret (optional)</strong>
    <p class="muted">Only for a multi-node cluster. Every node shares one
    secret. Skip this for a single broker.</p>
    <label><input type="radio" name="secret_mode" value="none" checked> None (single broker)</label>
    <label><input type="radio" name="secret_mode" value="generate"> Generate one (first node)</label>
    <label><input type="radio" name="secret_mode" value="provided"> Paste the secret from an existing node</label>
    <input type="password" name="secret_value" autocomplete="off" placeholder="cluster secret">
  </div>

  <button type="submit">Apply and start the broker</button>
</form>
</body></html>"#
    )
}

fn render_done(summary: &str) -> String {
    format!(
        r#"<!doctype html><html><head><meta charset="utf-8">
<title>Fibril setup complete</title><style>{PAGE_STYLE}</style></head><body>
<h1><span>fibril</span> setup complete</h1>
<div class="card"><p>{summary}</p></div>
<p>The broker is starting now. This page will not be served again; the
dashboard is available on the configured admin address.</p>
</body></html>"#
    )
}
