use std::{
    sync::OnceLock,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tracing_appender::non_blocking;
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};

pub mod net;
pub mod sniff;

/// Milliseconds since UNIX epoch
pub type UnixMillis = u64;

pub fn unix_millis() -> UnixMillis {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis().min(u64::MAX as u128) as u64,
        Err(_) => 0, // clock went backwards; clamp
    }
}

pub async fn sleep_until(ts_millis: u64) {
    if let Some(deadline) = deadline_instant(ts_millis) {
        tokio::time::sleep_until(deadline).await;
    }
}

pub fn deadline_instant(ts_millis: u64) -> Option<tokio::time::Instant> {
    let now = unix_millis();
    ts_millis
        .checked_sub(now)
        .map(|delta| tokio::time::Instant::now() + Duration::from_millis(delta))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn unix_millis_is_monotonic_enough() {
        let t1 = unix_millis();
        let t2 = unix_millis();
        assert!(t2 >= t1, "unix_millis went backwards");
    }

    #[test]
    fn deadline_instant_returns_none_for_past() {
        let now = unix_millis();
        assert!(deadline_instant(now.saturating_sub(1)).is_none());
    }

    #[test]
    fn deadline_instant_allows_near_future() {
        let near_future = unix_millis() + 1;
        assert!(deadline_instant(near_future).is_some());
    }

    #[tokio::test]
    async fn sleep_until_past_returns_immediately() {
        let start = tokio::time::Instant::now();

        sleep_until(unix_millis().saturating_sub(100)).await;

        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(5),
            "sleep_until for past timestamp blocked unexpectedly: {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn sleep_until_future_waits_at_least_until_deadline() {
        let start = tokio::time::Instant::now();
        let deadline = unix_millis() + 20;

        sleep_until(deadline).await;

        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(15),
            "sleep_until returned too early: {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn sleep_until_does_not_oversleep_badly() {
        let start = tokio::time::Instant::now();
        let deadline = unix_millis() + 20;

        sleep_until(deadline).await;

        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(200),
            "sleep_until overslept badly: {:?}",
            elapsed
        );
    }
}

static LOG_GUARD: OnceLock<non_blocking::WorkerGuard> = OnceLock::new();

pub fn init_tracing() {
    let app_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // Console needs TRACE-ish telemetry
    // let console_filter = EnvFilter::new("trace");

    // let console_layer = console_subscriber::ConsoleLayer::builder().spawn();

    let (non_blocking, guard) = non_blocking(std::io::stdout());

    // Store globally
    LOG_GUARD.set(guard).expect("Tracing already initialized");

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_thread_ids(true)
        .with_line_number(true)
        .with_file(true)
        .with_writer(non_blocking);

    tracing_subscriber::registry()
        // .with(console_layer.with_filter(console_filter))
        .with(fmt_layer.with_filter(app_filter))
        .init();
}

pub fn init_tracing_dbg() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));

    let console_layer = console_subscriber::ConsoleLayer::builder().spawn();

    let (non_blocking, guard) = non_blocking(std::io::stdout());

    // Store globally
    LOG_GUARD.set(guard).expect("Tracing already initialized");

    tracing_subscriber::registry()
        .with(filter)
        .with(console_layer)
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_thread_ids(true)
                .with_line_number(true)
                .with_file(true)
                .with_writer(non_blocking),
        )
        .init();
}

// TODO: make a "common" crate?

/// Outcome of an authentication attempt. A denial carries the message the
/// client sees in `AuthErr`, so peer-dependent rules can guide the fix
/// instead of failing opaquely.
#[derive(Debug, Clone)]
pub enum AuthDecision {
    Allow,
    Deny { message: String },
}

#[async_trait::async_trait]
pub trait AuthHandler {
    async fn verify(&self, username: &str, password: &str) -> bool;

    /// Authentication with connection context. The default adapts `verify`;
    /// handlers with peer-dependent rules (loopback-only default
    /// credentials) override this.
    async fn decide(
        &self,
        username: &str,
        password: &str,
        peer: Option<std::net::IpAddr>,
    ) -> AuthDecision {
        let _ = peer;
        if self.verify(username, password).await {
            AuthDecision::Allow
        } else {
            AuthDecision::Deny {
                message: "invalid credentials".to_string(),
            }
        }
    }

    /// Whether a TLS-verified client-certificate identity authenticates the
    /// connection as that user with no password. The default never maps, so
    /// handlers without a user store keep password auth only; a denial here
    /// is not an error, the connection just stays unauthenticated.
    async fn decide_certificate(&self, identity: &str) -> AuthDecision {
        let _ = identity;
        AuthDecision::Deny {
            message: "certificate identities are not mapped by this broker".to_string(),
        }
    }
}

/// Identity a verified client certificate asserts: the first DNS subject
/// alternative name, else the subject common name. Identities in the `@`
/// namespace never map, so node principals stay unclaimable by certificate.
pub fn client_identity_from_der(cert_der: &[u8]) -> Option<String> {
    let (_, parsed) = x509_parser::parse_x509_certificate(cert_der).ok()?;
    let identity = parsed
        .subject_alternative_name()
        .ok()
        .flatten()
        .and_then(|san| {
            san.value.general_names.iter().find_map(|name| match name {
                x509_parser::extensions::GeneralName::DNSName(dns) => Some(dns.to_string()),
                _ => None,
            })
        })
        .or_else(|| {
            parsed
                .subject()
                .iter_common_name()
                .next()
                .and_then(|cn| cn.as_str().ok())
                .map(str::to_string)
        })?;
    if identity.is_empty() || identity.starts_with('@') {
        return None;
    }
    Some(identity)
}

#[derive(Debug, Clone)]
pub struct StaticAuthHandler {
    username: String,
    password: String,
}

impl StaticAuthHandler {
    pub fn new(username: String, password: String) -> Self {
        Self { username, password }
    }
}

#[async_trait::async_trait]
impl AuthHandler for StaticAuthHandler {
    async fn verify(&self, username: &str, password: &str) -> bool {
        username.to_lowercase() == self.username && password == self.password
    }
}
