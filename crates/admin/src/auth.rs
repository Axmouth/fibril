use axum::{http::HeaderMap, http::StatusCode};
use base64::Engine;
use dashmap::DashSet;
use fibril_util::{AuthHandler, StaticAuthHandler};
use std::sync::Arc;

pub const ADMIN_SESSION_COOKIE: &str = "fibril_admin_session";

#[derive(Debug, Clone, Default)]
pub struct AdminSessions {
    tokens: Arc<DashSet<String>>,
}

impl AdminSessions {
    pub fn create(&self) -> String {
        let token = uuid::Uuid::now_v7().to_string();
        self.tokens.insert(token.clone());
        token
    }

    pub fn remove(&self, token: &str) {
        self.tokens.remove(token);
    }

    pub fn contains(&self, token: &str) -> bool {
        self.tokens.contains(token)
    }
}

pub async fn check_basic_auth(
    headers: &HeaderMap,
    auth: &Option<StaticAuthHandler>,
) -> Result<(), StatusCode> {
    let Some(auth) = auth else {
        return Ok(());
    };

    let Some(value) = headers.get("authorization") else {
        return Err(StatusCode::UNAUTHORIZED);
    };

    let value = value.to_str().map_err(|_| StatusCode::UNAUTHORIZED)?;
    let b64 = value
        .strip_prefix("Basic ")
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let decoded = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;

    let decoded = String::from_utf8(decoded).map_err(|_| StatusCode::UNAUTHORIZED)?;
    let (user, pass) = decoded.split_once(':').ok_or(StatusCode::UNAUTHORIZED)?;

    if auth.verify(user, pass).await {
        Ok(())
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

pub async fn check_admin_auth(
    headers: &HeaderMap,
    auth: &Option<StaticAuthHandler>,
    sessions: &AdminSessions,
) -> Result<(), StatusCode> {
    let Some(auth) = auth else {
        return Ok(());
    };

    if check_basic_auth(headers, &Some(auth.clone())).await.is_ok() {
        return Ok(());
    }

    if let Some(token) = session_cookie(headers)
        && sessions.contains(token)
    {
        return Ok(());
    }

    Err(StatusCode::UNAUTHORIZED)
}

pub async fn verify_credentials(
    auth: &Option<StaticAuthHandler>,
    username: &str,
    password: &str,
) -> bool {
    let Some(auth) = auth else {
        return true;
    };
    auth.verify(username, password).await
}

pub fn session_cookie(headers: &HeaderMap) -> Option<&str> {
    let cookie = headers.get("cookie")?.to_str().ok()?;
    cookie.split(';').find_map(|entry| {
        let (name, value) = entry.trim().split_once('=')?;
        (name == ADMIN_SESSION_COOKIE).then_some(value)
    })
}

pub fn session_cookie_header(token: &str) -> String {
    format!("{ADMIN_SESSION_COOKIE}={token}; Path=/; HttpOnly; SameSite=Lax")
}

pub fn clear_session_cookie_header() -> String {
    format!("{ADMIN_SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0")
}
