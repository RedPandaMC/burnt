use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use serde::de::DeserializeOwned;
use std::time::Duration;

/// Thin blocking wrapper around `reqwest` for the Spark monitoring REST API.
///
/// Holds the parsed `Authorization` header so callers don't re-parse it
/// on every request, and the underlying `reqwest::Client` reuses its
/// connection pool across calls — important when `/sql/{id}` is fetched
/// in parallel for dozens of executions.
pub struct RestClient {
    client: Client,
    auth: Option<HeaderValue>,
}

impl RestClient {
    /// Build a client with the given (already-formatted) auth header.
    ///
    /// `auth` should be the full header value, e.g. `"Bearer <token>"`.
    /// An invalid string silently falls through to no-auth — the calling
    /// layer surfaces failures via HTTP status, not header construction.
    pub fn new(auth: Option<&str>) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .pool_idle_timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_else(|_| Client::new());
        let auth = auth.and_then(|s| HeaderValue::from_str(s).ok());
        Self { client, auth }
    }

    /// GET `url` and deserialize JSON body into `T`.
    pub fn get_json<T: DeserializeOwned>(&self, url: &str) -> Result<T, reqwest::Error> {
        self.send(url)?.json::<T>()
    }

    /// GET `url` and return the raw response body as a String.
    ///
    /// Used by the plan-fetch path so the body can be fed verbatim to
    /// the plan parser without a second deserialisation pass.
    pub fn get_text(&self, url: &str) -> Result<String, reqwest::Error> {
        self.send(url)?.text()
    }

    fn send(&self, url: &str) -> Result<reqwest::blocking::Response, reqwest::Error> {
        let mut headers = HeaderMap::new();
        if let Some(auth) = &self.auth {
            headers.insert(AUTHORIZATION, auth.clone());
        }
        self.client
            .get(url)
            .headers(headers)
            .send()?
            .error_for_status()
    }
}
