use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use serde::de::DeserializeOwned;
use std::time::Duration;

/// Thin blocking wrapper around `reqwest` for the Spark monitoring REST API.
pub struct RestClient {
    client: Client,
}

impl RestClient {
    pub fn new() -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap_or_else(|_| Client::new()),
        }
    }

    /// GET `url` and deserialize JSON body into `T`.
    ///
    /// Returns `Err` on network failure, non-2xx status, or malformed JSON.
    pub fn get_json<T: DeserializeOwned>(
        &self,
        url: &str,
        auth_header: Option<&str>,
    ) -> Result<T, reqwest::Error> {
        let mut headers = HeaderMap::new();
        if let Some(auth) = auth_header {
            if let Ok(value) = HeaderValue::from_str(auth) {
                headers.insert(AUTHORIZATION, value);
            }
        }
        self.client
            .get(url)
            .headers(headers)
            .send()?
            .error_for_status()?
            .json::<T>()
    }

    /// GET `url` and return the raw response body as a String.
    ///
    /// Used by the plan-fetch path so the body can be fed verbatim to
    /// the plan parser without a second deserialisation pass.
    pub fn get_text(
        &self,
        url: &str,
        auth_header: Option<&str>,
    ) -> Result<String, reqwest::Error> {
        let mut headers = HeaderMap::new();
        if let Some(auth) = auth_header {
            if let Ok(value) = HeaderValue::from_str(auth) {
                headers.insert(AUTHORIZATION, value);
            }
        }
        self.client
            .get(url)
            .headers(headers)
            .send()?
            .error_for_status()?
            .text()
    }
}
