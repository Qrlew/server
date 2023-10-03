use serde::{Deserialize, Serialize};
use crate::{auth, Error};

/// Simplified DataType
#[derive(Clone, Debug, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct Response {
    value: String,
    signature: Option<String>,
}

impl Response {
    pub fn new(value: String) -> Self {
        Response {
            value,
            signature: None,
        }
    }

    pub fn signed(value: String, auth: &auth::Authenticator) -> Self {
        Response {
            signature: Some(auth.sign(&value)),
            value,
        }
    }

    pub fn value(&self) -> &str {
        &self.value
    }

    pub fn signature(&self) -> Option<&str> {
        self.signature.as_deref()
    }
}

// Errors need to be convertible to responses
impl axum::response::IntoResponse for Response {
    fn into_response(self) -> axum::response::Response {
        serde_json::to_string(&self).or_else(|err| Err(Error::from(err))).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_response() {
        let response = Response::new("Hello\nSarus !".to_string());
        let signed_response = Response::signed("Hello\nSarus !".to_string(), &auth::Authenticator::get("secret_key.pem").unwrap());
        println!("{:?}", signed_response);
        println!("{}", signed_response.value());
    }
}