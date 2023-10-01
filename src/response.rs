use std::{sync::Arc, ops::Deref};
use serde::{Deserialize, Serialize};
use crate::auth;

/// Simplified DataType
#[derive(Clone, Debug, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct Response {
    value: String,
    signature: Option<String>,
}

impl Response {
    pub fn value(value: String) -> Self {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_response() {
        let response = Response::value("Hello Sarus !".to_string());
        let signed_response = Response::signed("Hello Sarus !".to_string(), &auth::Authenticator::random_2048().unwrap());
        println!("{:?}", signed_response);
    }
}