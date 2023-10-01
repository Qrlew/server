use std::{sync::Arc, ops::Deref};
use serde::{Deserialize, Serialize};
use super::*;

/// Simplified DataType
#[derive(Copy, Clone, Debug, PartialEq, PartialOrd, Deserialize, Serialize)]
struct Response {
    value: String,
    signature: Option<String>,
}

impl Response {
    pub fn value(value: String) -> Self {
        Response {
            value,
            None,
        }
    }

    pub fn signed(value: String, auth: auth::Authenticator) -> Self {
        Response {
            value,
            Some(auth.sign(value)),
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
        let signed_response = Response::signed("Hello Sarus !".to_string(), Authenticator::random_2048());
        println!("{}", Response::new());
        auth.verify("Hello Sarus !".to_string(), signature).expect("OK");
    }
}