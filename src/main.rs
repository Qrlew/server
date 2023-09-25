pub mod request;

use std::{error, result, fmt, io, string};

use axum::{
    extract,
    response::{self, IntoResponse},
    routing::{get, post},
    handler::Handler,
    Router,
};
use serde::{Deserialize, Serialize};
use serde_json;

#[derive(Debug, Clone)]
pub enum Error {
    InvalidRequest(String),
    InvalidSQL(String),
    Other(String),
}

impl Error {
    pub fn invalid_request(request: impl fmt::Display) -> Error {
        Error::InvalidRequest(format!("Invalid request: {}", request))
    }
    pub fn invalid_sql(sql: impl fmt::Display) -> Error {
        Error::InvalidSQL(format!("Invalid SQL: {}", sql))
    }
    pub fn other<T: fmt::Display>(desc: T) -> Error {
        Error::Other(desc.to_string())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::InvalidRequest(request) => writeln!(f, "InvalidRequest: {}", request),
            Error::InvalidSQL(sql) => writeln!(f, "InvalidSQL: {}", sql),
            Error::Other(err) => writeln!(f, "{}", err),
        }
    }
}

impl error::Error for Error {}

// Errors need to be convertible to responses
impl IntoResponse for Error {
    fn into_response(self) -> response::Response {
        self.to_string().into_response()
    }
}

impl From<serde_json::error::Error> for Error {
    fn from(err: serde_json::error::Error) -> Self {
        Error::invalid_request(err)
    }
}

impl From<qrlew::sql::Error> for Error {
    fn from(err: qrlew::sql::Error) -> Self {
        Error::invalid_sql(err)
    }
}

impl From<string::FromUtf8Error> for Error {
    fn from(err: string::FromUtf8Error) -> Self {
        Error::other(err)
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::other(err)
    }
}


pub type Result<T> = result::Result<T, Error>;

async fn dot(extract::Json(dot_request): extract::Json<request::Dot>) -> Result<String> {
    dot_request.response()
}

#[tokio::main]
async fn main() {
    // build our application with a single route
    let app = Router::new()
        .route("/", get(|| async { "Hello, World!" }))
        .route("/dot", post(dot));

    // run it with hyper on localhost:3000
    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();

    // Test with:
    // curl -d '{"dataset":{"tables":[{"name":"table_1","path":["schema","table_1"],"schema":{"fields":[{"name":"a","data_type":"Float"},{"name":"b","data_type":"Integer"}]},"size":10000}]},"query":"SELECT * FROM table_1","dark_mode":false}' -H "Content-Type: application/json" -X POST localhost:3000/dot
}
