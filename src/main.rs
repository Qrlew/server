pub mod auth;
pub mod request;
pub mod response;
// Reexport
pub use auth::Authenticator;
pub use request::{Dot, Protect, DPCompile};
pub use response::Response;

use std::{error, result, fmt, io, string, sync::OnceLock};
use rsa;
use axum::{
    extract,
    routing::{get, post},
    Router,
};
use serde_json;
use qrlew::differential_privacy;


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
impl axum::response::IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
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

impl From<differential_privacy::Error> for Error {
    fn from(err: differential_privacy::Error) -> Self {
        Error::other(err)
    }
}

impl From<rsa::Error> for Error {
    fn from(err: rsa::Error) -> Self {
        Error::other(err)
    }
}

impl From<base64::DecodeError> for Error {
    fn from(err: base64::DecodeError) -> Self {
        Error::other(err)
    }
}

impl From<rsa::signature::Error> for Error {
    fn from(err: rsa::signature::Error) -> Self {
        Error::other(err)
    }
}

pub type Result<T> = result::Result<T, Error>;

/// A global shared Authenticator
static AUTH: OnceLock<Authenticator> = OnceLock::new();

/// A function used to count named objects
fn auth() -> &'static Authenticator {
    AUTH.get_or_init(|| Authenticator::random_2048().unwrap())
}

async fn dot(extract::Json(dot_request): extract::Json<request::Dot>) -> Result<Response> {
    dot_request.response()
}

async fn protect(extract::Json(protect_request): extract::Json<request::Protect>) -> Result<Response> {
    protect_request.response()
}

async fn dp_compile(extract::Json(dp_compile_request): extract::Json<request::DPCompile>) -> Result<Response> {
    dp_compile_request.response(auth())
}

#[tokio::main]
async fn main() {
    // build our application with a single route
    let app = Router::new()
        .route("/", get(|| async { format!("This is Qrlew server {}", env!("CARGO_PKG_VERSION"))}))
        .route("/dot", post(dot))
        .route("/protect", post(protect))
        .route("/dp_compile", post(dp_compile)); 

    // run it with hyper on localhost:3000
    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();

    // Test with:
    // curl -d '{"dataset":{"tables":[{"name":"table_1","path":["schema","table_1"],"schema":{"fields":[{"name":"a","data_type":"Float"},{"name":"b","data_type":"Integer"}]},"size":10000}]},"query":"SELECT * FROM table_1","dark_mode":false}' -H "Content-Type: application/json" -X POST localhost:3000/dot
    // curl -d '{"dataset":{"tables":[{"name":"user_table","path":["schema","user_table"],"schema":{"fields":[{"name":"id","data_type":"Integer"},{"name":"name","data_type":"Text"},{"name":"age","data_type":"Integer"},{"name":"weight","data_type":"Float"}]},"size":10000},{"name":"action_table","path":["schema","action_table"],"schema":{"fields":[{"name":"action","data_type":"Text"},{"name":"user_id","data_type":"Integer"},{"name":"duration","data_type":"Float"}]},"size":10000}]},"query":"SELECT * FROM action_table","protected_entity":[["user_table",[],"id"],["action_table",[["user_id","user_table","id"]],"id"]]}' -H "Content-Type: application/json" -X POST localhost:3000/protect
    // curl -d '{"dataset":{"tables":[{"name":"user_table","path":["schema","user_table"],"schema":{"fields":[{"name":"id","data_type":"Integer"},{"name":"name","data_type":"Text"},{"name":"age","data_type":"Integer"},{"name":"weight","data_type":"Float"}]},"size":10000},{"name":"action_table","path":["schema","action_table"],"schema":{"fields":[{"name":"action","data_type":"Text"},{"name":"user_id","data_type":"Integer"},{"name":"duration","data_type":"Float"}]},"size":10000}]},"query":"SELECT sum(duration) FROM action_table WHERE duration > 0 AND duration < 24","protected_entity":[["user_table",[],"id"],["action_table",[["user_id","user_table","id"]],"id"]],"epsilon":1.0,"delta":0.00001,"epsilon_tau_thresholding":1.0,"delta_tau_thresholding":0.00001}' -H "Content-Type: application/json" -X POST localhost:3000/dp_compile
}
