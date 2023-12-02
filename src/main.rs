pub mod auth;
pub mod request;
pub mod response;
// Reexport
pub use auth::Authenticator;
pub use request::{Dot, RewriteAsPrivacyUnitPreserving, RewriteWithDifferentialPrivacy};
pub use response::Response;

use std::{error, result, fmt, io, string, sync::OnceLock};
use rsa;
use rsa::pkcs8::spki::{EncodePublicKey, der::pem::LineEnding};
use axum::{
    extract,
    routing::{get, post},
    Router,
};
use tower_http::{
    trace::{self, TraceLayer},
    cors::CorsLayer,
};
use tracing::Level;
use serde_json;
use qrlew::{differential_privacy, rewriting};


#[derive(Debug, Clone)]
pub enum Error {
    InvalidRequest(String),
    InvalidSQL(String),
    ImpossibleRewriting(String),
    Other(String),
}

impl Error {
    pub fn invalid_request(request: impl fmt::Display) -> Error {
        Error::InvalidRequest(format!("Invalid request: {}", request))
    }
    pub fn invalid_sql(sql: impl fmt::Display) -> Error {
        Error::InvalidSQL(format!("Invalid SQL: {}", sql))
    }
    pub fn impossible_rewriting(sql: impl fmt::Display) -> Error {
        Error::InvalidSQL(format!("Impossible Rewriting: {}", sql))
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
            Error::ImpossibleRewriting(sql) => writeln!(f, "ImpossibleRewriting: {}", sql),
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

impl From<rewriting::Error> for Error {
    fn from(err: rewriting::Error) -> Self {
        Error::impossible_rewriting(err)
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

impl From<rsa::pkcs8::spki::Error> for Error {
    fn from(err: rsa::pkcs8::spki::Error) -> Self {
        Error::other(err)
    }
}

impl From<rsa::pkcs8::Error> for Error {
    fn from(err: rsa::pkcs8::Error) -> Self {
        Error::other(err)
    }
}

pub type Result<T> = result::Result<T, Error>;

/// A global shared Authenticator
static AUTH: OnceLock<Authenticator> = OnceLock::new();

/// A function used to count named objects
fn auth() -> &'static Authenticator {
    AUTH.get_or_init(|| Authenticator::get("secret_key.pem").unwrap())
}

async fn verify(extract::Json(response): extract::Json<Response>) -> Result<String> {
    auth().verify(response.value(), response.signature().ok_or(Error::invalid_request(response.value()))?).and_then(|_| Ok(format!("Verified"))).or_else(|_| Ok(format!("Not verified")))
}

async fn public_key() -> Result<String> {
    Ok(auth().verifying_key().to_public_key_pem(LineEnding::CRLF)?)
}

async fn dot(extract::Json(dot_request): extract::Json<request::Dot>) -> Result<Response> {
    dot_request.response()
}

async fn rewrite_as_privacy_unit_preserving(extract::Json(rewrite_as_privacy_unit_preserving_request): extract::Json<request::RewriteAsPrivacyUnitPreserving>) -> Result<Response> {
    rewrite_as_privacy_unit_preserving_request.response()
}

async fn rewrite_with_differential_privacy(extract::Json(rewrite_with_differential_privacy_request): extract::Json<request::RewriteWithDifferentialPrivacy>) -> Result<Response> {
    rewrite_with_differential_privacy_request.response(auth())
}

async fn rewrite_as_privacy_unit_preserving_with_dot(extract::Json(rewrite_as_privacy_unit_preserving_request_with_dot): extract::Json<request::RewriteAsPrivacyUnitPreservingWithDot>) -> Result<Response> {
    rewrite_as_privacy_unit_preserving_request_with_dot.response()
}

async fn rewrite_with_differential_privacy_with_dot(extract::Json(rewrite_with_differential_privacy_request_with_dot): extract::Json<request::RewriteWithDifferentialPrivacyWithDot>) -> Result<Response> {
    rewrite_with_differential_privacy_request_with_dot.response(auth())
}

#[tokio::main]
async fn main() {
    // Setup tracing
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();

    // build our application with a single route
    let app = Router::new()
        .route("/", get(|| async { format!("This is Qrlew server {}", env!("CARGO_PKG_VERSION"))}))
        .route("/public_key", get(public_key))
        .route("/verify", post(verify))
        .route("/dot", post(dot))
        .route("/rewrite_as_privacy_unit_preserving", post(rewrite_as_privacy_unit_preserving))
        .route("/rewrite_with_differential_privacy", post(rewrite_with_differential_privacy))
        .route("/rewrite_as_privacy_unit_preserving_with_dot", post(rewrite_as_privacy_unit_preserving_with_dot))
        .route("/rewrite_with_differential_privacy_with_dot", post(rewrite_with_differential_privacy_with_dot))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(trace::DefaultMakeSpan::new()
                    .level(Level::INFO))
                .on_response(trace::DefaultOnResponse::new()
                    .level(Level::INFO)),
                )
        .layer(
            CorsLayer::permissive()
        );
    
    // load authenticator
    auth();

    // run it with hyper on localhost:3000
    tracing::info!("listening on 0.0.0.0:3000");
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
