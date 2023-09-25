pub mod query;

use axum::{
    extract::Json,
    routing::{get, post},
    handler::Handler,
    Router,
};
use serde::{Deserialize, Serialize};


#[tokio::main]
async fn main() {
    // build our application with a single route
    let app = Router::new().route("/", get(|| async { "Hello, World!" }));

    // run it with hyper on localhost:3000
    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}
