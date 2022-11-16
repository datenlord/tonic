use crate::{body::BoxBody, transport};
use async_rdma::Rdma;
use std::io;
use tokio::net::ToSocketAddrs;
use tower::Service;

use super::ResponseFuture;

///
#[derive(Debug)]
pub struct RdmaChannel {
    rdma: Rdma,
}

impl RdmaChannel {
    ///
    pub async fn new<A>(addr: A) -> Result<Self, io::Error>
    where
        A: ToSocketAddrs,
    {
        let rdma = Rdma::connect(addr, 1, 1, 512).await?;
        Ok(Self {
            rdma,
        })
    }
    ///
    async fn call(&mut self, _request: http::Request<BoxBody>) -> http::Response<transport::Body> {
        todo!()
        // serialize and copy
        // send
        // recv
        // deserialized and copy
    }
}

impl Service<http::Request<BoxBody>> for RdmaChannel {
    type Response = http::Response<transport::Body>;
    type Error = transport::Error;
    type Future = ResponseFuture;
    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        todo!()
    }

    fn call(&mut self, req: http::Request<BoxBody>) -> Self::Future {
        todo!()
    }
}
