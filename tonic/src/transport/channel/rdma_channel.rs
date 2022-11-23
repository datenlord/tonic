use crate::{
    body::BoxBody,
    codec::{encode_rdma, Encoder},
    transport::{self, BoxFuture},
    Status,
};
use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, Rdma};
use bytes::BufMut;
use futures_core::Stream;
use http::{uri::PathAndQuery, Request, Response};
use std::{alloc::Layout, io, sync::Arc};
use tokio::net::ToSocketAddrs;
use tower::Service;

///
#[derive(Debug)]
pub struct RdmaChannel {
    rdma: Arc<Rdma>,
}

const MAX_MSG_LEN: usize = 10240;

impl RdmaChannel {
    ///
    pub async fn new<A>(addr: A) -> Result<Self, io::Error>
    where
        A: ToSocketAddrs,
    {
        // TODO: expose these params to user (create a `RdmaEndpoint` maybe)
        let rdma = Arc::new(Rdma::connect(addr, 1, 1, MAX_MSG_LEN).await?);
        Ok(Self { rdma })
    }
}

impl Service<Request<BoxBody>> for RdmaChannel {
    type Response = Response<hyper::Body>;
    type Error = transport::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    // TODO: handle error
    fn call(&mut self, _req: Request<BoxBody>) -> Self::Future {
        unreachable!()
    }
}

///
#[async_trait::async_trait]
pub trait RdmaOp {
    ///
    async fn call<T, U>(
        &mut self,
        encoder: T,
        req: crate::Request<U>,
        path: PathAndQuery,
    ) -> Result<Response<hyper::Body>, transport::Error>
    where
        T: Encoder<Error = Status> + Send,
        U: Stream<Item = T::Item> + Send;
    ///
    fn is_rdma(&self) -> bool;
}

#[async_trait::async_trait]
impl RdmaOp for RdmaChannel {
    async fn call<T, U>(
        &mut self,
        encoder: T,
        req: crate::Request<U>,
        path: PathAndQuery,
    ) -> Result<Response<hyper::Body>, transport::Error>
    where
        T: Encoder<Error = Status> + Send,
        U: Stream<Item = T::Item> + Send,
    {
        // alloc rdma mr
        let mut req_mr = self
            .rdma
            .alloc_local_mr(Layout::new::<[u8; MAX_MSG_LEN]>())
            .unwrap();

        // encode `req` into mr
        let mut buf = unsafe { req_mr.as_mut_slice_unchecked() };
        // path
        let path = path.as_str();
        let mut len = path.len();
        buf[0..len].copy_from_slice(path.as_bytes());
        unsafe { buf.advance_mut(len) };
        buf.put_u8(32u8);
        len += 1;
        // body
        len += encode_rdma(encoder, req.into_inner(), buf).await;

        // send mr
        self.rdma.send_with_imm(&req_mr, len as u32).await.unwrap();

        // recv mr
        let (resp_mr, len) = self.rdma.receive_with_imm().await.unwrap();
        let len = len.unwrap() as usize;
        let resp_vec = resp_mr.as_slice()[0..len].to_vec();

        // from mr to resp
        let body = hyper::Body::from(resp_vec);
        let resp = http::Response::builder().body(body).unwrap();

        Ok(resp)
    }

    fn is_rdma(&self) -> bool {
        true
    }
}
