use crate::{
    body::BoxBody,
    transport::{self, BoxFuture},
};
use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, Rdma};
use http::{Request, Response};
use http_body::Body;
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
    fn call(&mut self, req: Request<BoxBody>) -> Self::Future {
        let rdma = Arc::clone(&self.rdma);
        Box::pin(async move {
            tokio::spawn(rdma_send_req(Arc::clone(&rdma), req));

            // let mut start = 0;
            // loop {
            //     if start >= len {
            //         break;
            //     }
            //     let step = len.min(MAX_MSG_LEN);
            //     req_mr.as_mut_slice()[start..start + step]
            //         .copy_from_slice(&req_vec[start..start + step]);
            //     rdma.send_with_imm(&req_mr, step as u32).await.unwrap();
            //     start += step;
            // }

            let (resp_mr, len) = rdma.receive_with_imm().await.unwrap();
            let len = len.unwrap() as usize;
            let resp_vec = resp_mr.as_slice()[0..len].to_vec();

            // deserialize
            let body = hyper::Body::from(resp_vec);
            let resp = http::Response::builder().body(body).unwrap();
            Ok(resp)
        })
    }
}

async fn rdma_send_req(rdma: Arc<Rdma>, mut req: Request<BoxBody>) {
    let mut req_header = req.uri().to_string().into_bytes();
    req_header.push(32u8);
    let len_header = req_header.len();
    let mut req_mr = rdma
        .alloc_local_mr(Layout::new::<[u8; MAX_MSG_LEN]>())
        .unwrap();
    req_mr.as_mut_slice()[0..len_header].copy_from_slice(req_header.as_slice());

    let mut len = len_header;
    while let Some(Ok(bytes)) = req.data().await {
        let req_body = bytes.to_vec();
        let len_body = req_body.len();
        assert!(len + len_body <= MAX_MSG_LEN); // TODO: len > MAX_MSG_LEN
        req_mr.as_mut_slice()[len..len + len_body].copy_from_slice(req_body.as_slice());
        len += len_body;
    }
    rdma.send_with_imm(&req_mr, len as u32).await.unwrap();
}
