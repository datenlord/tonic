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

const MAX_MSG_LEN: usize = 512;

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

    fn call(&mut self, mut req: Request<BoxBody>) -> Self::Future {
        let rdma = Arc::clone(&self.rdma);
        Box::pin(async move {
            // TODO: handle error
            let mut req_vec = req.uri().to_string().into_bytes();
            req_vec.push(32u8);
            req_vec.extend(req.data().await.unwrap().unwrap().into_iter());

            let mut req_mr = rdma
                .alloc_local_mr(Layout::new::<[u8; MAX_MSG_LEN]>())
                .unwrap();

            let len = req_vec.len();
            assert!(len <= MAX_MSG_LEN); // TODO: len > MAX_MSG_LEN
            req_mr.as_mut_slice()[0..len].copy_from_slice(req_vec.as_slice());
            rdma.send_with_imm(&req_mr, len as u32).await.unwrap();

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
