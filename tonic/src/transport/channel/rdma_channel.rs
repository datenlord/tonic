use async_rdma::{LocalMr, Rdma};
use std::{alloc::Layout, io, sync::Arc};
use tokio::net::ToSocketAddrs;

/// A RDMA transport channel
#[derive(Debug, Clone)]
pub struct RdmaChannel {
    rdma: Arc<Rdma>,
}

const MAX_MSG_LEN: usize = 10240;

impl RdmaChannel {
    /// Create a new [`RdmaChannel`]
    pub async fn new<A>(addr: A) -> Result<Self, io::Error>
    where
        A: ToSocketAddrs,
    {
        // TODO: expose these params to user (create a `RdmaEndpoint` maybe)
        let rdma = Arc::new(Rdma::connect(addr, 1, 1, MAX_MSG_LEN).await?);
        Ok(Self { rdma })
    }
}

/// RDMA service
#[async_trait::async_trait]
pub trait RdmaService {
    /// 
    fn alloc_mr(&self) -> io::Result<LocalMr>;
    /// 
    async fn call(&mut self, req: LocalMr, len: usize) -> (LocalMr, Option<u32>);
}

#[async_trait::async_trait]
impl RdmaService for RdmaChannel {
    fn alloc_mr(&self) -> io::Result<LocalMr> {
        self.rdma.alloc_local_mr(Layout::new::<[u8; MAX_MSG_LEN]>())
    }
    async fn call(&mut self, req: LocalMr, len: usize) -> (LocalMr, Option<u32>) {
        self.rdma.send_with_imm(&req, len as u32).await.unwrap();
        self.rdma.receive_with_imm().await.unwrap()
    }
}
