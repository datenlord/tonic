use async_rdma::{LocalMr, LocalMrReadAccess, LocalMrWriteAccess};

/// RDMA request. Corresponding to gRPC http::Request
///
/// Request content:
/// {Path of Service}{Blank Space}{Serialied Message}
#[derive(Debug)]
pub struct RdmaRequest {
    req_mr: LocalMr,
    len: usize,
    resp_mr: LocalMr,
}

/// RDMA response. Corresponding to gRPC http::Response
#[derive(Debug)]
pub struct RdmaResponse {
    /// MR where the response message is stored
    pub resp_mr: LocalMr,
    /// length of message in MR
    pub len: usize,
}

impl RdmaRequest {
    /// Create a new RdmaRequest
    pub fn new(req_mr: LocalMr, len: usize, resp_mr: LocalMr) -> Self {
        Self {
            req_mr,
            len,
            resp_mr,
        }
    }
    /// Get the index of separator(i.e. blank space).
    fn separator_index(&self) -> usize {
        self.req_mr
            .as_slice()
            .iter()
            .position(|num| *num == ' ' as u8) // blank space
            .unwrap()
    }
    /// Get service name.
    pub fn service(&self) -> &str {
        let pos = self.separator_index();
        let idx = self.req_mr.as_slice()[0..pos]
            .iter()
            .rev()
            .position(|c| *c == '/' as u8)
            .unwrap();
        std::str::from_utf8(&self.req_mr.as_slice()[0..pos - idx - 1]).unwrap()
    }
    /// Get path of service function.
    pub fn path(&self) -> &str {
        let pos = self.separator_index();
        std::str::from_utf8(&self.req_mr.as_slice()[0..pos]).unwrap()
    }
    /// Get serialized data from MR.
    pub fn body(&self) -> &[u8] {
        let pos = self.separator_index();
        let res = &self.req_mr.as_slice()[pos + 1..self.len];
        println!("body: {:?}", res);
        res
    }
}

impl RdmaResponse {
    /// Create RdmaResponse from RdmaRequest.
    pub fn from_req(req: RdmaRequest) -> Self {
        Self {
            resp_mr: req.resp_mr,
            len: 0,
        }
    }
    /// Get mutable reference of MR slice.
    pub fn buf(&mut self) -> &mut [u8] {
        unsafe { self.resp_mr.as_mut_slice_unchecked() }
    }
}
