use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub struct Response {
    pub server_processing_time: u64,
    pub payload: Option<Vec<u8>>,
}

#[derive(Default)]
pub struct LatencyRecord {
    pub latency: u64,
    pub send_timestamp: u64,
    pub server_processing_time: u64,
    pub recv_timestamp: u64,
    pub payload_size: u64,
}