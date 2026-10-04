use std::fs::{self, File};
use std::io::{BufWriter, Write};

use crate::protocols::LatencyRecord;

pub fn write_records(records: &Vec<LatencyRecord>, outpath: &std::path::PathBuf,) {
    fs::create_dir_all(outpath)
        .expect("failed to create output directory");

    let file_path = outpath.join("latency.csv");

    let file = File::create(file_path)
        .expect("failed to create latency file");

    let mut writer = BufWriter::new(file);

    writeln!(
        writer,
        "latency,send_timestamp,server_processing_time,recv_timestamp,payload_size"
    ).expect("failed to write header");

    for record in records {
        writeln!(
            writer,
            "{},{},{},{},{}",
            record.latency,
            record.send_timestamp,
            record.server_processing_time,
            record.recv_timestamp,
            record.payload_size
        ).expect("failed to write record");
    }
}