//use woonsocket_work::args::ClientMode::OpenLoop;
use woonsocket_work::args::*;
use std::net::TcpStream;
use std::io::Read;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use std::thread;
use std::io::{Write};
use crate::protocols::LatencyRecord;
use crate::writer;
use crate::protocols::Response;
use rand_distr::{Distribution, Exp};
use rand;
use std::sync::mpsc;



pub fn run(args: &WoonsocketClientOpt, interval: u64, kind: &OpenLoopKind ){
            let ip = args.ip;
            let port = args.port;
            let work = args.work;
            let runtime_secs = args.runtime_secs;
            let outpath = &args.outpath;

            let mut conn = TcpStream::connect((ip, port)).expect("failed to dial");
            let reader = conn.try_clone().expect("failed to clone conn");
            let (tx, rx) = mpsc::channel();
            let reader_handle = thread::spawn(move || {
            read_responses(reader, rx, Instant::now(), runtime_secs)
            });

            let request = bincode::serialize(&work).expect("failed to serialize"); 
            let start = Instant::now();
            let mut next_tick = Instant::now();

            while start.elapsed().as_secs() < runtime_secs {
                //performance struct
                let wait_time = match kind {
                OpenLoopKind::Constant => {
                    Duration::from_micros(interval)
                }

                OpenLoopKind::Poisson => {
                    let lambda =  1.0 / interval as f64;
                    let exp = Exp::new(lambda).unwrap();
                    let wait_micros = exp.sample(&mut rand::rng());
                    Duration::from_micros(wait_micros as u64)
                }
            };
                next_tick+=wait_time;

                thread::sleep(next_tick.saturating_duration_since(Instant::now()));
                if start.elapsed().as_secs() >= runtime_secs {
                    break;}
              //start latency
                let send_timestamp: u64 = SystemTime::now().duration_since(UNIX_EPOCH).expect("Wemt backwards in time").as_micros() as u64;
                let latency_start = Instant::now();
                //write len then actual request to do work
                let write_len = request.len() as u32;
                conn.write_all(&write_len.to_be_bytes()).expect("fauled to write len");
                conn.write_all(&request).expect("failed to write");
                //make len buf then read into len buffer and get the length
                println!("write");
                tx.send((send_timestamp, latency_start)).expect("failed to send timing info");
            }
        drop(tx);
        let records = reader_handle.join().expect("failed to join reader");
        writer::write_records(&records, &outpath);
        }


fn read_responses(mut reader: TcpStream, rx: std::sync::mpsc::Receiver<(u64, Instant)>, start: Instant, runtime_secs: u64) -> Vec<LatencyRecord> {

    let mut records = Vec::new();

    while let Ok((send_timestamp, latency_start)) = rx.recv() {
        let mut len_buf = [0u8; 4];

        if !read_until_runtime(&mut reader, &mut len_buf, start, runtime_secs){
            break
        }

        let length = u32::from_be_bytes(len_buf) as usize;

        let mut buf = vec![0u8; length];

        if !read_until_runtime(&mut reader, &mut buf, start, runtime_secs){
            break
        }

        let recv_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("backwards time")
            .as_micros() as u64;

        let response: Response =
            bincode::deserialize(&buf)
                .expect("failed to deserialize");

        records.push(LatencyRecord {
            send_timestamp,
            recv_timestamp,
            latency: latency_start.elapsed().as_micros() as u64,
            server_processing_time: response.server_processing_time,
            payload_size: response
                .payload
                .as_ref()
                .map(|p| p.len() as u64)
                .unwrap_or(0),
        });
        println!("read, latency: {:?}", latency_start.elapsed().as_micros() as u64)
    }

    records
}


fn read_until_runtime(stream: &mut TcpStream, buf: &mut [u8], start: Instant, runtime_secs: u64,) -> bool {
    let mut bytes_read = 0;

    while bytes_read < buf.len() {
        // Runtime is over
        if start.elapsed().as_secs() >= runtime_secs {
            return false;
        }

        match stream.read(&mut buf[bytes_read..]) {
            // Connection closed
            Ok(0) => return false,

            // Successfully got some bytes
            Ok(n) => {
                bytes_read += n;
            }

            // 50ms timeout — go around and check runtime again
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                continue;
            }

            // Actual socket error
            Err(_) => return false,
        }
    }
    true
}