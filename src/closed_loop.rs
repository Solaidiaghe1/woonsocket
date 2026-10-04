//use woonsocket_work::args::ClientMode::OpenLoop;
use woonsocket_work::args::*;
use std::net::TcpStream;
use std::io::Read;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use std::thread;
use std::io::{Write};
use crate::protocols::LatencyRecord;
use crate::writer;
use crate::protocols::Response;

pub fn run(args: &WoonsocketClientOpt, num_threads: u64){
                let ip = args.ip;
                let port = args.port;
                let work = args.work;
                let runtime_secs = args.runtime_secs;
                let outpath = &args.outpath;

            let mut handles = Vec::new();
            let mut main_records = Vec::new();
            for i in 0..num_threads{
                let handle = thread::spawn(move||{
                let mut records: Vec<LatencyRecord> = Vec::new();

                println!("thread {:?}", i);
                let mut conn = TcpStream::connect((ip, port)).expect("failed to dial");
                let request = bincode::serialize(&work).expect("failed to serialize"); 
                let start = Instant::now();
                let mut count = 0;
                while start.elapsed().as_secs() < runtime_secs {
                    //performance struct
                    let mut record = LatencyRecord{
                        send_timestamp: SystemTime::now().duration_since(UNIX_EPOCH).expect("backwards time").as_micros() as u64,
                        ..LatencyRecord::default()
                    };
                    //start latency
                    let latency_start = Instant::now();

                    //write len then actual request to do work
                    let write_len = request.len() as u32;
                    conn.write_all(&write_len.to_be_bytes()).expect("fauled to write len");
                    conn.write_all(&request).expect("failed to write");
                    
                    //make len buf then read into len buffer and get the length
                    let mut len_buf = [0u8; 4]; 
                    match conn.read_exact(&mut len_buf) {
                        Ok(()) => {}

                        Err(e) => {
                            println!("server disconnected: {:?}", e);
                            break;
                        }
                    }
                    let length = u32::from_be_bytes(len_buf) as usize;
                    
                    //make new buf to read server message
                    let mut buf = vec![0u8; length];
                    match conn.read_exact(&mut buf) {
                        Ok(()) => {}

                        Err(e) => {
                            println!("server disconnected: {:?}", e);
                            break;
                        }
                    }
                    

                    //record latnecy
                    record.recv_timestamp = SystemTime::now().duration_since(UNIX_EPOCH).expect("backwards time").as_micros() as u64;
                    record.latency = latency_start.elapsed().as_micros() as u64;
                    //get the response struct from server message and update processing time
                    let response: Response = bincode::deserialize(&buf).expect("failed to deserialize");
                    record.server_processing_time = response.server_processing_time;
                    record.payload_size = response.payload.as_ref().map(|p| p.len() as u64).unwrap_or(0);
                    println!("server work time {:?} sent from thread {:?} number {:?}, payload {:?}", record.server_processing_time, i, count, response.payload.as_ref().map(|p| p.len()));
                    records.push(record);
                    count+=1;
                }
                records
                });
                handles.push(handle);
            }
            for h in handles{
                let mut local_res = h.join().unwrap();
                main_records.append(&mut local_res);
            }
            writer::write_records(&main_records, &outpath);
        }