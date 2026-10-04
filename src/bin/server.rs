use clap::Parser;
use woonsocket_work::args::*;
use std::net::{TcpListener, TcpStream};
use std::io::Read;
use woonsocket_work::Work;
use std::io::Write;
use std::time::Instant;
use serde::{Deserialize, Serialize};



#[derive(Serialize, Deserialize)]
struct Response{
    server_work_time: u64,
    payload: Option<Vec<u8>>,
}
fn main(){
    let args = WoonsocketServerOpt::parse();
    let listener = TcpListener::bind(("0.0.0.0", args.port))
            .expect("failed to bind");
    listener.set_nonblocking(true).expect("failed to set to non blocking");
    let server_start = Instant::now();
    while server_start.elapsed().as_secs() < args.runtime_secs{
        match listener.accept() {
        Ok((stream, _addr)) => {
            std::thread::spawn(move || {
                handle_client(stream, server_start, args.runtime_secs);
            });
        }

        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
            // No client waiting right now
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        Err(e) => {
            eprintln!("failed to accept client: {:?}", e);
        }

}       
    }
    println!("server disconnected after {:?}", args.runtime_secs);
}

fn handle_client(mut stream: TcpStream, serv_start: Instant, time_up: u64){
    stream.set_read_timeout(Some(std::time::Duration::from_millis(100))).expect("failed to set read timeout");

    while serv_start.elapsed().as_secs() < time_up{
    let mut len_buf = [0u8; 4];
    

   if !read_exact_until_runtime(&mut stream, &mut len_buf, serv_start, time_up){
    break;
   }
    let client_msg_len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; client_msg_len];

    if !read_exact_until_runtime(&mut stream, &mut buf, serv_start, time_up){
        break;
    }

    let work_start: Instant = Instant::now();
    let work: Work = bincode::deserialize(&buf).expect("failed to deserialize");

    let _result = work.perform();
    let responding = Response{
        server_work_time: work_start.elapsed().as_micros() as u64,
        payload: _result
    };
    
    let serialized_response = bincode::serialize(&responding).expect("failed to serialize response");
    let  length = serialized_response.len() as u32;
    stream.write_all(&length.to_be_bytes()).expect("failed to write length ");
    stream.write_all(&serialized_response).expect("failed to write serialized response");
    println!("recieved from client {:?}", work)
    };
}

fn read_exact_until_runtime(stream: &mut TcpStream, buf: &mut [u8], server_start: Instant, runtime_secs: u64,) -> bool {
    let mut bytes_read = 0;

    while bytes_read < buf.len() {
        if server_start.elapsed().as_secs() >= runtime_secs {
            return false;
        }

        match stream.read(&mut buf[bytes_read..]) {
            Ok(0) => return false,

            Ok(n) => {
                bytes_read += n;
            }

            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut => {}

            Err(_) => return false,
        }
    }

    true
}
