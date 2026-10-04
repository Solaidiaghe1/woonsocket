use clap::Parser;
//use woonsocket_work::args::ClientMode::OpenLoop;
use woonsocket_work::args::*;
use woonsocket::{closed_loop, open_loop};


fn main(){
    let args = WoonsocketClientOpt::parse();
    match &args.mode{
        ClientMode::ClosedLoop { num_threads } => {
        closed_loop::run(&args, *num_threads);
    }

        ClientMode::OpenLoop { interval_us: interval, kind: k} =>{
            open_loop::run(&args, *interval, k);
        }
    
        }
}