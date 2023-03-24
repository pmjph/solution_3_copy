use tokio::time::{sleep, Duration};
use chrono::prelude::*;
use clap::Parser;
use std::collections::VecDeque;
use tokio::sync::{ Mutex, MutexGuard};
use std::sync::{Arc};
use xactor::*;
mod actors;
mod signal;
use actors::{
    BufferDataRequest, BufferSink, FileSink, PerformanceIndicators, QuoteRequest,
    StockDataDownloader, StockDataProcessor,
};

use warp::{Filter, Rejection};


///
/// Number of items in the ring buffer
///
const BUFFER_SIZE: usize = 2000;

#[derive(Parser, Debug)]
#[clap(
    version = "1.0",
    author = "Claus Matzinger",
    about = "A Manning LiveProject: async Rust"
)]
struct Opts {
    #[clap(short, long, default_value = "AAPL,MSFT,UBER,GOOG")]
    symbols: String,
    #[clap(short, long)]
    from: String,
}

type Result<T, Rejection> = std::result::Result<T, Rejection>;

#[derive(Debug,)]
pub struct CustomReject(anyhow::Error);
    
impl warp::reject::Reject for CustomReject {}


async fn tail(n: usize, req: State) -> Result<impl warp::Reply, Rejection>{

    let data = req.lock().await.as_ref().unwrap().call(BufferDataRequest { n }).await.unwrap();

    Ok(warp::reply::json(&data))
}

type State = Arc<tokio::sync::Mutex<std::result::Result<Addr<BufferSink>, CustomReject>>>;

///
/// Main!
///
#[xactor::main]
async fn main() -> std::result::Result<(), Rejection> {
    
    let opts: Opts = Opts::parse();
    let from: DateTime<Utc> = opts.from.parse().expect("Couldn't parse 'from' date");
    let symbols: Vec<String> = opts.symbols.split(',').map(|s| s.to_owned()).collect();

    // Start actors. Supervisors also keep those actors alive
    let _downloader = Supervisor::start(|| StockDataDownloader).await;
    let _processor = Supervisor::start(|| StockDataProcessor).await;
    let _sink = Supervisor::start(|| FileSink {
        filename: format!("{}.csv", Utc::now().timestamp()), // create a unique file name every time
        writer: None,
    })
    .await;

    let data_actor = Supervisor::start(move || BufferSink {
        data_sink: VecDeque::with_capacity(BUFFER_SIZE),
    })
    .await
    .map_err(|e| CustomReject(e));
   
    let state = Arc::new(Mutex::new(data_actor));
    let state_filter = warp::any().map(move || state.clone());
    
    let root = warp::path::end().map(|| "Welcome to my warp server!");    

    let data_actor_route = warp::path!("tail"/usize)
        .and(warp::get())
        .and(state_filter.clone())
        .and_then(tail);
    
    let routes = root
        .or(data_actor_route);

    //warp::serve(routes).run(([127, 0, 0, 1], 5000)).await;
    warp::serve(routes).run(([127, 0, 0, 1], 4321)).await;
    // CSV header
    println!("period start,symbol,price,change %,min,max,30d avg");
    'outer: loop {
        sleep(Duration::from_millis(30000)).await;
        println!("30000 ms have elapsed");
        let now = Utc::now(); // Period end for this fetch
        for symbol in &symbols {
            if let Err(e) = Broker::from_registry().await.map_err(|e| CustomReject(e))?.publish(QuoteRequest {
                symbol: symbol.clone(),
                from,
                to: now,
            }) {
                eprint!("{}", e);
                println!("{}", e);
                break 'outer;
            }
        }
    }
    
    Ok(())
}
