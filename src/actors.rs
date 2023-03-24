use crate::signal::{MaxPrice, MinPrice, PriceDifference, AsyncStockSignal, WindowedSMA};
use crate::BUFFER_SIZE;
use chrono::prelude::*;
use serde::Serialize;
use std::collections::VecDeque;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use xactor::*;
use yahoo_finance_api as yahoo;

#[message]
#[derive(Debug, Default, Clone)]
pub struct Quotes {
    pub symbol: String,
    pub quotes: Vec<yahoo::Quote>,
}

#[message]
#[derive(Debug, Clone)]
pub struct QuoteRequest {
    pub symbol: String,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

///
/// Performance indicators of a stock data time series
///
#[message]
#[derive(Debug, Clone, Serialize)]
pub struct PerformanceIndicators {
    pub symbol: String,
    pub timestamp: DateTime<Utc>,
    pub price: f64,
    pub pct_change: f64,
    pub period_min: f64,
    pub period_max: f64,
    pub last_sma: f64,
}

///
/// Request data from the buffer actor
///
#[message(result = "Vec<PerformanceIndicators>")]
#[derive(Debug, Clone)]
pub struct BufferDataRequest {
    pub n: usize,
}

///
/// Actor that downloads stock data for a specified symbol and period
///
pub struct StockDataDownloader;

#[async_trait::async_trait]
impl Handler<QuoteRequest> for StockDataDownloader {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: QuoteRequest) {
        let symbol = msg.symbol.clone();

        let provider = yahoo::YahooConnector::new();
        let data = match provider
            .get_quote_history(&msg.symbol, msg.from, msg.to)
            .await
        {
            Ok(response) => {
                if let Ok(quotes) = response.quotes() {
                    Quotes {
                        symbol: symbol.clone(),
                        quotes,
                    }
                } else {
                    Quotes {
                        symbol: symbol.clone(),
                        quotes: vec![],
                    }
                }
            }
            Err(e) => {
                eprintln!("Ignoring API error for symbol '{}': {}", symbol, e);
                Quotes {
                    symbol: symbol.clone(),
                    quotes: vec![],
                }
            }
        };
        if let Err(e) = Broker::from_registry().await.unwrap().publish(data) {
            eprint!("{}", e);
        }
    }
}

#[async_trait::async_trait]
impl Actor for StockDataDownloader {
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        ctx.subscribe::<QuoteRequest>().await
    }
}

///
/// Actor to create performance indicators from incoming stock data
///
pub struct StockDataProcessor;

#[async_trait::async_trait]
impl Handler<Quotes> for StockDataProcessor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, mut msg: Quotes) {
        let data = msg.quotes.as_mut_slice();
        if !data.is_empty() {
            // ensure that the data is sorted by time (asc)
            data.sort_by_cached_key(|k| k.timestamp);

            let last_date = Utc.timestamp(data.last().unwrap().timestamp as i64, 0);
            let closes: Vec<f64> = data.iter().map(|q| q.close).collect();

            let diff = PriceDifference {};
            let min = MinPrice {};
            let max = MaxPrice {};
            let sma = WindowedSMA { window_size: 30 };

            let period_max: f64 = max.calculate(&closes).await.unwrap_or(0.0);
            let period_min: f64 = min.calculate(&closes).await.unwrap_or(0.0);

            let last_price = *closes.last().unwrap();
            let (_, pct_change) = diff.calculate(&closes).await.unwrap_or((0.0, 0.0));
            let sma = sma.calculate(&closes).await.unwrap();

            let data = PerformanceIndicators {
                timestamp: last_date,
                symbol: msg.symbol.clone(),
                price: last_price,
                pct_change,
                period_min,
                period_max,
                last_sma: *sma.last().unwrap_or(&0.0),
            };

            if let Err(e) = Broker::from_registry().await.unwrap().publish(data) {
                eprint!("{}", e);
            }

            println!(
                "{},{},${:.2},{:.2}%,${:.2},${:.2},${:.2}",
                last_date.to_rfc3339(),
                msg.symbol,
                last_price,
                pct_change * 100.0,
                period_min,
                period_max,
                sma.last().unwrap_or(&0.0)
            );
        } else {
            println!("Got nothing");
        }
    }
}

#[async_trait::async_trait]
impl Actor for StockDataProcessor {
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        ctx.subscribe::<Quotes>().await
    }
}

///
/// Actor for storing incoming messages in a csv file
///
#[derive(Default, Debug)]
pub struct FileSink {
    pub filename: String,
    pub writer: Option<BufWriter<File>>,
}

#[async_trait::async_trait]
impl Actor for FileSink {
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        let mut file = File::create(&self.filename)
            .unwrap_or_else(|_| panic!("Could not open target file '{}'", self.filename));
        let _ = writeln!(
            &mut file,
            "period start,symbol,price,change %,min,max,30d avg"
        );
        self.writer = Some(BufWriter::new(file));
        ctx.subscribe::<PerformanceIndicators>().await
    }

    async fn stopped(&mut self, ctx: &mut Context<Self>) {
        if let Some(writer) = &mut self.writer {
            writer
                .flush()
                .expect("Something happened when flushing. Data loss :(")
        };
        ctx.stop(None);
    }
}

#[async_trait::async_trait]
impl Handler<PerformanceIndicators> for FileSink {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: PerformanceIndicators) {
        if let Some(file) = &mut self.writer {
            let _ = writeln!(
                file,
                "{},{},${:.2},{:.2}%,${:.2},${:.2},${:.2}",
                msg.timestamp.to_rfc3339(),
                msg.symbol,
                msg.price,
                msg.pct_change * 100.0,
                msg.period_min,
                msg.period_max,
                msg.last_sma
            );
        }
    }
}

///
/// Actor that saves incoming messages to a ring buffer
///
#[derive(Default, Debug)]
pub struct BufferSink {
    pub data_sink: VecDeque<PerformanceIndicators>,
}

impl Service for BufferSink {}

#[async_trait::async_trait]
impl Actor for BufferSink {
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        ctx.subscribe::<PerformanceIndicators>().await
    }
}

#[async_trait::async_trait]
impl Handler<PerformanceIndicators> for BufferSink {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: PerformanceIndicators) {
        self.data_sink.push_front(msg);
        self.data_sink.truncate(BUFFER_SIZE);
    }
}

#[async_trait::async_trait]
impl Handler<BufferDataRequest> for BufferSink {
    async fn handle(
        &mut self,
        _ctx: &mut Context<Self>,
        msg: BufferDataRequest,
    ) -> Vec<PerformanceIndicators> {
        self.data_sink.iter().take(msg.n).cloned().collect()
    }
}
