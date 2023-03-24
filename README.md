tokio-warp-xactor-yahoo
Warp, tokio and xactor for yahooo-finance

=> ```rust
//unfortunatly the **tail async fn** does not work propelly. Aparently the future is not send  

async fn tail(n: usize, req: State) -> Result<impl warp::Reply, Rejection>{

    let data = req.lock().await.as_ref().unwrap().call(BufferDataRequest { n }).await.unwrap();

    Ok(warp::reply::json(&data))
}
```