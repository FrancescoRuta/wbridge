use client::Client;
use connection::Connection;
use server::Server;
use tokio::{io::{AsyncWriteExt, AsyncReadExt}, time::Instant};

pub mod client;
mod connection;
pub mod server;
mod stop_handle;

pub const HEADER_SIZE: usize = 20;

#[tokio::main]
async fn main() {
    let (stop_handle_snd, stop_handle_rcv) = stop_handle::create();
    let server_handle = tokio::spawn(server(stop_handle_rcv));
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let (mut r, w) = tokio::net::TcpSocket::new_v4().unwrap().connect("127.0.0.1:8002".parse().unwrap()).await.unwrap().into_split();
    let client1_id = r.read_u32().await.unwrap();
    let client1 = Client::new(client1_id, Connection::new(w, r));
    let (mut r, w) = tokio::net::TcpSocket::new_v4().unwrap().connect("127.0.0.1:8002".parse().unwrap()).await.unwrap().into_split();
    let client2_id = r.read_u32().await.unwrap();
    let client2 = Client::new(client2_id, Connection::new(w, r));
    
    let client1_handle = tokio::spawn(async move {
        let client = client1.start();
        
        let broadcast_channel = client.open_new_channel().await;
        let mut channel1 = client.get_channel_if_free(1).await.unwrap();
        
        broadcast_channel.send_broadcast([]).await.unwrap();
        println!("broadcast_channel_id: {}", broadcast_channel.id());
        channel1.send(client2_id, 1, broadcast_channel.id().to_be_bytes()).await.unwrap();
        println!("1: {}", std::str::from_utf8(&channel1.read().await.unwrap().data).unwrap());
        broadcast_channel.send_broadcast(b"Test broadcast").await.unwrap();
        println!("DATA SENT");
        println!("1: {}", std::str::from_utf8(&channel1.read().await.unwrap().data).unwrap());
        
        let test_message = bytes::BytesMut::zeroed(10 * 1024 * 1024).freeze();
        
        for _ in 0..10000 {
            broadcast_channel.send_broadcast(test_message.clone()).await.unwrap();
        }
        
        println!("1: {}", std::str::from_utf8(&channel1.read().await.unwrap().data).unwrap());
        
        client.stop().await;
    });
    
    let client2_handle = tokio::spawn(async move {
        let client = client2.start();
        
        let mut channel1 = client.get_channel_if_free(1).await.unwrap();
        
        let broadcast_channel_id = u32::from_be_bytes((&*channel1.read().await.unwrap().data).try_into().unwrap());
        println!("broadcast_channel_id: {}", broadcast_channel_id);
        let mut broadcast_subsription = client.subscribe_to_broadcast(client1_id, broadcast_channel_id).await.unwrap();
        println!("SUBSCRIPTION OK");
        channel1.send(client1_id, 1, b"CONN2 READY").await.unwrap();
        let message = broadcast_subsription.read().await.unwrap().data;
        let message = std::str::from_utf8(&message).unwrap();
        
        println!("2: {}", message);
        channel1.send(client1_id, 1, format!("FWD : {}", message).as_bytes()).await.unwrap();
        
        let now = Instant::now();
        let mut data_rec = 0;
        
        for _ in 0..10000 {
            data_rec += broadcast_subsription.read().await.unwrap().data.len();
        }
        
        channel1.send(client1_id, 1, format!("Broadcast speed: {:.3} MB/s", data_rec as f64 / 1024.0 / 1024.0 / now.elapsed().as_secs_f64()).as_bytes()).await.unwrap();
        
        //client.stop();
    });
    
    let _ = client1_handle.await;
    let _ = client2_handle.await;
    
    stop_handle_snd.send_stop().await;
    
    let _ = server_handle.await;
}

async fn server(mut stop_handle_rcv: stop_handle::StopHandleRcv) {
    let Ok(listener) = tokio::net::TcpListener::bind(&"127.0.0.1:8002").await else {
        eprintln!("Cannot bind port");
        return;
    };
    let server = Server::new();
    let server = server.start().await;
    let mut connid = 0;
    loop {
        let Ok((socket, _)) = tokio::select! {
            _ = stop_handle_rcv.wait() => break,
            c = listener.accept() => c,
        } else { break; };
        let (r, mut w) = socket.into_split();
        connid += 1;
        if let Err(e) = w.write_u32(connid).await {
            eprintln!("Write error: {}", e);
        }
        if let Err(e) = w.flush().await {
            eprintln!("Flush error: {}", e);
        }
        if let Err(e) = server.push_connection((connid, Connection::new(w, r))).await {
            eprintln!("Connection error: {}", e);
        }
    }
    let _ = server.stop().await;
}
