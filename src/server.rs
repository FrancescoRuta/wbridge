use std::{collections::BTreeMap, sync::{atomic::AtomicBool, Arc}};

use dashmap::DashMap;
use parking_lot::Mutex;
use crate::{connection::Connection, stop_handle::StopHandleSnd};

pub struct Server<W, R> {
    accept_connection_tx: tokio::sync::mpsc::Sender<(u32, Connection<W, R>)>,
    accept_connection_rx: tokio::sync::mpsc::Receiver<(u32, Connection<W, R>)>,
}

pub struct RunningServer<W, R> {
    stop_tx: StopHandleSnd,
    accept_connection_tx: tokio::sync::mpsc::Sender<(u32, Connection<W, R>)>,
    server_future: tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>>,
}

impl<W, R> Server<W, R>
where
    W: tokio::io::AsyncWrite + Unpin + std::marker::Send + std::marker::Sync + 'static,
    R: tokio::io::AsyncRead + Unpin + std::marker::Send + std::marker::Sync + 'static,
{
    
    pub fn new() -> Self {
        let (accept_connection_tx, accept_connection_rx) = tokio::sync::mpsc::channel(16);
        Self {
            accept_connection_tx,
            accept_connection_rx,
        }
    }
    
    pub async fn start(self) -> RunningServer<W, R> {
        let (stop_tx, mut stop_rx) = crate::stop_handle::create();
        let Self {
            accept_connection_tx,
            mut accept_connection_rx,
        } = self;
        let server_future = tokio::spawn(async move {
            let runtime = ServerRuntime::new();
            loop {
                tokio::select! {
                    _ = stop_rx.wait() => break,
                    conn = accept_connection_rx.recv() => {
                        if let Some(conn) = conn {
                            let _ = runtime.push_connection(conn).await;
                        } else {
                            break
                        }
                    },
                }
            }
            Ok(())
        });
        RunningServer {
            accept_connection_tx,
            server_future,
            stop_tx,
        }
    }
    
}

impl<W, R> RunningServer<W, R> {
    
    pub async fn push_connection(&self, conn: (u32, Connection<W, R>)) -> Result<(), tokio::sync::mpsc::error::SendError<(u32, Connection<W, R>)>> {
        self.accept_connection_tx.send(conn).await
    }
    
    pub async fn stop(self) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        self.stop_tx.send_stop().await;
        self.server_future.await?
    }
    
}

struct ServerRuntime {
    is_stopped: AtomicBool,
    stop_tx_list: Arc<Mutex<BTreeMap<u32, tokio::sync::mpsc::Sender<tokio::sync::mpsc::Sender<()>>>>>,
    connections: Arc<DashMap<u32, tokio::sync::mpsc::Sender<bytes::Bytes>>>,
    broadcast_subscriptions: Arc<DashMap<u64, DashMap<u32, tokio::sync::mpsc::Sender<bytes::Bytes>>>>,
}

impl ServerRuntime {
    
    pub fn new() -> Self {
        Self {
            is_stopped: AtomicBool::new(false),
            stop_tx_list: Arc::new(Mutex::new(BTreeMap::new())),
            connections: Arc::new(DashMap::new()),
            broadcast_subscriptions: Arc::new(DashMap::new()),
        }
    }
    
    pub async fn push_connection<W, R>(&self, (id, conn): (u32, Connection<W, R>)) -> Result<(), ()>
    where
        W: tokio::io::AsyncWrite + Unpin + std::marker::Send + std::marker::Sync + 'static,
        R: tokio::io::AsyncRead + Unpin + std::marker::Send + std::marker::Sync + 'static,
    {
        let (stop_tx, mut stop_rx) = tokio::sync::mpsc::channel(1);
        let stop_tx_list = Arc::clone(&self.stop_tx_list);
        
        let old_conn = {
            let mut stop_tx_list = self.stop_tx_list.lock();
            if self.is_stopped.load(std::sync::atomic::Ordering::SeqCst) { return Err(()); }
            stop_tx_list.insert(id, stop_tx.clone())
        };
        let (conn_tx, mut conn_rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(16);
        let connections = Arc::clone(&self.connections);
        connections.insert(id, conn_tx.clone());
        
        if let Some(old_conn) = old_conn {
            let (closed_tx, mut closed_rx) = tokio::sync::mpsc::channel(1);
            if old_conn.send(closed_tx).await.is_ok() {
                let _ = closed_rx.recv().await;
            }
        }
        
        let (mut wh, mut rh) = conn.split();
        let r_notify_stop = stop_tx.clone();
        let w_notify_stop = stop_tx;
        let (stop_wh_tx, mut stop_wh_rx) = tokio::sync::mpsc::channel(1);
        let (stop_rh_tx, mut stop_rh_rx) = tokio::sync::mpsc::channel(1);
        let broadcast_subscriptions = Arc::clone(&self.broadcast_subscriptions);
        
        tokio::spawn({
            let connections = Arc::clone(&connections);
            async move {
                let stop_rx = stop_rx.recv().await;
                
                let w = async move {
                    let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);
                    if stop_wh_tx.send(tx).await.is_ok() {
                        let _ = rx.recv().await;
                    }
                };
                let r = async move {
                    let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);
                    if stop_rh_tx.send(tx).await.is_ok() {
                        let _ = rx.recv().await;
                    }
                };
                tokio::join!(w, r);
                
                if let Some(snd) = stop_rx {
                    let _ = snd.send(()).await;
                }
                
                stop_tx_list.lock().remove(&id);
                connections.remove(&id);
            }
        });
        tokio::spawn(async move {
            let snd = loop {
                tokio::select! {
                    snd = stop_wh_rx.recv() => break snd,
                    message = conn_rx.recv() => {
                        if let Some(message) = message {
                            if wh.snd(message).await.is_err() {
                                break None;
                            }
                        } else {
                            break None;
                        }
                    },
                }
            };
            if let Some(snd) = snd {
                let _ = snd.send(()).await;
            }
            let _ = w_notify_stop.send(tokio::sync::mpsc::channel::<()>(1).0).await;
        });
        tokio::spawn(async move {
            let mut broadcast_channels = Vec::new();
            let snd = loop {
                let message = tokio::select! {
                    snd = stop_rh_rx.recv() => break snd,
                    message = rh.rcv() => {
                        if let Some(message) = message {
                            if message.len() < 20 { continue; }
                            message
                        } else {
                            break None;
                        }
                    },
                };
                
                let from_conn = u32::from_be_bytes([message[4], message[5], message[6], message[7]]);
                let from_channel = u32::from_be_bytes([message[8], message[9], message[10], message[11]]);
                let to_conn = u32::from_be_bytes([message[12], message[13], message[14], message[15]]);
                //let to_channel = u32::from_be_bytes([message[16], message[17], message[18], message[19]]);
                
                if from_conn == id {
                    if to_conn == 0 {
                        let broadcast_channel = (from_conn as u64) << 32 | from_channel as u64;
                        let channels = broadcast_subscriptions.entry(broadcast_channel).or_insert_with(|| {
                            broadcast_channels.push(broadcast_channel);
                            DashMap::new()
                        })
                            .iter()
                            .map(|d| (*d.key(), d.value().clone()))
                            .collect::<Vec<_>>();
                        let mut broken_channels = Vec::new();
                        for (key, channel) in channels {
                            if channel.send(message.clone()).await.is_err() {
                                broken_channels.push(key);
                            }
                        }
                        if broken_channels.len() > 0 {
                            if let Some(m) = broadcast_subscriptions.get(&broadcast_channel) {
                                broken_channels.iter().for_each(|k| { m.remove(k); });
                            }
                        }
                    } else {
                        if let Some(data_tx) = connections.get(&to_conn).map(|c| c.clone()) {
                            let _ = data_tx.send(message).await;
                        }
                    }
                } else {
                    let broadcast_channel = (from_conn as u64) << 32 | from_channel as u64;
                    if to_conn == 0 {
                        // Subscribe
                        if let Some(c) = broadcast_subscriptions.get(&broadcast_channel) {
                            c.insert(id, conn_tx.clone());
                        }
                    } else {
                        // Unsubscribe
                        if let Some(c) = broadcast_subscriptions.get(&broadcast_channel) {
                            c.remove(&id);
                        }
                    }
                    
                }
                
            };
            if let Some(snd) = snd {
                let _ = snd.send(()).await;
            }
            let _ = r_notify_stop.send(tokio::sync::mpsc::channel::<()>(1).0).await;
            for channel in broadcast_channels {
                broadcast_subscriptions.remove(&channel);
            }
        });
        
        Ok(())
    }
    
}