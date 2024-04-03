use std::{collections::BTreeMap, sync::{atomic::AtomicBool, Arc}, marker::PhantomData};

use bytes::BufMut;
use dashmap::DashMap;
use parking_lot::Mutex;
use crate::{connection::ConnectionManager, message::{BroadcastMessage, FrozenMessage, Message, PrepardMessage}, stop_handle::StopHandleSnd, HEADER_SIZE};

pub struct Server<Connection> {
    accept_connection_tx: tokio::sync::mpsc::Sender<(u32, Connection)>,
    accept_connection_rx: tokio::sync::mpsc::Receiver<(u32, Connection)>,
}

pub struct RunningServer<Connection> {
    stop_tx: StopHandleSnd,
    connections: Arc<DashMap<u32, tokio::sync::mpsc::Sender<bytes::Bytes>>>,
    broadcast_subscriptions: Arc<DashMap<u64, DashMap<u32, tokio::sync::mpsc::Sender<bytes::Bytes>>>>,
    accept_connection_tx: tokio::sync::mpsc::Sender<(u32, Connection)>,
    server_future: tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>>,
    stop_tx_list: Arc<Mutex<BTreeMap<u32, tokio::sync::mpsc::Sender<tokio::sync::mpsc::Sender<()>>>>>,
}

impl<Connection> Server<Connection>
where
    Connection: ConnectionManager + Send + 'static,
{
    
    pub fn new() -> Self {
        let (accept_connection_tx, accept_connection_rx) = tokio::sync::mpsc::channel(16);
        Self {
            accept_connection_tx,
            accept_connection_rx,
        }
    }
    
    pub async fn start(self) -> RunningServer<Connection> {
        let (stop_tx, mut stop_rx) = crate::stop_handle::create();
        let Self {
            accept_connection_tx,
            mut accept_connection_rx,
        } = self;
        let connections = Arc::new(DashMap::new());
        let broadcast_subscriptions = Arc::new(DashMap::new());
        let stop_tx_list = Arc::new(Mutex::new(BTreeMap::new()));
        let server_future = tokio::spawn({
            let connections = Arc::clone(&connections);
            let broadcast_subscriptions = Arc::clone(&broadcast_subscriptions);
            let stop_tx_list = Arc::clone(&stop_tx_list);
            async move {
                let runtime = ServerRuntime::new(connections, broadcast_subscriptions, stop_tx_list);
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
            }
        });
        RunningServer {
            connections,
            broadcast_subscriptions,
            accept_connection_tx,
            server_future,
            stop_tx_list,
            stop_tx,
        }
    }
    
}

impl<Connection> RunningServer<Connection> {
    
    pub async fn push_connection(&self, conn: (u32, Connection)) -> Result<(), tokio::sync::mpsc::error::SendError<(u32, Connection)>> {
        self.accept_connection_tx.send(conn).await
    }
    
    pub fn push_connection_async<F: std::future::Future<Output = Option<Connection>> + Send + 'static>(&self, conn: (u32, F), callback: impl FnOnce(Option<Result<(), tokio::sync::mpsc::error::SendError<(u32, Connection)>>>) + Send + 'static) where Connection: Send + 'static {
        let accept_connection_tx = self.accept_connection_tx.clone();
        tokio::spawn(async move {
            let (id, conn) = conn;
            if let Some(conn) = conn.await {
                callback(Some(accept_connection_tx.send((id, conn)).await));
            } else {
                callback(None);
            }
        });
    }
    
    pub fn get_local_connection_factory(&self) -> LocalConnectionFactory {
        LocalConnectionFactory {
            connections: Arc::clone(&self.connections),
            broadcast_subscriptions: Arc::clone(&self.broadcast_subscriptions),
        }
    }
    
    pub fn get_local_connection(&self, connection_id: u32) -> LocalConnection {
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        self.connections.insert(connection_id, tx);
        LocalConnection {
            connection_id,
            data_rx: rx,
            connections: Arc::clone(&self.connections),
            broadcast_subscriptions: Arc::clone(&self.broadcast_subscriptions),
        }
    }
    
    pub async fn stop(self) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        self.stop_tx.send_stop().await;
        self.server_future.await??;
        Self::stop_clients(self.stop_tx_list).await;
        Ok(())
    }
    
    async fn stop_clients(stop_tx_list: Arc<Mutex<BTreeMap<u32, tokio::sync::mpsc::Sender<tokio::sync::mpsc::Sender<()>>>>>) {
        let stop_handles = {
            let stop_tx_list = stop_tx_list.lock();
            let ss = stop_tx_list.iter().map(|(_, s)| s.clone()).collect::<Vec<_>>();
            drop(stop_tx_list);
            ss
        };
        let mut r = Vec::with_capacity(stop_handles.len());
        for s in stop_handles {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            if s.send(tx).await.is_ok() {
                r.push(rx);
            }
        }
        for mut r in r {
            let _ = r.recv().await;
        }
    }
    
}

struct ServerRuntime {
    is_stopped: AtomicBool,
    stop_tx_list: Arc<Mutex<BTreeMap<u32, tokio::sync::mpsc::Sender<tokio::sync::mpsc::Sender<()>>>>>,
    connections: Arc<DashMap<u32, tokio::sync::mpsc::Sender<bytes::Bytes>>>,
    broadcast_subscriptions: Arc<DashMap<u64, DashMap<u32, tokio::sync::mpsc::Sender<bytes::Bytes>>>>,
}

impl ServerRuntime {
    
    pub fn new(connections: Arc<DashMap<u32, tokio::sync::mpsc::Sender<bytes::Bytes>>>, broadcast_subscriptions: Arc<DashMap<u64, DashMap<u32, tokio::sync::mpsc::Sender<bytes::Bytes>>>>, stop_tx_list: Arc<Mutex<BTreeMap<u32, tokio::sync::mpsc::Sender<tokio::sync::mpsc::Sender<()>>>>>) -> Self {
        Self {
            is_stopped: AtomicBool::new(false),
            stop_tx_list,
            connections,
            broadcast_subscriptions,
        }
    }
    
    pub async fn push_connection<Connection: ConnectionManager + Send + 'static>(&self, (id, conn): (u32, Connection)) -> Result<(), ()> {
        let (stop_tx, mut stop_rx) = tokio::sync::mpsc::channel(1);
        let stop_tx_list = Arc::clone(&self.stop_tx_list);
        
        let old_conn = {
            let mut stop_tx_list = self.stop_tx_list.lock();
            if self.is_stopped.load(std::sync::atomic::Ordering::SeqCst) { return Err(()); }
            stop_tx_list.insert(id, stop_tx.clone())
        };
        let (conn_tx, conn_rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(16);
        let connections = Arc::clone(&self.connections);
        connections.insert(id, conn_tx.clone());
        
        if let Some(old_conn) = old_conn {
            let (closed_tx, mut closed_rx) = tokio::sync::mpsc::channel(1);
            if old_conn.send(closed_tx).await.is_ok() {
                let _ = closed_rx.recv().await;
            }
        }
        
        let (stop_wh_tx, stop_wh_rx) = tokio::sync::mpsc::channel::<tokio::sync::mpsc::Sender<()>>(1);
        let (stop_rh_tx, stop_rh_rx) = tokio::sync::mpsc::channel::<tokio::sync::mpsc::Sender<()>>(1);
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
        conn.manage(id, broadcast_subscriptions, connections, conn_tx, stop_wh_rx, stop_rh_rx, conn_rx, stop_tx);
        
        Ok(())
    }
    
}

#[derive(Clone)]
pub struct LocalConnectionFactory {
    connections: Arc<DashMap<u32, tokio::sync::mpsc::Sender<bytes::Bytes>>>,
    broadcast_subscriptions: Arc<DashMap<u64, DashMap<u32, tokio::sync::mpsc::Sender<bytes::Bytes>>>>,
}

impl LocalConnectionFactory {
    
    pub fn get_local_connection(&self, connection_id: u32) -> LocalConnection {
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        self.connections.insert(connection_id, tx);
        LocalConnection {
            connection_id,
            data_rx: rx,
            connections: Arc::clone(&self.connections),
            broadcast_subscriptions: Arc::clone(&self.broadcast_subscriptions),
        }
    }
    
}

pub struct LocalConnection {
    connection_id: u32,
    data_rx: tokio::sync::mpsc::Receiver<bytes::Bytes>,
    connections: Arc<DashMap<u32, tokio::sync::mpsc::Sender<bytes::Bytes>>>,
    broadcast_subscriptions: Arc<DashMap<u64, DashMap<u32, tokio::sync::mpsc::Sender<bytes::Bytes>>>>,
}

impl LocalConnection {
    
    pub fn get_broadcast_subscription_factory(&self) -> BroadcastSubscriptionFactory {
        BroadcastSubscriptionFactory {
            connection_id: self.connection_id,
            broadcast_subscriptions: Arc::clone(&self.broadcast_subscriptions),
        }
    }
    pub fn subscribe_to_broadcast(&self, addr: u32, channel: u32) -> Result<BroadcastSubscription, ()> {
        let broadcast_channel = (addr as u64) << 32 | channel as u64;
        if let Some(b) = self.broadcast_subscriptions.get(&broadcast_channel) {
            if let dashmap::mapref::entry::Entry::Vacant(e) = b.entry(self.connection_id) {
                let (tx, rx) = tokio::sync::mpsc::channel(16);
                e.insert(tx);
                Ok(BroadcastSubscription {
                    this_connection_id: self.connection_id,
                    broadcast_channel,
                    broadcast_subscriptions: Arc::clone(&self.broadcast_subscriptions),
                    data_reader: rx,
                })
            } else {
                Err(())
            }
        } else {
            Err(())
        }
    }
    
    pub fn get_writer_factory(&self) -> LocalConnectionWriterFactory {
        LocalConnectionWriterFactory {
            conn_id: self.connection_id,
            connections: Arc::clone(&self.connections),
        }
    }
    
    pub async fn read_next(&mut self) -> Option<Message> {
        if let Some(message) = self.data_rx.recv().await {
            let from_conn = u32::from_be_bytes([message[4], message[5], message[6], message[7]]);
            let from_channel = u32::from_be_bytes([message[8], message[9], message[10], message[11]]);
            let to_conn = u32::from_be_bytes([message[12], message[13], message[14], message[15]]);
            let to_channel = u32::from_be_bytes([message[16], message[17], message[18], message[19]]);
            Some(Message {
                from_conn,
                from_channel,
                to_conn,
                to_channel,
                data: message.slice(HEADER_SIZE..),
            })
        } else {
            None
        }
    }
    
}

#[derive(Clone)]
pub struct BroadcastSubscriptionFactory {
    connection_id: u32,
    broadcast_subscriptions: Arc<DashMap<u64, DashMap<u32, tokio::sync::mpsc::Sender<bytes::Bytes>>>>,
}

impl BroadcastSubscriptionFactory {
    
    pub fn subscribe_to_broadcast(&self, addr: u32, channel: u32) -> Result<BroadcastSubscription, ()> {
        let broadcast_channel = (addr as u64) << 32 | channel as u64;
        if let Some(b) = self.broadcast_subscriptions.get(&broadcast_channel) {
            if let dashmap::mapref::entry::Entry::Vacant(e) = b.entry(self.connection_id) {
                let (tx, rx) = tokio::sync::mpsc::channel(16);
                e.insert(tx);
                Ok(BroadcastSubscription {
                    this_connection_id: self.connection_id,
                    broadcast_channel,
                    broadcast_subscriptions: Arc::clone(&self.broadcast_subscriptions),
                    data_reader: rx,
                })
            } else {
                Err(())
            }
        } else {
            Err(())
        }
    }
    
}

pub struct BroadcastSubscription {
    this_connection_id: u32,
    broadcast_channel: u64,
    data_reader: tokio::sync::mpsc::Receiver<bytes::Bytes>,
    broadcast_subscriptions: Arc<DashMap<u64, DashMap<u32, tokio::sync::mpsc::Sender<bytes::Bytes>>>>,
}

impl BroadcastSubscription {
    pub async fn cancel(&self) {
        if let Some(b) = self.broadcast_subscriptions.get(&self.broadcast_channel) {
            b.remove(&self.this_connection_id);
        }
    }
    pub async fn read(&mut self) -> Option<BroadcastMessage> {
        if let Some(message) = self.data_reader.recv().await {
            let from_conn = u32::from_be_bytes([message[4], message[5], message[6], message[7]]);
            let from_channel = u32::from_be_bytes([message[8], message[9], message[10], message[11]]);
            Some(BroadcastMessage {
                from_conn,
                from_channel,
                data: message.slice(HEADER_SIZE..),
            })
        } else {
            None
        }
    }
}

#[derive(Clone)]
pub struct LocalConnectionWriterFactory {
    conn_id: u32,
    connections: Arc<DashMap<u32, tokio::sync::mpsc::Sender<bytes::Bytes>>>,
}

impl LocalConnectionWriterFactory {
    
    pub fn get_writer(&self, channel: u32) -> LocalConnectionWriter {
        LocalConnectionWriter {
            conn_id: self.conn_id,
            channel_id: channel,
            connections: Arc::clone(&self.connections),
        }
    }
    
}


#[derive(Clone)]
pub struct LocalConnectionWriter {
    conn_id: u32,
    channel_id: u32,
    connections: Arc<DashMap<u32, tokio::sync::mpsc::Sender<bytes::Bytes>>>,
}

impl LocalConnectionWriter {
    pub fn conn_id(&self) -> u32 {
        self.conn_id
    }
    pub fn id(&self) -> u32 {
        self.channel_id
    }
    pub async fn send(&self, addr: u32, channel: u32, data: impl AsRef<[u8]>) -> Result<(), ()> {
        if let Some(c) = {self.connections.get(&addr).map(|c| c.clone())} {
            let data = data.as_ref();
            let mut request = bytes::BytesMut::with_capacity(20 + data.len());
            request.put_u32(data.len() as u32);
            request.put_u32(self.conn_id);
            request.put_u32(self.channel_id);
            request.put_u32(addr);
            request.put_u32(channel);
            request.put_slice(data);
            if c.send(request.freeze()).await.is_ok() {
                Ok(())
            } else {
                Err(())
            }
        } else {
            Err(())
        }
    }
    pub async fn send_broadcast(&self, data: impl AsRef<[u8]>) -> Result<(), ()> {
        self.send(0, 0, data).await
    }
    pub async fn send_prepared<'a>(&'a self, addr: u32, channel: u32, mut message: PrepardMessage) -> Result<FrozenMessage<'a>, ()> {
        if let Some(c) = {self.connections.get(&addr).map(|c| c.clone())} {
            message.buffer[0..4].copy_from_slice(&(message.len as u32).to_be_bytes());
            message.buffer[4..8].copy_from_slice(&self.conn_id().to_be_bytes());
            message.buffer[8..12].copy_from_slice(&self.id().to_be_bytes());
            message.buffer[12..16].copy_from_slice(&addr.to_be_bytes());
            message.buffer[16..20].copy_from_slice(&channel.to_be_bytes());
            let message = message.buffer.freeze();
            if c.send(message.clone()).await.is_ok() {
                Ok(FrozenMessage(addr, message, PhantomData::default()))
            } else {
                Err(())
            }
        } else {
            Err(())
        }
    }
    pub async fn send_prepared_broadcast<'a>(&'a self, message: PrepardMessage) -> Result<FrozenMessage<'a>, ()> {
        self.send_prepared(0, 0, message).await
    }
    pub async fn send_frozen(&self, message: &FrozenMessage<'_>) -> Result<(), ()> {
        if let Some(c) = {self.connections.get(&message.0).map(|c| c.clone())} {
            if c.send(message.1.clone()).await.is_ok() {
                Ok(())
            } else {
                Err(())
            }
        } else {
            Err(())
        }
    }
}
