use std::{sync::{atomic::AtomicU32, Arc}, marker::PhantomData};

use bytes::{BytesMut, BufMut, Buf};
use dashmap::DashMap;

use crate::{stop_handle::{StopHandleSnd, self}, HEADER_SIZE, message::{Message, BroadcastMessage, PrepardMessage, FrozenMessage}, connection::DuplexConnection};

pub struct Client<Connection> {
    id: u32,
    connection: Connection,
}

impl<Connection> Client<Connection>
where
    Connection: DuplexConnection + Send + 'static,
{
    
    pub fn new(id: u32, connection: Connection) -> Self {
        Self {
            id,
            connection,
        }
    }
    
    pub fn start(self) -> RunningClient {
        let (stop_tx, mut stop_rx) = stop_handle::create();
        let (stop_tx_w, mut stop_rx_w) = stop_handle::create();
        let (stop_tx_r, mut stop_rx_r) = stop_handle::create();
        let (data_writer_tx, mut data_writer_rx) = tokio::sync::mpsc::channel(16);
        let data_channels: Arc<DashMap<u32, tokio::sync::mpsc::Sender<Message>>> = Arc::new(DashMap::new());
        let broadcast_subscriptions: Arc<DashMap<u64, tokio::sync::mpsc::Sender<BroadcastMessage>>> = Arc::new(DashMap::new());
        let mut join_handles = Vec::new();
        let w_notify_stop = stop_tx.clone();
        let r_notify_stop = stop_tx.clone();
        let mut connection = self.connection;
        
        join_handles.push(tokio::spawn(async move {
            stop_rx.wait().await;
            stop_tx_w.send_stop().await;
            stop_tx_r.send_stop().await;
        }));
        join_handles.push(tokio::spawn({
            let data_channels = Arc::clone(&data_channels);
            let broadcast_subscriptions = Arc::clone(&broadcast_subscriptions);
            async move {
                let (mut wh, mut rh) = connection.split();
                let wh = async move {
                    loop {
                        tokio::select! {
                            _ = stop_rx_w.wait() => break,
                            message = data_writer_rx.recv() => {
                                if let Some(message) = message {
                                    if wh.snd(message).await.is_err() {
                                        break;
                                    }
                                } else {
                                    break;
                                }
                            },
                        }
                    };
                    w_notify_stop.send_stop().await;
                };
                let rh = async move {
                    loop {
                        let message = tokio::select! {
                            _ = stop_rx_r.wait() => break,
                            message = rh.rcv() => {
                                if let Some(message) = message {
                                    message
                                } else {
                                    break;
                                }
                            },
                        };
                        let from_conn = u32::from_be_bytes([message[4], message[5], message[6], message[7]]);
                        let from_channel = u32::from_be_bytes([message[8], message[9], message[10], message[11]]);
                        let to_conn = u32::from_be_bytes([message[12], message[13], message[14], message[15]]);
                        let to_channel = u32::from_be_bytes([message[16], message[17], message[18], message[19]]);
                        
                        if to_conn == 0 {
                            let brid = (from_conn as u64) << 32 | from_channel as u64;
                            if let Some(b) = {broadcast_subscriptions.get(&brid).map(|b| b.clone())} {
                                if b.send(BroadcastMessage {
                                    from_conn,
                                    from_channel,
                                    data: message.slice(HEADER_SIZE..),
                                }).await.is_err() {
                                    broadcast_subscriptions.remove(&brid);
                                }
                            }
                        } else {
                            if let Some(b) = {data_channels.get(&to_channel).map(|b| b.clone())} {
                                if b.send(Message {
                                    from_conn,
                                    from_channel,
                                    to_conn,
                                    to_channel,
                                    data: message.slice(HEADER_SIZE..),
                                }).await.is_err() {
                                    data_channels.remove(&to_channel);
                                }
                            }
                        }
                    };
                    r_notify_stop.send_stop().await;
                };
                tokio::join!(rh, wh);
            }
        }));
        
        RunningClient {
            id: self.id,
            stop_tx,
            next_channel_id: AtomicU32::new(1 << 31),
            data_writer: data_writer_tx,
            data_channels,
            broadcast_subscriptions,
            join_handles,
        }
    }
    
}

pub struct RunningClient {
    id: u32,
    stop_tx: StopHandleSnd,
    next_channel_id: AtomicU32,
    data_writer: tokio::sync::mpsc::Sender<bytes::Bytes>,
    data_channels: Arc<DashMap<u32, tokio::sync::mpsc::Sender<Message>>>,
    broadcast_subscriptions: Arc<DashMap<u64, tokio::sync::mpsc::Sender<BroadcastMessage>>>,
    join_handles: Vec<tokio::task::JoinHandle<()>>,
}

impl RunningClient {
    
    pub async fn stop(self) {
        self.stop_tx.send_stop().await;
        for h in self.join_handles {
            let _ = h.await;
        }
    }
    
    pub async fn subscribe_to_broadcast(&self, addr: u32, channel: u32) -> Option<BroadcastSubscription> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        if let dashmap::mapref::entry::Entry::Vacant(e) = self.broadcast_subscriptions.entry((addr as u64) << 32 | channel as u64) {
            e.insert(tx);
            let mut request = BytesMut::with_capacity(20);
            request.put_u32(0);
            request.put_u32(addr);
            request.put_u32(channel);
            request.put_u32(0);
            request.put_u32(0);
            let _  = self.data_writer.send(request.freeze()).await;
            if let Some(mut result) = rx.recv().await {
                if result.data.get_u8() == 0 {
                    Some(BroadcastSubscription {
                        addr,
                        channel,
                        data_reader: rx,
                        data_writer: self.data_writer.clone(),
                        broadcast_subscriptions: Arc::clone(&self.broadcast_subscriptions),
                    })
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    }
    
    pub async fn open_new_channel(&self) -> (ChannelWriteHalf, ChannelReadHalf) {
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        loop {
            let id = self.next_channel_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if let dashmap::mapref::entry::Entry::Vacant(e) = self.data_channels.entry(id) {
                e.insert(tx);
                return (
                    ChannelWriteHalf {
                        conn_id: self.id,
                        channel_id: id,
                        data_writer: self.data_writer.clone(),
                    },
                    ChannelReadHalf {
                        conn_id: self.id,
                        channel_id: id,
                        data_reader: rx,
                    },
                );
            }
        }
    }
    
    pub async fn get_channel_if_free(&self, channel: u32) -> Option<(ChannelWriteHalf, ChannelReadHalf)> {
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        if let dashmap::mapref::entry::Entry::Vacant(e) = self.data_channels.entry(channel) {
            e.insert(tx);
            Some((
                ChannelWriteHalf {
                    conn_id: self.id,
                    channel_id: channel,
                    data_writer: self.data_writer.clone(),
                },
                ChannelReadHalf {
                    conn_id: self.id,
                    channel_id: channel,
                    data_reader: rx,
                },
            ))
        } else {
            None
        }
    }
    
}

pub struct BroadcastSubscription {
    addr: u32,
    channel: u32,
    data_writer: tokio::sync::mpsc::Sender<bytes::Bytes>,
    data_reader: tokio::sync::mpsc::Receiver<BroadcastMessage>,
    broadcast_subscriptions: Arc<DashMap<u64, tokio::sync::mpsc::Sender<BroadcastMessage>>>,
}

impl BroadcastSubscription {
    pub async fn cancel(&self) {
        let mut request = BytesMut::with_capacity(20);
        request.put_u32(0);
        request.put_u32(self.addr);
        request.put_u32(self.channel);
        request.put_u32(!0);
        request.put_u32(0);
        let _  = self.data_writer.send(request.freeze()).await;
        self.broadcast_subscriptions.remove(&((self.addr as u64) << 32 | self.channel as u64));
    }
    pub async fn read(&mut self) -> Option<BroadcastMessage> {
        self.data_reader.recv().await
    }
}

pub struct ChannelReadHalf {
    conn_id: u32,
    channel_id: u32,
    data_reader: tokio::sync::mpsc::Receiver<Message>,
}

impl ChannelReadHalf {
    pub fn conn_id(&self) -> u32 {
        self.conn_id
    }
    pub fn id(&self) -> u32 {
        self.channel_id
    }
    pub async fn read(&mut self) -> Option<Message> {
        self.data_reader.recv().await
    }
}

#[derive(Clone)]
pub struct ChannelWriteHalf {
    conn_id: u32,
    channel_id: u32,
    data_writer: tokio::sync::mpsc::Sender<bytes::Bytes>,
}

impl ChannelWriteHalf {
    pub fn conn_id(&self) -> u32 {
        self.conn_id
    }
    pub fn id(&self) -> u32 {
        self.channel_id
    }
    pub async fn send(&self, addr: u32, channel: u32, data: impl AsRef<[u8]>) -> Result<(), ()> {
        let data = data.as_ref();
        let mut request = BytesMut::with_capacity(20 + data.len());
        request.put_u32(data.len() as u32);
        request.put_u32(self.conn_id);
        request.put_u32(self.channel_id);
        request.put_u32(addr);
        request.put_u32(channel);
        request.put_slice(data);
        self.data_writer.send(request.freeze()).await.map_err(|_| ())?;
        Ok(())
    }
    pub async fn send_broadcast(&self, data: impl AsRef<[u8]>) -> Result<(), ()> {
        self.send(0, 0, data).await
    }
    pub async fn send_prepared<'a>(&'a self, addr: u32, channel: u32, mut message: PrepardMessage) -> Result<FrozenMessage<'a>, ()> {
        message.buffer[0..4].copy_from_slice(&(message.len as u32).to_be_bytes());
        message.buffer[4..8].copy_from_slice(&self.conn_id().to_be_bytes());
        message.buffer[8..12].copy_from_slice(&self.id().to_be_bytes());
        message.buffer[12..16].copy_from_slice(&addr.to_be_bytes());
        message.buffer[16..20].copy_from_slice(&channel.to_be_bytes());
        let message = message.buffer.freeze();
        self.data_writer.send(message.clone()).await.map_err(|_| ())?;
        Ok(FrozenMessage(addr, message, PhantomData::default()))
    }
    pub async fn send_frozen(&self, message: &FrozenMessage<'_>) -> Result<(), ()> {
        self.data_writer.send(message.1.clone()).await.map_err(|_| ())
    }
    pub async fn send_prepared_broadcast<'a>(&'a self, message: PrepardMessage) -> Result<FrozenMessage<'a>, ()> {
        self.send_prepared(0, 0, message).await
    }
}
