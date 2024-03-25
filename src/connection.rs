use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use dashmap::DashMap;

use crate::HEADER_SIZE;

pub trait DuplexConnection {
    type W<'a>: tokio::io::AsyncWrite + Unpin + Send + 'a where Self: 'a;
    type R<'a>: tokio::io::AsyncRead + Unpin + Send + 'a where Self: 'a;
    fn split<'a>(&'a mut self) -> (ConnectionWriteHalf<Self::W<'a>>, ConnectionReadHalf<Self::R<'a>>);
}

impl DuplexConnection for tokio::net::TcpStream {
    type W<'a> = tokio::net::tcp::WriteHalf<'a>;
    type R<'a> = tokio::net::tcp::ReadHalf<'a>;

    fn split<'a>(&'a mut self) -> (ConnectionWriteHalf<Self::W<'a>>, ConnectionReadHalf<Self::R<'a>>) {
        let (r, w) = self.split();
        (ConnectionWriteHalf(w), ConnectionReadHalf(r))
    }
}

#[cfg(unix)]
impl DuplexConnection for tokio::net::UnixStream {
    type W<'a> = tokio::net::unix::WriteHalf<'a>;
    type R<'a> = tokio::net::unix::ReadHalf<'a>;

    fn split<'a>(&'a mut self) -> (ConnectionWriteHalf<Self::W<'a>>, ConnectionReadHalf<Self::R<'a>>) {
        let (r, w) = self.split();
        (ConnectionWriteHalf(w), ConnectionReadHalf(r))
    }
}

pub struct ConnectionWriteHalf<T>(T);
pub struct ConnectionReadHalf<T>(T);


impl<T> ConnectionWriteHalf<T>
where
    T: tokio::io::AsyncWrite + Unpin
{
    
    pub async fn snd(&mut self, mut data: bytes::Bytes) -> Result<(), ()> {
        use tokio::io::AsyncWriteExt;
        self.0.write_all_buf(&mut data).await.map_err(|_| ())?;
        self.0.flush().await.map_err(|_| ())?;
        Ok(())
    }
    
}

impl<T> ConnectionReadHalf<T>
where
    T: tokio::io::AsyncRead + Unpin
{
    
    pub async fn rcv(&mut self) -> Option<bytes::Bytes> {
        use tokio::io::AsyncReadExt;
        let mut size = [0u8; 4];
        let mut cursor = 0;
        while cursor < size.len() {
            let len = self.0.read(&mut size).await.ok()?;
            cursor += len;
            if len == 0 {
                return None;
            }
        }
        let msg_size = u32::from_be_bytes(size);
        let size = msg_size as usize + HEADER_SIZE;
        let mut buffer = BytesMut::with_capacity(size);
        bytes::BufMut::put_u32(&mut buffer, msg_size);
        while buffer.len() < size {
            self.0.read_buf(&mut buffer).await.ok()?;
        }
        Some(buffer.freeze())
    }
    
}

pub trait ConnectionManager {
    fn manage(self, id: u32, broadcast_subscriptions: Arc<DashMap<u64, DashMap<u32, tokio::sync::mpsc::Sender<bytes::Bytes>>>>, connections: Arc<DashMap<u32, tokio::sync::mpsc::Sender<bytes::Bytes>>>, conn_tx: tokio::sync::mpsc::Sender::<bytes::Bytes>, stop_wh_rx: tokio::sync::mpsc::Receiver::<tokio::sync::mpsc::Sender<()>>, stop_rh_rx: tokio::sync::mpsc::Receiver::<tokio::sync::mpsc::Sender<()>>, conn_rx: tokio::sync::mpsc::Receiver::<bytes::Bytes>, stop_tx: tokio::sync::mpsc::Sender<tokio::sync::mpsc::Sender<()>>);
}

pub struct StConnectionManager<Connection: DuplexConnection> {
    conn: Connection,
}

impl<Connection: DuplexConnection> StConnectionManager<Connection> {
    pub fn new(conn: Connection) -> Self {
        Self { conn }
    }
}

impl<T: DuplexConnection + Send + 'static> ConnectionManager for StConnectionManager<T> {
    fn manage(self, id: u32, broadcast_subscriptions: Arc<DashMap<u64, DashMap<u32, tokio::sync::mpsc::Sender<bytes::Bytes>>>>, connections: Arc<DashMap<u32, tokio::sync::mpsc::Sender<bytes::Bytes>>>, conn_tx: tokio::sync::mpsc::Sender::<bytes::Bytes>, mut stop_wh_rx: tokio::sync::mpsc::Receiver::<tokio::sync::mpsc::Sender<()>>, mut stop_rh_rx: tokio::sync::mpsc::Receiver::<tokio::sync::mpsc::Sender<()>>, mut conn_rx: tokio::sync::mpsc::Receiver::<bytes::Bytes>, stop_tx: tokio::sync::mpsc::Sender<tokio::sync::mpsc::Sender<()>>) {
        let r_notify_stop = stop_tx.clone();
        let w_notify_stop = stop_tx;
        let mut conn = self.conn;
        tokio::spawn(async move {
            let (mut wh, mut rh) = conn.split();
            let wh = async move {
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
            };
            let rh = async move {
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
                                broadcast_channels.push((from_channel, broadcast_channel));
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
                            let mut result = bytes::BytesMut::with_capacity(HEADER_SIZE + 1);
                            result.put_u32(1);
                            result.put_u32(from_conn);
                            result.put_u32(from_channel);
                            result.put_u32(0);
                            result.put_u32(0);
                            // Subscribe
                            if let Some(c) = broadcast_subscriptions.get(&broadcast_channel) {
                                c.insert(id, conn_tx.clone());
                                result.put_u8(0);
                                let _ = conn_tx.send(result.freeze()).await;
                            } else {
                                result.put_u8(1);
                                let _ = conn_tx.send(result.freeze()).await;
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
                for (channel_id, channel) in broadcast_channels {
                    let mut close_message = bytes::BytesMut::with_capacity(HEADER_SIZE + 1);
                    close_message.put_u32(0);
                    close_message.put_u32(id);
                    close_message.put_u32(channel_id);
                    close_message.put_u32(0);
                    close_message.put_u32(1);
                    let close_message = close_message.freeze();
                    let chs = if let dashmap::mapref::entry::Entry::Occupied(e) = broadcast_subscriptions.entry(channel) {
                        Some(e.remove())
                    } else {
                        None
                    };
                    if let Some(chs) = chs {
                        for (_, ch) in chs {
                            let _ = ch.send(close_message.clone()).await;
                        }
                    }
                }
            };
            tokio::join!(rh, wh);
        });
    }
}